package app

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	"github.com/amirzre/payment-system/services/account/internal/domain"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

type balanceLedgerClient struct {
	ledgerv1.LedgerServiceClient
	accountID string
	calls     int
	response  *ledgerv1.GetBalanceResponse
}

func (c *balanceLedgerClient) GetBalance(_ context.Context, req *ledgerv1.GetBalanceRequest, _ ...grpc.CallOption) (*ledgerv1.GetBalanceResponse, error) {
	c.calls++
	c.accountID = req.GetAccountId()
	if c.response != nil {
		return c.response, nil
	}
	return &ledgerv1.GetBalanceResponse{AccountId: req.GetAccountId(), BalanceMinor: 987, Currency: "EUR"}, nil
}

type accountMemoryRepository struct {
	accounts   map[uuid.UUID]*domain.Account
	queried    []uuid.UUID
	queryError error
}

func (r *accountMemoryRepository) Create(_ context.Context, account *domain.Account) error {
	copy := *account
	r.accounts[account.ID] = &copy
	return nil
}

func (r *accountMemoryRepository) GetByID(_ context.Context, id uuid.UUID) (*domain.Account, error) {
	r.queried = append(r.queried, id)
	return r.accounts[id], r.queryError
}

func TestCreatePersistsNormalizedCurrency(t *testing.T) {
	repo := &accountMemoryRepository{accounts: make(map[uuid.UUID]*domain.Account)}
	service := NewAccountService(repo, nil, nil)
	account, err := service.Create(context.Background(), CreateAccountReq{OwnerRef: "owner", Currency: " usd "})
	if err != nil {
		t.Fatal(err)
	}
	if account.Currency != "USD" || repo.accounts[account.ID].Currency != "USD" {
		t.Fatalf("returned and persisted currency = %q, %q, want USD", account.Currency, repo.accounts[account.ID].Currency)
	}
}

func TestGetBalanceResolvesMissingLedgerCurrencyFromAccount(t *testing.T) {
	for _, tc := range []struct {
		name, ledgerCurrency string
		missing              bool
		queryError           error
		wantKind             sharederrors.Kind
		wantError            bool
	}{
		{name: "account currency"},
		{name: "nonexistent account", missing: true, wantKind: sharederrors.KindNotFound, wantError: true},
		{name: "metadata lookup error", queryError: errors.New("metadata unavailable"), wantKind: sharederrors.KindInternal, wantError: true},
		{name: "nonempty ledger currency preserved", ledgerCurrency: "EUR", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := uuid.New()
			repo := &accountMemoryRepository{accounts: make(map[uuid.UUID]*domain.Account), queryError: tc.queryError}
			if !tc.missing {
				repo.accounts[id] = &domain.Account{ID: id, Currency: "USD", Status: domain.StatusActive}
			}
			ledger := &balanceLedgerClient{response: &ledgerv1.GetBalanceResponse{AccountId: id.String(), BalanceMinor: 0, Currency: tc.ledgerCurrency}}
			balance, err := NewAccountService(repo, ledger, nil).GetBalance(context.Background(), id)
			if tc.wantError {
				if !sharederrors.Is(err, tc.wantKind) || balance != nil {
					t.Fatalf("balance = %+v, error = %v, want kind %v", balance, err, tc.wantKind)
				}
				return
			}
			wantCurrency := "USD"
			if tc.ledgerCurrency != "" {
				wantCurrency = "EUR"
			}
			if err != nil || balance.BalanceMinor != 0 || balance.Currency != wantCurrency {
				t.Fatalf("balance = %+v, error = %v, want 0 %s", balance, err, wantCurrency)
			}
			if tc.ledgerCurrency != "" {
				if len(repo.queried) != 0 {
					t.Fatal("nonempty ledger currency must not require a metadata lookup")
				}
			} else if len(repo.queried) != 1 || repo.queried[0] != id {
				t.Fatalf("metadata lookups = %v, want account %s", repo.queried, id)
			}
		})
	}
}

func TestGetBalanceCachesResolvedCurrencyForPristineAndFundedAccounts(t *testing.T) {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL is required for Redis integration")
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name             string
		amount           int64
		currency, cached string
	}{
		{"pristine zero account", 0, "USD", "0:USD"},
		{"funded account", 2500, "EUR", "2500:EUR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := uuid.New()
			key := "balance:" + id.String()
			t.Cleanup(func() { client.Del(context.Background(), key) })
			repo := &accountMemoryRepository{accounts: map[uuid.UUID]*domain.Account{id: {ID: id, Currency: tc.currency, Status: domain.StatusActive}}}
			ledger := &balanceLedgerClient{response: &ledgerv1.GetBalanceResponse{AccountId: id.String(), BalanceMinor: tc.amount}}
			service := NewAccountService(repo, ledger, client)
			balance, err := service.GetBalance(context.Background(), id)
			if err != nil || balance.BalanceMinor != tc.amount || balance.Currency != tc.currency {
				t.Fatalf("balance = %+v, error = %v, want %d %s", balance, err, tc.amount, tc.currency)
			}
			if val, err := client.Get(context.Background(), key).Result(); err != nil || val != tc.cached {
				t.Fatalf("cached = %q, error = %v, want %q", val, err, tc.cached)
			}
			balance, err = service.GetBalance(context.Background(), id)
			if err != nil || balance.BalanceMinor != tc.amount || balance.Currency != tc.currency {
				t.Fatalf("cached balance = %+v, error = %v", balance, err)
			}
			if ledger.calls != 1 || len(repo.queried) != 1 || repo.queried[0] != id {
				t.Fatalf("cache hit called upstream: ledger %d, metadata %v", ledger.calls, repo.queried)
			}
		})
	}
}

func TestGetBalanceReadsCachedValueAndRejectsMalformedCache(t *testing.T) {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL is required for Redis integration")
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	// The regression reads stdin instead of the cached string. Keep stdin at EOF
	// so a broken cache read fails deterministically without waiting for input.
	stdin, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdin
	os.Stdin = stdin
	t.Cleanup(func() { os.Stdin = original; stdin.Close() })
	for _, tc := range []struct {
		name, value string
		amount      int64
		currency    string
		calls       int
	}{
		{"cached balance", "1250:USD", 1250, "USD", 0},
		{"negative cached balance", "-42:EUR", -42, "EUR", 0},
		{"malformed amount", "not-money:USD", 987, "EUR", 1},
		{"missing currency", "1250:", 987, "EUR", 1},
		{"extra field", "1250:USD:extra", 987, "EUR", 1},
		{"trailing garbage", "1250:USD garbage", 987, "EUR", 1},
		{"overflow", "9223372036854775808:USD", 987, "EUR", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := uuid.New()
			key := "balance:" + id.String()
			t.Cleanup(func() { client.Del(context.Background(), key) })
			if err := client.Set(context.Background(), key, tc.value, time.Minute).Err(); err != nil {
				t.Fatal(err)
			}
			ledger := &balanceLedgerClient{}
			service := NewAccountService(nil, ledger, client)
			balance, err := service.GetBalance(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if balance.BalanceMinor != tc.amount || balance.Currency != tc.currency {
				t.Fatalf("balance = %+v, want %d %s", balance, tc.amount, tc.currency)
			}
			if ledger.calls != tc.calls {
				t.Fatalf("ledger calls = %d, want %d", ledger.calls, tc.calls)
			}
			if tc.calls > 0 && ledger.accountID != id.String() {
				t.Fatalf("ledger queried wrong account: %s", ledger.accountID)
			}
		})
	}
}
