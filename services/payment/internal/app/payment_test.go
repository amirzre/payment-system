package app

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	accountv1 "github.com/amirzre/payment-system/gen/go/account/v1"
	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	"github.com/amirzre/payment-system/services/payment/internal/domain"
	"github.com/amirzre/payment-system/services/payment/internal/repository"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func paymentAppDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "payment_app_" + uuid.New().String()[:8]
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	files, _ := filepath.Glob("../../migrations/*.up.sql")
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(data)); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

type accountClientFixture struct{ accountv1.AccountServiceClient }

type changingAccountClient struct {
	accountClientFixture
	inactive bool
}

func (f *changingAccountClient) GetAccount(ctx context.Context, req *accountv1.GetAccountRequest, opts ...grpc.CallOption) (*accountv1.GetAccountResponse, error) {
	response, err := f.accountClientFixture.GetAccount(ctx, req, opts...)
	if f.inactive {
		response.Account.Status = "frozen"
	}
	return response, err
}

func TestLostReplyRecoverySurvivesAccountStateChange(t *testing.T) {
	pool := paymentAppDB(t)
	repo := repository.NewPaymentRepository(pool)
	ledger := &ledgerClientFixture{committed: map[string]bool{}, loseReply: true}
	account := &changingAccountClient{}
	service := NewPaymentService(repo, account, ledger, nil)
	input := CreatePaymentInput{"lost-reply-account-change", uuid.New(), uuid.New(), 250, "USD"}
	if _, err := service.Create(context.Background(), input); status.Code(err) != codes.Unavailable {
		t.Fatalf("first error=%v", err)
	}
	account.inactive = true
	p, err := service.Create(context.Background(), input)
	if err != nil || p.Status != domain.StatusCompleted || len(ledger.committed) != 1 {
		t.Fatalf("committed payment incorrectly rejected after account change: %+v %v", p, err)
	}
}

func (accountClientFixture) GetAccount(_ context.Context, req *accountv1.GetAccountRequest, _ ...grpc.CallOption) (*accountv1.GetAccountResponse, error) {
	return &accountv1.GetAccountResponse{Account: &accountv1.Account{Id: req.Id, Currency: "USD", Status: "active"}}, nil
}
func (accountClientFixture) GetBalance(context.Context, *accountv1.GetBalanceRequest, ...grpc.CallOption) (*accountv1.GetBalanceResponse, error) {
	return &accountv1.GetBalanceResponse{BalanceMinor: 1000, Currency: "USD"}, nil
}

// Simulate a remote ledger commit whose first reply is lost. It honors the
// transaction-ID contract independently of the payment database transaction.
type ledgerClientFixture struct {
	ledgerv1.LedgerServiceClient
	mu        sync.Mutex
	committed map[string]bool
	loseReply bool
}

func (f *ledgerClientFixture) PostEntries(_ context.Context, req *ledgerv1.PostEntriesRequest, _ ...grpc.CallOption) (*ledgerv1.PostEntriesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed[req.TransactionId] = true
	if f.loseReply {
		f.loseReply = false
		return nil, status.Error(codes.Unavailable, "reply lost after commit")
	}
	return &ledgerv1.PostEntriesResponse{Success: true}, nil
}

func TestLostLedgerReplyRemainsRetryable(t *testing.T) {
	pool := paymentAppDB(t)
	repo := repository.NewPaymentRepository(pool)
	ledger := &ledgerClientFixture{committed: map[string]bool{}, loseReply: true}
	service := NewPaymentService(repo, accountClientFixture{}, ledger, nil)
	input := CreatePaymentInput{"lost-reply", uuid.New(), uuid.New(), 250, "USD"}
	if _, err := service.Create(context.Background(), input); status.Code(err) != codes.Unavailable {
		t.Fatalf("error=%v, want Unavailable", err)
	}
	p, err := repo.GetByIdempotencyKey(context.Background(), input.IdempotencyKey)
	if err != nil || p.Status != domain.StatusPosting || p.TransactionID == nil {
		t.Fatalf("ambiguous commit marked terminal: %+v err=%v", p, err)
	}
	stable := *p.TransactionID
	p, err = service.Create(context.Background(), input)
	if err != nil || p.Status != domain.StatusCompleted || p.TransactionID == nil || *p.TransactionID != stable {
		t.Fatalf("retry result=%+v err=%v", p, err)
	}
	if len(ledger.committed) != 1 {
		t.Fatal("retry used another ledger transaction ID")
	}
	var events int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox WHERE subject='payment.completed'").Scan(&events); err != nil || events != 1 {
		t.Fatalf("completion events=%d err=%v", events, err)
	}
}

func TestConcurrentRequestsReuseOnePayment(t *testing.T) {
	pool := paymentAppDB(t)
	repo := repository.NewPaymentRepository(pool)
	ledger := &ledgerClientFixture{committed: map[string]bool{}}
	service := NewPaymentService(repo, accountClientFixture{}, ledger, nil)
	input := CreatePaymentInput{"concurrent", uuid.New(), uuid.New(), 250, "USD"}
	results := make(chan *domain.PaymentIntent, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := service.Create(context.Background(), input)
			results <- p
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id uuid.UUID
	for p := range results {
		if p == nil || p.Status != domain.StatusCompleted {
			t.Fatalf("payment=%+v", p)
		}
		if id == uuid.Nil {
			id = p.ID
		}
		if p.ID != id {
			t.Fatal("duplicate payment IDs")
		}
	}
	if len(ledger.committed) != 1 {
		t.Fatal("duplicate ledger transactions")
	}
	input.AmountMinor = 300
	if _, err := service.Create(context.Background(), input); !sharederrors.Is(err, sharederrors.KindAlreadyExists) {
		t.Fatalf("conflicting request error=%v", err)
	}
}

func TestLegacyUnknownLedgerTransactionCannotBeReposted(t *testing.T) {
	pool := paymentAppDB(t)
	repo := repository.NewPaymentRepository(pool)
	ledger := &ledgerClientFixture{committed: map[string]bool{}}
	service := NewPaymentService(repo, accountClientFixture{}, ledger, nil)
	for _, state := range []string{domain.StatusFundsReserved, domain.StatusLedgerPosted} {
		input := CreatePaymentInput{"legacy-" + state, uuid.New(), uuid.New(), 250, "USD"}
		p, _ := domain.NewPaymentIntent(input.IdempotencyKey, input.SourceAccountID, input.DestinationAccountID, input.AmountMinor, input.Currency)
		if err := repo.Create(context.Background(), p); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(context.Background(), "UPDATE payment_intents SET transaction_id=NULL,status=$1 WHERE id=$2", state, p.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := service.Create(context.Background(), input); !sharederrors.Is(err, sharederrors.KindFailedPrecondition) {
			t.Fatalf("ambiguous legacy retry error=%v", err)
		}
	}
	if len(ledger.committed) != 0 {
		t.Fatal("reposted ambiguous legacy transaction")
	}
}
