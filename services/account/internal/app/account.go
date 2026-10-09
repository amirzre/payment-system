package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/amirzre/payment-system/services/account/internal/domain"
	"github.com/amirzre/payment-system/services/account/internal/repository"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
)

// CreateAccountInput holds the data needed to create an account.
type CreateAccountReq struct {
	OwnerRef string
	Currency string
}

// BalanceResponse holds the result of a balance query.
type BalanceResponse struct {
	BalanceMinor int64
	Currency     string
}

// AccountService defines the interface for account business logic.
type AccountService interface {
	Create(ctx context.Context, accountReq CreateAccountReq) (*domain.Account, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Account, error)
	GetBalance(ctx context.Context, accountID uuid.UUID) (*BalanceResponse, error)
	Deposit(ctx context.Context, accountID uuid.UUID, amount int64, currency string) (*BalanceResponse, error)
}

// accountService implements AccountService interface.
type accountService struct {
	repo         repository.AccountRepository
	ledgerClient ledgerv1.LedgerServiceClient
	redisClient  *redis.Client
}

// NewAccountService creates a new account service instance.
func NewAccountService(
	repo repository.AccountRepository,
	ledgerClient ledgerv1.LedgerServiceClient,
	redisClient *redis.Client,
) AccountService {
	return &accountService{
		repo:         repo,
		ledgerClient: ledgerClient,
		redisClient:  redisClient,
	}
}

// Create creates a new account after validating the input and domain rules.
func (s *accountService) Create(ctx context.Context, accountReq CreateAccountReq) (*domain.Account, error) {
	if strings.TrimSpace(accountReq.OwnerRef) == "" {
		return nil, fmt.Errorf("owner_ref must not be empty")
	}

	currency := strings.ToUpper(strings.TrimSpace(accountReq.Currency))
	if len(currency) != 3 {
		return nil, fmt.Errorf("currency must be a 3-letter ISO 4217 code, got %q", currency)
	}

	account := domain.Account{
		ID:        uuid.New(),
		OwnerRef:  accountReq.OwnerRef,
		Currency:  currency,
		Status:    domain.StatusActive,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, &account); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "failed to persist account", err)
	}

	return &account, nil
}

// GetByID retrieves an account by its ID.
func (s *accountService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	account, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "failed to query account", err)
	}
	if account == nil {
		return nil, sharederrors.New(sharederrors.KindNotFound, fmt.Sprintf("account %s not found", id))
	}

	return account, nil
}

// GetBalance retrieves the balance, using Redis cache if available.
func (s *accountService) GetBalance(ctx context.Context, accountID uuid.UUID) (*BalanceResponse, error) {
	cacheKey := fmt.Sprintf("balance:%s", accountID.String())

	if s.redisClient != nil {
		val, err := s.redisClient.Get(ctx, cacheKey).Result()
		if err == nil {
			amount, currency, found := strings.Cut(val, ":")
			balance, parseErr := strconv.ParseInt(amount, 10, 64)
			if found && parseErr == nil && len(currency) == 3 && strings.IndexFunc(currency, func(r rune) bool { return r < 'A' || r > 'Z' }) == -1 {
				return &BalanceResponse{BalanceMinor: balance, Currency: currency}, nil
			}
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := s.ledgerClient.GetBalance(ctx, &ledgerv1.GetBalanceRequest{
		AccountId: accountID.String(),
	})
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal,
			fmt.Sprintf("calling ledger-service.GetBalance for account %s", accountID), err)
	}

	result := &BalanceResponse{
		BalanceMinor: resp.GetBalanceMinor(),
		Currency:     resp.GetCurrency(),
	}
	if result.Currency == "" {
		account, err := s.GetByID(ctx, accountID)
		if err != nil {
			return nil, err
		}
		result.Currency = account.Currency
	}

	if s.redisClient != nil {
		cacheVal := fmt.Sprintf("%d:%s", result.BalanceMinor, result.Currency)
		s.redisClient.Set(ctx, cacheKey, cacheVal, 60*time.Second)
	}

	return result, nil
}

// Deposit validates the account and currency, then delegates the credit entry to ledger-service.
func (s *accountService) Deposit(ctx context.Context, accountID uuid.UUID, amount int64, currency string) (*BalanceResponse, error) {
	if amount <= 0 {
		return nil, sharederrors.New(sharederrors.KindInvalidArgument,
			fmt.Sprintf("amount must be positive, got %d", amount))
	}

	account, err := s.repo.GetByID(ctx, accountID)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "fetching account", err)
	}
	if account == nil {
		return nil, sharederrors.New(sharederrors.KindNotFound, fmt.Sprintf("account %s not found", accountID))
	}
	if account.Status != domain.StatusActive {
		return nil, sharederrors.New(sharederrors.KindFailedPrecondition,
			fmt.Sprintf("account %s is not active (status: %s)", accountID, account.Status))
	}

	normalizedCurrency := strings.ToUpper(strings.TrimSpace(currency))
	if account.Currency != normalizedCurrency {
		return nil, sharederrors.New(sharederrors.KindInvalidArgument,
			fmt.Sprintf("currency mismatch: account is %s, deposit is %s", account.Currency, normalizedCurrency))
	}

	depositCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := s.ledgerClient.Deposit(depositCtx, &ledgerv1.DepositRequest{
		AccountId:   accountID.String(),
		AmountMinor: amount,
		Currency:    normalizedCurrency,
	})
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "depositing via ledger", err)
	}

	if s.redisClient != nil {
		cacheKey := fmt.Sprintf("balance:%s", accountID.String())
		s.redisClient.Del(ctx, cacheKey)
	}

	return &BalanceResponse{
		BalanceMinor: resp.GetBalanceMinor(),
		Currency:     resp.GetCurrency(),
	}, nil
}
