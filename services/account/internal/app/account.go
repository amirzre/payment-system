package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/amirzre/payment-system/services/account/internal/domain"
	"github.com/amirzre/payment-system/services/account/internal/repository"
	"github.com/google/uuid"

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
}

// accountService implements AccountService interface.
type accountService struct {
	repo         repository.AccountRepository
	ledgerClient ledgerv1.LedgerServiceClient
}

// NewAccountService creates a new account service instance.
func NewAccountService(repo repository.AccountRepository, ledgerClient ledgerv1.LedgerServiceClient) AccountService {
	return &accountService{repo: repo}
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
		Currency:  accountReq.Currency,
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

	return result, nil
}
