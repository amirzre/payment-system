package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/amirzre/payment-system/services/account/internal/domain"
	"github.com/amirzre/payment-system/services/account/internal/repository"
	"github.com/google/uuid"

	sharederrors "github.com/amirzre/payment-system/shared/errors"
)

// CreateAccountInput holds the data needed to create an account.
type CreateAccountReq struct {
	OwnerRef string
	Currency string
}

// AccountService defines the interface for account business logic.
type AccountService interface {
	Create(ctx context.Context, accountReq CreateAccountReq) (*domain.Account, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Account, error)
}

// accountService implements AccountService interface.
type accountService struct {
	repo repository.AccountRepository
}

// NewAccountService creates a new account service instance.
func NewAccountService(repo repository.AccountRepository) AccountService {
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
