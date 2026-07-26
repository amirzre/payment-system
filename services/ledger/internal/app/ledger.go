package app

import (
	"context"
	"fmt"

	"github.com/amirzre/payment-system/services/ledger/internal/domain"
	"github.com/amirzre/payment-system/services/ledger/internal/repository"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
	"github.com/google/uuid"
)

// LedgerEntryService defines the interface for ledger business logic.
type LedgerService interface {
	PostEntries(ctx context.Context, input PostEntriesInput) error
	GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error)
}

// ledgerEntryService implements LedgerEntryService interface.
type ledgerService struct {
	repo repository.LedgerRepository
}

// NewLedgerEntryService creates a new ledger service instance.
func NewLedgerEntryService(repo repository.LedgerRepository) LedgerService {
	return &ledgerService{repo: repo}
}

// PostEntriesInput holds the data needed to post ledger entries.
type PostEntriesInput struct {
	TransactionID   uuid.UUID
	SourceAccountID uuid.UUID
	DestAccountID   uuid.UUID
	AmountMinor     int64
	Currency        string
}

// PostEntries validates the double-entry invariant and persists the entries.
func (s *ledgerService) PostEntries(ctx context.Context, input PostEntriesInput) error {
	debit, credit, err := domain.NewLedgerEntryPair(
		input.TransactionID,
		input.SourceAccountID,
		input.DestAccountID,
		input.AmountMinor,
		input.Currency,
	)
	if err != nil {
		return sharederrors.New(sharederrors.KindInvalidArgument, err.Error())
	}

	entries := []domain.LedgerEntry{debit, credit}
	if err := s.repo.PostEntries(ctx, input.TransactionID, entries); err != nil {
		return sharederrors.Wrap(sharederrors.KindInternal, "failed to post entries", err)
	}

	return nil
}

// GetBalance retrieves the balance for the given account.
func (s *ledgerService) GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	balance, err := s.repo.GetBalance(ctx, accountID)
	if err != nil {
		return 0, sharederrors.Wrap(sharederrors.KindInternal,
			fmt.Sprintf("failed to get balance for account %s", accountID), err)
	}

	return balance, nil
}
