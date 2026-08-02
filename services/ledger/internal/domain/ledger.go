package domain

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Direction constants for ledger entries.
const (
	DirectionDebit  = "debit"
	DirectionCredit = "credit"
)

// LedgerEntry represents a single immutable entry in the double-entry ledger.
// Every transaction produces exactly two entries: one debit and one credit.
type LedgerEntry struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	AccountID     uuid.UUID
	Direction     string
	AmountMinor   int64
	Currency      string
}

var ErrDoubleEntryViolation = errors.New("debit and credit amounts do not match")

// NewLedgerEntryPair returns exactly one debit and one credit entry that sum to zero.
func NewLedgerEntryPair(txID, sourceAccountID, destAccountID uuid.UUID, amountMinor int64, currency string) (debit, credit LedgerEntry, err error) {
	if amountMinor <= 0 {
		return LedgerEntry{}, LedgerEntry{}, fmt.Errorf("%w: amount must be positive, got %d", ErrDoubleEntryViolation, amountMinor)
	}

	if sourceAccountID == destAccountID {
		return LedgerEntry{}, LedgerEntry{}, fmt.Errorf("%w: source and destination accounts must differ", ErrDoubleEntryViolation)
	}

	debit = LedgerEntry{
		ID:            uuid.New(),
		TransactionID: txID,
		AccountID:     sourceAccountID,
		Direction:     DirectionDebit,
		AmountMinor:   amountMinor,
		Currency:      currency,
	}

	credit = LedgerEntry{
		ID:            uuid.New(),
		TransactionID: txID,
		AccountID:     destAccountID,
		Direction:     DirectionCredit,
		AmountMinor:   amountMinor,
		Currency:      currency,
	}

	return debit, credit, nil
}
