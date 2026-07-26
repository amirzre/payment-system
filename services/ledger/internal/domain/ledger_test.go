package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestNewLedgerEntryPair(t *testing.T) {
	txID := uuid.New()
	sourceID := uuid.New()
	destID := uuid.New()

	t.Run("valid pair", func(t *testing.T) {
		debit, credit, err := NewLedgerEntryPair(txID, sourceID, destID, 1000, "USD")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if debit.AmountMinor != credit.AmountMinor {
			t.Errorf("debit amount %d != credit amount %d", debit.AmountMinor, credit.AmountMinor)
		}

		if debit.Direction != DirectionDebit {
			t.Errorf("expected debit direction, got %q", debit.Direction)
		}
		if credit.Direction != DirectionCredit {
			t.Errorf("expected credit direction, got %q", credit.Direction)
		}

		if debit.AccountID != sourceID {
			t.Errorf("debit account_id mismatch")
		}
		if credit.AccountID != destID {
			t.Errorf("credit account_id mismatch")
		}

		if debit.TransactionID != txID {
			t.Errorf("debit transaction_id mismatch")
		}
		if credit.TransactionID != txID {
			t.Errorf("credit transaction_id mismatch")
		}

		if debit.Currency != "USD" || credit.Currency != "USD" {
			t.Errorf("currency mismatch")
		}

		// Verify unique IDs
		if debit.ID == credit.ID {
			t.Errorf("debit and credit should have unique IDs")
		}
		if debit.ID == uuid.Nil || credit.ID == uuid.Nil {
			t.Errorf("IDs should not be nil")
		}
	})

	t.Run("zero amount", func(t *testing.T) {
		_, _, err := NewLedgerEntryPair(txID, sourceID, destID, 0, "USD")
		if err == nil {
			t.Fatal("expected error for zero amount")
		}
		if !errors.Is(err, ErrDoubleEntryViolation) {
			t.Errorf("expected ErrDoubleEntryViolation, got %v", err)
		}
	})

	t.Run("negative amount", func(t *testing.T) {
		_, _, err := NewLedgerEntryPair(txID, sourceID, destID, -500, "USD")
		if err == nil {
			t.Fatal("expected error for negative amount")
		}
		if !errors.Is(err, ErrDoubleEntryViolation) {
			t.Errorf("expected ErrDoubleEntryViolation, got %v", err)
		}
	})

	t.Run("same source and destination", func(t *testing.T) {
		_, _, err := NewLedgerEntryPair(txID, sourceID, sourceID, 1000, "USD")
		if err == nil {
			t.Fatal("expected error for same source and destination")
		}
		if !errors.Is(err, ErrDoubleEntryViolation) {
			t.Errorf("expected ErrDoubleEntryViolation, got %v", err)
		}
	})
}
