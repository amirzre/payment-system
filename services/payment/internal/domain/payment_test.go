package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestPaymentIntentStateMachine(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		p, err := NewPaymentIntent("key-1", uuid.New(), uuid.New(), 1000, "USD")
		if err != nil {
			t.Fatalf("NewPaymentIntent: %v", err)
		}

		if p.Status != StatusCreated {
			t.Errorf("expected status created, got %q", p.Status)
		}

		// created → funds_reserved
		if err := p.Transition(StatusFundsReserved); err != nil {
			t.Fatalf("transition to funds_reserved: %v", err)
		}
		if p.Status != StatusFundsReserved {
			t.Errorf("expected funds_reserved, got %q", p.Status)
		}

		// funds_reserved → ledger_posted
		if err := p.Transition(StatusLedgerPosted); err != nil {
			t.Fatalf("transition to ledger_posted: %v", err)
		}
		if p.Status != StatusLedgerPosted {
			t.Errorf("expected ledger_posted, got %q", p.Status)
		}

		// ledger_posted → completed
		if err := p.Transition(StatusCompleted); err != nil {
			t.Fatalf("transition to completed: %v", err)
		}
		if p.Status != StatusCompleted {
			t.Errorf("expected completed, got %q", p.Status)
		}

		if !p.IsTerminal() {
			t.Error("expected completed to be terminal")
		}
	})

	t.Run("failure path", func(t *testing.T) {
		p, err := NewPaymentIntent("key-2", uuid.New(), uuid.New(), 500, "EUR")
		if err != nil {
			t.Fatalf("NewPaymentIntent: %v", err)
		}

		// created → failed
		if err := p.Transition(StatusFailed); err != nil {
			t.Fatalf("transition to failed: %v", err)
		}
		if !p.IsTerminal() {
			t.Error("expected failed to be terminal")
		}
	})

	t.Run("compensated path", func(t *testing.T) {
		p, err := NewPaymentIntent("key-3", uuid.New(), uuid.New(), 2000, "USD")
		if err != nil {
			t.Fatalf("NewPaymentIntent: %v", err)
		}

		_ = p.Transition(StatusFundsReserved)
		_ = p.Transition(StatusLedgerPosted)
		if err := p.Transition(StatusCompensated); err != nil {
			t.Fatalf("transition to compensated: %v", err)
		}
		if !p.IsTerminal() {
			t.Error("expected compensated to be terminal")
		}
	})

	t.Run("payment_failed path", func(t *testing.T) {
		p, err := NewPaymentIntent("key-4", uuid.New(), uuid.New(), 100, "USD")
		if err != nil {
			t.Fatalf("NewPaymentIntent: %v", err)
		}

		_ = p.Transition(StatusFundsReserved)
		if err := p.Transition(StatusPaymentFailed); err != nil {
			t.Fatalf("transition to payment_failed: %v", err)
		}
		if !p.IsTerminal() {
			t.Error("expected payment_failed to be terminal")
		}
	})

	t.Run("illegal transitions", func(t *testing.T) {
		tests := []struct {
			name  string
			from  string
			to    string
			setup func(*PaymentIntent)
		}{
			{"created → completed (skipping steps)", StatusCreated, StatusCompleted, nil},
			{"created → ledger_posted (skipping funds_reserved)", StatusCreated, StatusLedgerPosted, nil},
			{"funds_reserved → completed (skipping ledger_posted)", StatusFundsReserved, StatusCompleted, nil},
			{"funds_reserved → failed (not a legal branch)", StatusFundsReserved, StatusFailed, nil},
			{"ledger_posted → failed", StatusLedgerPosted, StatusFailed, nil},
			{"completed → anything", StatusCompleted, StatusFundsReserved, func(p *PaymentIntent) {
				p.Status = StatusCompleted
			}},
			{"failed → anything", StatusFailed, StatusFundsReserved, func(p *PaymentIntent) {
				p.Status = StatusFailed
			}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p, _ := NewPaymentIntent("key", uuid.New(), uuid.New(), 100, "USD")
				if tt.setup != nil {
					tt.setup(p)
				} else {
					p.Status = tt.from
				}

				err := p.Transition(tt.to)
				if err == nil {
					t.Errorf("expected error for transition %q → %q", tt.from, tt.to)
				}
				if !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("expected ErrInvalidTransition, got %v", err)
				}
			})
		}
	})

	t.Run("NewPaymentIntent validation", func(t *testing.T) {
		sourceID := uuid.New()
		destID := uuid.New()

		tests := []struct {
			name        string
			key         string
			source      uuid.UUID
			dest        uuid.UUID
			amount      int64
			currency    string
			expectError bool
		}{
			{"empty idempotency key", "", sourceID, destID, 100, "USD", true},
			{"zero amount", "key", sourceID, destID, 0, "USD", true},
			{"negative amount", "key", sourceID, destID, -100, "USD", true},
			{"same source/dest", "key", sourceID, sourceID, 100, "USD", true},
			{"invalid currency", "key", sourceID, destID, 100, "US", true},
			{"valid", "key", sourceID, destID, 100, "USD", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := NewPaymentIntent(tt.key, tt.source, tt.dest, tt.amount, tt.currency)
				if tt.expectError && err == nil {
					t.Error("expected error")
				}
				if !tt.expectError && err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		}
	})

	t.Run("SetTransactionID", func(t *testing.T) {
		p, _ := NewPaymentIntent("key", uuid.New(), uuid.New(), 100, "USD")
		txID := uuid.New()

		if p.TransactionID != nil {
			t.Error("expected nil TransactionID initially")
		}

		p.SetTransactionID(txID)
		if p.TransactionID == nil || *p.TransactionID != txID {
			t.Error("SetTransactionID did not set correctly")
		}
	})
}
