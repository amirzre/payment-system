package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Payment status constants — the state machine.
const (
	StatusCreated       = "created"
	StatusPosting       = "posting"
	StatusFundsReserved = "funds_reserved"
	StatusLedgerPosted  = "ledger_posted"
	StatusCompleted     = "completed"
	StatusFailed        = "failed"
	StatusPaymentFailed = "payment_failed"
	StatusCompensated   = "compensated"
)

// ErrInvalidTransition is returned when an illegal state transition is attempted.
var ErrInvalidTransition = errors.New("invalid state transition")

// terminalStatuses are states from which no further transition is allowed.
var terminalStatuses = map[string]bool{
	StatusCompleted:     true,
	StatusFailed:        true,
	StatusPaymentFailed: true,
	StatusCompensated:   true,
}

// PaymentIntent represents a payment in the system.
type PaymentIntent struct {
	ID                   uuid.UUID
	IdempotencyKey       string
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	AmountMinor          int64
	Currency             string
	Status               string
	TransactionID        *uuid.UUID
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// NewPaymentIntent creates a new PaymentIntent in the "created" state.
func NewPaymentIntent(idempotencyKey string, sourceID, destID uuid.UUID, amountMinor int64, currency string) (*PaymentIntent, error) {
	if idempotencyKey == "" {
		return nil, fmt.Errorf("idempotency_key must not be empty")
	}
	if amountMinor <= 0 {
		return nil, fmt.Errorf("amount_minor must be positive, got %d", amountMinor)
	}
	if sourceID == destID {
		return nil, fmt.Errorf("source and destination accounts must differ")
	}
	if len(currency) != 3 {
		return nil, fmt.Errorf("currency must be a 3-letter ISO 4217 code, got %q", currency)
	}

	now := time.Now().UTC()
	return &PaymentIntent{
		ID:                   uuid.New(),
		IdempotencyKey:       idempotencyKey,
		SourceAccountID:      sourceID,
		DestinationAccountID: destID,
		AmountMinor:          amountMinor,
		Currency:             currency,
		Status:               StatusCreated,
		CreatedAt:            now,
		UpdatedAt:            now,
	}, nil
}

// Transition attempts to move the PaymentIntent to the target status.
func (p *PaymentIntent) Transition(target string) error {
	if terminalStatuses[p.Status] {
		return fmt.Errorf("%w: cannot transition from terminal status %q to %q", ErrInvalidTransition, p.Status, target)
	}

	if !isLegalTransition(p.Status, target) {
		return fmt.Errorf("%w: cannot transition from %q to %q", ErrInvalidTransition, p.Status, target)
	}

	p.Status = target
	p.UpdatedAt = time.Now().UTC()
	return nil
}

// transitionTable defines all legal transitions.
var transitionTable = map[string][]string{
	StatusCreated:       {StatusPosting, StatusFundsReserved, StatusFailed},
	StatusPosting:       {StatusCompleted, StatusFailed},
	StatusFundsReserved: {StatusLedgerPosted, StatusPaymentFailed},
	StatusLedgerPosted:  {StatusCompleted, StatusCompensated},
}

// isLegalTransition checks whether a transition is in the legal set.
func isLegalTransition(from, to string) bool {
	targets, ok := transitionTable[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}

// IsTerminal returns true if the payment is in a terminal (final) state.
func (p *PaymentIntent) IsTerminal() bool {
	return terminalStatuses[p.Status]
}

// SetTransactionID records the stable ledger transaction ID before posting.
func (p *PaymentIntent) SetTransactionID(txID uuid.UUID) {
	p.TransactionID = &txID
}
