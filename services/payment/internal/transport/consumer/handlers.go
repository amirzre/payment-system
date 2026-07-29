package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/amirzre/payment-system/services/payment/internal/domain"
	"github.com/amirzre/payment-system/services/payment/internal/repository"
	"github.com/google/uuid"

	"github.com/amirzre/payment-system/shared/eventbus"
)

// LedgerPostedEvent is received when the ledger successfully posts entries.
type LedgerPostedEvent struct {
	TransactionID string `json:"transaction_id"`
	PaymentKey    string `json:"payment_key"`
}

// LedgerPostedHandler processes ledger.posted events.
// Transitions the PaymentIntent to completed.
func LedgerPostedHandler(repo repository.PaymentRepository, log *slog.Logger) eventbus.Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		var evt LedgerPostedEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("unmarshaling ledger.posted: %w", err)
		}

		intent, err := repo.GetByIdempotencyKey(ctx, evt.PaymentKey)
		if err != nil {
			return fmt.Errorf("getting payment by key %s: %w", evt.PaymentKey, err)
		}
		if intent == nil {
			return fmt.Errorf("payment not found for key %s", evt.PaymentKey)
		}

		if intent.Status == domain.StatusCompleted {
			log.Debug("payment already completed", slog.String("id", intent.ID.String()))
			return nil
		}

		txID, _ := uuid.Parse(evt.TransactionID)
		intent.SetTransactionID(txID)

		if err := intent.Transition(domain.StatusLedgerPosted); err != nil {
			return fmt.Errorf("transitioning to ledger_posted: %w", err)
		}
		if err := repo.UpdateStatus(ctx, intent.ID, intent.Status); err != nil {
			return fmt.Errorf("updating status: %w", err)
		}

		if err := intent.Transition(domain.StatusCompleted); err != nil {
			return fmt.Errorf("transitioning to completed: %w", err)
		}
		if err := repo.UpdateStatus(ctx, intent.ID, intent.Status); err != nil {
			return fmt.Errorf("updating status: %w", err)
		}

		log.Info("payment completed via event",
			slog.String("id", intent.ID.String()),
			slog.String("key", evt.PaymentKey),
		)

		return nil
	}
}

// FundsReservationFailedEvent is received when account-service cannot reserve funds.
type FundsReservationFailedEvent struct {
	PaymentKey string `json:"payment_key"`
	Reason     string `json:"reason"`
}

// FundamentalsReservationFailedHandler processes funds.reservation.failed events.
func FundsReservationFailedHandler(repo repository.PaymentRepository, log *slog.Logger) eventbus.Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		var evt FundsReservationFailedEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("unmarshaling funds.reservation.failed: %w", err)
		}

		intent, err := repo.GetByIdempotencyKey(ctx, evt.PaymentKey)
		if err != nil {
			return fmt.Errorf("getting payment: %w", err)
		}
		if intent == nil {
			return nil
		}

		if intent.IsTerminal() {
			return nil
		}

		if err := intent.Transition(domain.StatusFailed); err != nil {
			return fmt.Errorf("transitioning to failed: %w", err)
		}
		repo.UpdateStatus(ctx, intent.ID, domain.StatusFailed)

		log.Info("payment failed via event",
			slog.String("id", intent.ID.String()),
			slog.String("reason", evt.Reason),
		)

		return nil
	}
}
