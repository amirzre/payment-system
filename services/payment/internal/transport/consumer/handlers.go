package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/amirzre/payment-system/services/payment/internal/repository"
	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// LedgerPostedHandler reconciles a committed ledger operation when its gRPC
// reply was lost. The handler and processed-events marker share one transaction.
func LedgerPostedHandler(repo repository.PaymentRepository, log *slog.Logger) eventbus.TransactionalHandler {
	return func(ctx context.Context, tx pgx.Tx, key string, payload []byte) error {
		var evt struct {
			TransactionID string `json:"transaction_id"`
		}
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("decoding ledger notification: %w", err)
		}
		id, err := uuid.Parse(evt.TransactionID)
		if err != nil || id == uuid.Nil || key != evt.TransactionID {
			return fmt.Errorf("invalid ledger transaction ID or message key")
		}
		if err := repo.Reconcile(ctx, tx, id); err != nil {
			return err
		}
		log.Debug("reconciled ledger notification", slog.String("transaction_id", id.String()))
		return nil
	}
}
