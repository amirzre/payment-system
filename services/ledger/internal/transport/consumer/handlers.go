package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/amirzre/payment-system/services/ledger/internal/app"
	"github.com/google/uuid"

	"github.com/amirzre/payment-system/shared/eventbus"
)

// FundsReservedEvent is received when account-service confirms funds are available.
type FundsReservedEvent struct {
	PaymentKey           string `json:"payment_key"`
	SourceAccountID      string `json:"source_account_id"`
	DestinationAccountID string `json:"destination_account_id"`
	AmountMinor          int64  `json:"amount_minor"`
	Currency             string `json:"currency"`
}

// FundsReservedHandler processes funds.reserved events.
// Posts double-entry ledger entries and publishes ledger.posted.
func FundsReservedHandler(
	service app.LedgerService,
	pub eventbus.Publisher,
	log *slog.Logger,
) eventbus.Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		var evt FundsReservedEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("unmarshaling funds.reserved: %w", err)
		}

		sourceID, err := uuid.Parse(evt.SourceAccountID)
		if err != nil {
			return fmt.Errorf("parsing source account id: %w", err)
		}
		destID, err := uuid.Parse(evt.DestinationAccountID)
		if err != nil {
			return fmt.Errorf("parsing dest account id: %w", err)
		}

		txID := uuid.New()

		err = service.PostEntries(ctx, app.PostEntriesInput{
			TransactionID:   txID,
			SourceAccountID: sourceID,
			DestAccountID:   destID,
			AmountMinor:     evt.AmountMinor,
			Currency:        evt.Currency,
		})
		if err != nil {
			log.Error("posting entries failed", slog.String("payment_key", evt.PaymentKey), slog.Any("error", err))
			return fmt.Errorf("posting entries: %w", err)
		}

		postedEvt := map[string]any{
			"transaction_id":         txID.String(),
			"payment_key":            evt.PaymentKey,
			"source_account_id":      evt.SourceAccountID,
			"destination_account_id": evt.DestinationAccountID,
			"amount_minor":           evt.AmountMinor,
			"currency":               evt.Currency,
		}
		if err := pub.Publish(ctx, "ledger.posted", evt.PaymentKey, postedEvt); err != nil {
			return fmt.Errorf("publishing ledger.posted: %w", err)
		}

		log.Info("ledger entries posted via event",
			slog.String("tx_id", txID.String()),
			slog.String("payment_key", evt.PaymentKey),
		)

		return nil
	}
}
