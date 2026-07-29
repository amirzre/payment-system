package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	accountv1 "github.com/amirzre/payment-system/gen/go/account/v1"
	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/redis/go-redis/v9"
)

// PaymentRequestedEvent is received when a new payment is initiated.
type PaymentRequestedEvent struct {
	PaymentKey           string `json:"payment_key"`
	SourceAccountID      string `json:"source_account_id"`
	DestinationAccountID string `json:"destination_account_id"`
	AmountMinor          int64  `json:"amount_minor"`
	Currency             string `json:"currency"`
}

// PaymentRequestedHandler processes payment.requested events.
// Checks source account balance and publishes funds.reserved or funds.reservation.failed.
func PaymentRequestedHandler(
	accountClient accountv1.AccountServiceClient,
	pub eventbus.Publisher,
	log *slog.Logger,
) eventbus.Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		var evt PaymentRequestedEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("unmarshaling payment.requested: %w", err)
		}

		// Check source account balance
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		balanceResp, err := accountClient.GetBalance(ctx, &accountv1.GetBalanceRequest{
			AccountId: evt.SourceAccountID,
		})
		if err != nil {
			// Publish failure event
			failEvt := map[string]string{
				"payment_key": evt.PaymentKey,
				"reason":      fmt.Sprintf("balance check failed: %v", err),
			}
			pub.Publish(ctx, "funds.reservation.failed", evt.PaymentKey, failEvt)
			return fmt.Errorf("checking balance: %w", err)
		}

		if balanceResp.GetBalanceMinor() < evt.AmountMinor {
			failEvt := map[string]string{
				"payment_key": evt.PaymentKey,
				"reason":      "insufficient funds",
			}
			pub.Publish(ctx, "funds.reservation.failed", evt.PaymentKey, failEvt)
			return nil
		}

		// Publish funds.reserved
		reservedEvt := map[string]any{
			"payment_key":            evt.PaymentKey,
			"source_account_id":      evt.SourceAccountID,
			"destination_account_id": evt.DestinationAccountID,
			"amount_minor":           evt.AmountMinor,
			"currency":               evt.Currency,
		}
		if err := pub.Publish(ctx, "funds.reserved", evt.PaymentKey, reservedEvt); err != nil {
			return fmt.Errorf("publishing funds.reserved: %w", err)
		}

		log.Info("funds reserved", slog.String("payment_key", evt.PaymentKey))
		return nil
	}
}

// LedgerPostedHandler invalidates the balance cache for the affected account.
func CacheInvalidationHandler(redisClient *redis.Client, log *slog.Logger) eventbus.Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		var evt struct {
			SourceAccountID      string `json:"source_account_id"`
			DestinationAccountID string `json:"destination_account_id"`
		}
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("unmarshaling ledger.posted for cache invalidation: %w", err)
		}

		if redisClient == nil {
			return nil
		}

		for _, accountID := range []string{evt.SourceAccountID, evt.DestinationAccountID} {
			if accountID != "" {
				cacheKey := fmt.Sprintf("balance:%s", accountID)
				redisClient.Del(ctx, cacheKey)
			}
		}

		log.Debug("balance cache invalidated",
			slog.String("source", evt.SourceAccountID),
			slog.String("dest", evt.DestinationAccountID),
		)

		return nil
	}
}
