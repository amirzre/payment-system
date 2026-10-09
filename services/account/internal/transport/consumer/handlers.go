package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// CacheInvalidationHandler invalidates balances after committed ledger changes.
func CacheInvalidationHandler(redisClient *redis.Client, log *slog.Logger) eventbus.Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		var evt struct {
			TransactionID        string `json:"transaction_id"`
			SourceAccountID      string `json:"source_account_id"`
			DestinationAccountID string `json:"destination_account_id"`
		}
		if err := json.Unmarshal(payload, &evt); err != nil {
			return fmt.Errorf("unmarshaling ledger.posted for cache invalidation: %w", err)
		}
		for name, value := range map[string]string{"event key": key, "transaction_id": evt.TransactionID} {
			if value == "" {
				continue
			}
			id, err := uuid.Parse(value)
			if err != nil || id == uuid.Nil {
				return fmt.Errorf("invalid %s for ledger.posted", name)
			}
		}
		cacheKeys := make([]string, 0, 2)
		for _, accountID := range []string{evt.SourceAccountID, evt.DestinationAccountID} {
			if accountID == "" {
				continue
			}
			id, err := uuid.Parse(accountID)
			if err != nil || id == uuid.Nil {
				return fmt.Errorf("invalid account_id for ledger.posted: %q", accountID)
			}
			cacheKeys = append(cacheKeys, "balance:"+id.String())
		}
		if len(cacheKeys) == 0 {
			return fmt.Errorf("ledger.posted requires at least one affected account")
		}

		if redisClient == nil {
			return fmt.Errorf("Redis unavailable for ledger.posted cache invalidation")
		}

		if err := redisClient.Del(ctx, cacheKeys...).Err(); err != nil {
			return fmt.Errorf("invalidating ledger.posted balance cache: %w", err)
		}

		log.Debug("balance cache invalidated",
			slog.String("source", evt.SourceAccountID),
			slog.String("dest", evt.DestinationAccountID),
		)

		return nil
	}
}
