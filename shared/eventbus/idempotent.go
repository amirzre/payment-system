package eventbus

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WithIdempotency wraps a Handler with event deduplication using a processed_events table.
func WithIdempotency(db *pgxpool.Pool, consumerName string, h Handler) Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		tx, err := db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning idempotency tx: %w", err)
		}
		defer tx.Rollback(ctx)

		// Check if this event has already been processed
		var exists bool
		err = tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM processed_events
				WHERE consumer_name = $1 AND event_key = $2
			)
		`, consumerName, key).Scan(&exists)
		if err != nil {
			return fmt.Errorf("checking processed_events: %w", err)
		}
		if exists {
			return nil
		}

		// Record the event as being processed
		_, err = tx.Exec(ctx, `
			INSERT INTO processed_events (consumer_name, event_key)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, consumerName, key)
		if err != nil {
			return fmt.Errorf("inserting processed_events: %w", err)
		}

		// Call the actual handler within the same transaction
		if err := h(ctx, key, payload); err != nil {
			return fmt.Errorf("handling event: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing idempotency tx: %w", err)
		}

		return nil
	}
}
