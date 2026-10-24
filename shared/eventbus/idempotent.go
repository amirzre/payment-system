package eventbus

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// deliveryResult is scoped to one NATS delivery. The idempotency wrapper
// reports a skip without changing Handler's error contract; the subscriber
// records exactly one final outcome using its actual subject and consumer.
type deliveryResultKey struct{}
type deliveryResult struct{ duplicate atomic.Bool }

// TransactionalHandler must perform its database effects using the supplied
// transaction, so they commit atomically with the processed_events marker.
type TransactionalHandler func(context.Context, pgx.Tx, string, []byte) error

// WithTransactionalIdempotency commits the handler's database changes and its
// deduplication marker together. Failed handlers leave neither behind.
func WithTransactionalIdempotency(db *pgxpool.Pool, consumerName string, h TransactionalHandler) Handler {
	return withIdempotency(db, consumerName, h)
}

// WithIdempotency serializes duplicate deliveries using processed_events.
// Handler effects outside the marker transaction must themselves be idempotent:
// a crash after the effect but before commit may execute them again. Database
// handlers should use WithTransactionalIdempotency instead.
func WithIdempotency(db *pgxpool.Pool, consumerName string, h Handler) Handler {
	return withIdempotency(db, consumerName, func(ctx context.Context, _ pgx.Tx, key string, payload []byte) error {
		return h(ctx, key, payload)
	})
}

func withIdempotency(db *pgxpool.Pool, consumerName string, h func(context.Context, pgx.Tx, string, []byte) error) Handler {
	return func(ctx context.Context, key string, payload []byte) error {
		if key == "" {
			return fmt.Errorf("event key is required for idempotency")
		}
		tx, err := db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning idempotency tx: %w", err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()

		// Record the event as being processed
		result, err := tx.Exec(ctx, `
			INSERT INTO processed_events (consumer_name, event_key)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, consumerName, key)
		if err != nil {
			return fmt.Errorf("inserting processed_events: %w", err)
		}
		// The unique constraint waits for an in-flight duplicate to commit or
		// roll back. Only the delivery that inserted the marker may run h.
		if result.RowsAffected() == 0 {
			if delivery, ok := ctx.Value(deliveryResultKey{}).(*deliveryResult); ok {
				delivery.duplicate.Store(true)
			}
			return nil
		}

		// Run the handler before committing its marker. Transactional handlers
		// use tx for their database effects.
		if err := h(ctx, tx, key, payload); err != nil {
			return fmt.Errorf("handling event: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing idempotency tx: %w", err)
		}

		return nil
	}
}
