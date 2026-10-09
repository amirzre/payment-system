package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxRecord represents a row in the outbox table.
type OutboxRecord struct {
	ID      int64  `json:"-"`
	Subject string `json:"subject"`
	Key     string `json:"key"`
	Payload string `json:"payload"`
}

// RunOutboxPublisher polls the outbox table and publishes unpublished rows via the given Publisher.
// Delivery is at least once: a successful publish followed by a failed commit
// will be retried. Consumers must deduplicate using the event key.
func RunOutboxPublisher(ctx context.Context, db *pgxpool.Pool, pub Publisher, log *slog.Logger, pollInterval time.Duration) {
	if pollInterval <= 0 {
		pollInterval = 1 * time.Second
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publishOutbox(ctx, db, pub, log)
		}
	}
}

func publishOutbox(ctx context.Context, db *pgxpool.Pool, pub Publisher, log *slog.Logger) {
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Error("beginning outbox tx", slog.Any("error", err))
		return
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `
		SELECT id, subject, key, payload
		FROM outbox
		WHERE published = false
		ORDER BY created_at, id
		LIMIT 50
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		log.Error("querying outbox", slog.Any("error", err))
		return
	}
	defer rows.Close()

	var records []OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		if err := rows.Scan(&r.ID, &r.Subject, &r.Key, &r.Payload); err != nil {
			log.Error("scanning outbox row", slog.Any("error", err))
			return
		}
		records = append(records, r)
	}
	// pgx cannot execute another statement while rows are open. Finish reading
	// and check errors before publishing any part of the batch.
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Error("reading outbox rows", slog.Any("error", err))
		return
	}

	if len(records) == 0 {
		return
	}

	published := 0
	for _, r := range records {
		// RawMessage avoids converting JSON numbers through float64.
		if err := pub.Publish(ctx, r.Subject, r.Key, json.RawMessage(r.Payload)); err != nil {
			log.Error("publishing outbox event",
				slog.String("subject", r.Subject),
				slog.String("key", r.Key),
				slog.Any("error", err),
			)
			continue
		}

		// Mark as published
		_, err := tx.Exec(ctx, "UPDATE outbox SET published = true, published_at = now() WHERE id = $1", r.ID)
		if err != nil {
			log.Error("marking outbox published", slog.Int64("id", r.ID), slog.Any("error", err))
			return
		}
		published++
	}

	if err := tx.Commit(ctx); err != nil {
		log.Error("committing outbox tx", slog.Any("error", err))
		return
	}

	if published > 0 {
		log.Debug("published outbox records", slog.Int("count", published))
	}
}

// InsertOutbox inserts a row into the outbox table within the given transaction.
func InsertOutbox(ctx context.Context, tx pgx.Tx, subject, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling outbox payload: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (subject, key, payload)
		VALUES ($1, $2, $3)
	`, subject, key, string(data))
	if err != nil {
		return fmt.Errorf("inserting outbox row: %w", err)
	}

	return nil
}
