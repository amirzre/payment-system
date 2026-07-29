package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

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
	rows, err := db.Query(ctx, `
		SELECT id, subject, key, payload
		FROM outbox
		WHERE published = false
		ORDER BY created_at
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
			continue
		}
		records = append(records, r)
	}

	if len(records) == 0 {
		return
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Error("beginning outbox tx", slog.Any("error", err))
		return
	}
	defer tx.Rollback(ctx)

	for _, r := range records {
		// Publish to the event bus
		var payload any
		if err := json.Unmarshal([]byte(r.Payload), &payload); err != nil {
			payload = r.Payload
		}

		if err := pub.Publish(ctx, r.Subject, r.Key, payload); err != nil {
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
			continue
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Error("committing outbox tx", slog.Any("error", err))
		return
	}

	if len(records) > 0 {
		log.Debug("published outbox records", slog.Int("count", len(records)))
	}
}

// InsertOutbox inserts a row into the outbox table within the given transaction.
func InsertOutbox(ctx context.Context, tx pgxpool.Tx, subject, key string, payload any) error {
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
