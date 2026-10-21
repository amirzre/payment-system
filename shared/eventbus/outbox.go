package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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
	defer recordOutboxBacklog(ctx, db)
	tx, err := db.Begin(ctx)
	if err != nil {
		log.ErrorContext(ctx, "beginning outbox tx")
		return
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `
		SELECT id, subject, key, payload, trace_context
		FROM outbox
		WHERE published = false
		ORDER BY created_at, id
		LIMIT 50
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		log.ErrorContext(ctx, "querying outbox")
		return
	}
	defer rows.Close()

	type pendingRecord struct {
		OutboxRecord
		traceContext []byte
	}
	var records []pendingRecord
	for rows.Next() {
		var r pendingRecord
		if err := rows.Scan(&r.ID, &r.Subject, &r.Key, &r.Payload, &r.traceContext); err != nil {
			log.ErrorContext(ctx, "scanning outbox row")
			return
		}
		records = append(records, r)
	}
	// pgx cannot execute another statement while rows are open. Finish reading
	// and check errors before publishing any part of the batch.
	rows.Close()
	if err := rows.Err(); err != nil {
		log.ErrorContext(ctx, "reading outbox rows")
		return
	}

	if len(records) == 0 {
		return
	}

	published := 0
	for _, r := range records {
		// RawMessage avoids converting JSON numbers through float64.
		publishCtx := restoreOutboxContext(ctx, r.traceContext)
		publishCtx, span := otel.Tracer(tracerName).Start(
			publishCtx,
			"outbox publish",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(eventAttributes(r.Subject)...),
		)
		started := time.Now()
		err := pub.Publish(publishCtx, r.Subject, r.Key, json.RawMessage(r.Payload))
		outcome := "success"
		if err != nil {
			outcome = "error"
			span.SetStatus(codes.Error, "publication failed")
		}
		metrics.RecordOutbox(r.Subject, outcome, time.Since(started).Seconds())
		span.End()
		if err != nil {
			log.ErrorContext(publishCtx, "publishing outbox event",
				slog.String("subject", boundedSubject(r.Subject)),
			)
			continue
		}

		// Mark as published
		_, err = tx.Exec(ctx, "UPDATE outbox SET published = true, published_at = now() WHERE id = $1", r.ID)
		if err != nil {
			log.ErrorContext(publishCtx, "marking outbox published")
			return
		}
		published++
	}

	if err := tx.Commit(ctx); err != nil {
		log.ErrorContext(ctx, "committing outbox tx")
		return
	}

	if published > 0 {
		log.DebugContext(ctx, "published outbox records", slog.Int("count", published))
	}
}

// InsertOutbox inserts a row into the outbox table within the given transaction.
func InsertOutbox(ctx context.Context, tx pgx.Tx, subject, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling outbox payload: %w", err)
	}

	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	traceContext, err := json.Marshal(carrier)
	if err != nil {
		return fmt.Errorf("marshaling outbox trace context: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (subject, key, payload, trace_context)
		VALUES ($1, $2, $3, $4)
	`, subject, key, string(data), traceContext)
	if err != nil {
		return fmt.Errorf("inserting outbox row: %w", err)
	}

	return nil
}

// recordOutboxBacklog runs outside the publication transaction, so a metrics
// query failure cannot roll back a successful business publication. Grouping is
// bounded here as well as in metrics to combine every unknown subject safely.
func recordOutboxBacklog(ctx context.Context, db *pgxpool.Pool) {
	rows, err := db.Query(ctx, `SELECT CASE WHEN subject IN ('ledger.posted','payment.completed','account.created') THEN subject ELSE 'other' END,
 count(*), GREATEST(0, EXTRACT(EPOCH FROM now()-min(created_at)))::float8
 FROM outbox WHERE published = false GROUP BY 1`)
	if err != nil {
		return
	}
	defer rows.Close()
	type backlog struct {
		subject         string
		pending, oldest float64
	}
	var results []backlog
	for rows.Next() {
		var b backlog
		if err := rows.Scan(&b.subject, &b.pending, &b.oldest); err != nil {
			return
		}
		results = append(results, b)
	}
	if rows.Err() != nil {
		return
	}
	for _, subject := range []string{"ledger.posted", "payment.completed", "account.created", "other"} {
		metrics.SetOutbox(subject, 0, 0)
	}
	for _, b := range results {
		metrics.SetOutbox(b.subject, b.pending, b.oldest)
	}
}
