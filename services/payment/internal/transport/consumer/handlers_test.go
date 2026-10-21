package consumer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/amirzre/payment-system/services/payment/internal/domain"
	"github.com/amirzre/payment-system/services/payment/internal/repository"
	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLedgerNotificationReconcilesAndDeduplicates(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TEMP TABLE payment_intents (LIKE public.payment_intents INCLUDING ALL); CREATE TEMP TABLE outbox (LIKE public.outbox INCLUDING ALL); ALTER TABLE outbox ADD COLUMN IF NOT EXISTS trace_context JSONB NOT NULL DEFAULT '{}'; CREATE TEMP TABLE processed_events (LIKE public.processed_events INCLUDING ALL)`); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewPaymentRepository(pool)
	p, _ := domain.NewPaymentIntent("reconcile", uuid.New(), uuid.New(), 250, "USD")
	p.SetTransactionID(uuid.New())
	if err := repo.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := eventbus.WithTransactionalIdempotency(pool, "payment-ledger-posted", LedgerPostedHandler(repo, log))
	data, _ := json.Marshal(map[string]string{"transaction_id": p.TransactionID.String()})
	for range 2 {
		if err := handler(ctx, p.TransactionID.String(), data); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.GetByID(ctx, p.ID)
	if err != nil || got.Status != domain.StatusCompleted {
		t.Fatalf("reconciled payment=%+v err=%v", got, err)
	}
	for _, table := range []string{"outbox", "processed_events"} {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s rows=%d err=%v", table, count, err)
		}
	}
	if err := handler(ctx, "wrong-key", data); err == nil {
		t.Fatal("mismatched event key was accepted")
	}
	var markers int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM processed_events").Scan(&markers); err != nil || markers != 1 {
		t.Fatalf("invalid notification recorded as processed: %d %v", markers, err)
	}
}
