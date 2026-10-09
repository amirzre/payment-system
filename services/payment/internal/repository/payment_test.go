package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/amirzre/payment-system/services/payment/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGetByIDRoundTrip(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	// One session keeps all reads on the temporary table, preserving real data.
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, "CREATE TEMP TABLE payment_intents (LIKE public.payment_intents INCLUDING ALL)")
	if err != nil {
		t.Fatal(err)
	}
	repo := NewPaymentRepository(pool)
	for _, withTransaction := range []bool{false, true} {
		payment, err := domain.NewPaymentIntent(uuid.NewString(), uuid.New(), uuid.New(), 250, "USD")
		if err != nil {
			t.Fatal(err)
		}
		if withTransaction {
			payment.SetTransactionID(uuid.New())
		}
		if err := repo.Create(ctx, payment); err != nil {
			t.Fatal(err)
		}
		got, err := repo.GetByID(ctx, payment.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.ID != payment.ID || got.IdempotencyKey != payment.IdempotencyKey || got.SourceAccountID != payment.SourceAccountID || got.DestinationAccountID != payment.DestinationAccountID || got.AmountMinor != 250 || got.Currency != "USD" || got.Status != "created" || got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
			t.Fatalf("incorrect payment round trip: %+v", got)
		}
		if (got.TransactionID == nil) != (payment.TransactionID == nil) || (withTransaction && *got.TransactionID != *payment.TransactionID) {
			t.Fatal("transaction ID did not round trip")
		}
	}
	if got, err := repo.GetByID(ctx, uuid.New()); err != nil || got != nil {
		t.Fatalf("missing payment = %+v, error = %v", got, err)
	}
}

func TestPaymentCompletionAndOutboxAreAtomic(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `CREATE TEMP TABLE payment_intents (LIKE public.payment_intents INCLUDING ALL); CREATE TEMP TABLE outbox (LIKE public.outbox INCLUDING ALL);`); err != nil {
		t.Fatal(err)
	}
	repo := NewPaymentRepository(pool)
	p, _ := domain.NewPaymentIntent("completion-test", uuid.New(), uuid.New(), 250, "USD")
	p.SetTransactionID(uuid.New())
	if err := repo.Create(ctx, p); err != nil {
		t.Fatal(err)
	}
	_, err = repo.Process(ctx, p.ID, func(tx pgx.Tx, current *domain.PaymentIntent) error {
		return repo.Complete(ctx, tx, current)
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetByID(ctx, p.ID)
	if err != nil || got.Status != domain.StatusCompleted || got.TransactionID == nil || *got.TransactionID != *p.TransactionID {
		t.Fatalf("payment=%+v error=%v", got, err)
	}
	var data []byte
	if err := pool.QueryRow(ctx, "SELECT payload FROM outbox WHERE subject='payment.completed' AND key=$1", p.ID.String()).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var event struct {
		PaymentID     string `json:"payment_id"`
		TransactionID string `json:"transaction_id"`
		Amount        int64  `json:"amount_minor"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if event.PaymentID != p.ID.String() || event.TransactionID != p.TransactionID.String() || event.Amount != 250 {
		t.Fatalf("bad completion event: %+v", event)
	}
	_, err = repo.Process(ctx, p.ID, func(tx pgx.Tx, current *domain.PaymentIntent) error { return repo.Complete(ctx, tx, current) })
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&count); err != nil || count != 1 {
		t.Fatalf("completion events=%d error=%v", count, err)
	}
	p2, _ := domain.NewPaymentIntent("rollback-test", uuid.New(), uuid.New(), 250, "USD")
	p2.SetTransactionID(uuid.New())
	if err := repo.Create(ctx, p2); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE outbox ADD CONSTRAINT reject_new CHECK (key <> '"+p2.ID.String()+"')"); err != nil {
		t.Fatal(err)
	}
	_, err = repo.Process(ctx, p2.ID, func(tx pgx.Tx, current *domain.PaymentIntent) error { return repo.Complete(ctx, tx, current) })
	if err == nil {
		t.Fatal("expected failed outbox insertion")
	}
	got, err = repo.GetByID(ctx, p2.ID)
	if err != nil || got.Status != domain.StatusCreated {
		t.Fatalf("partial payment completion: %+v err=%v", got, err)
	}
}
