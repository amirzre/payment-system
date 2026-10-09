package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/amirzre/payment-system/services/payment/internal/domain"
	"github.com/google/uuid"
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
