package repository

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/amirzre/payment-system/services/ledger/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ledgerTestDB(t *testing.T) (*pgxpool.Pool, LedgerRepository) {
	t.Helper()
	url := os.Getenv("LEDGER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LEDGER_TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "ledger_test_" + uuid.New().String()[:8]
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	files, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(data)); err != nil {
			t.Fatal(err)
		}
	}
	return pool, NewLedgerRepository(pool)
}

func TestLedgerPostingIsIdempotentAndEmitsOutbox(t *testing.T) {
	pool, repo := ledgerTestDB(t)
	ctx := context.Background()
	source, destination, txID := uuid.New(), uuid.New(), uuid.New()
	if err := repo.Deposit(ctx, source, 1000, "USD"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		debit, credit, _ := domain.NewLedgerEntryPair(txID, source, destination, 250, "USD")
		if err := repo.PostEntries(ctx, txID, []domain.LedgerEntry{debit, credit}); err != nil {
			t.Fatal(err)
		}
	}
	if balance, err := repo.GetBalance(ctx, source); err != nil || balance != 750 {
		t.Fatalf("source balance=%d, error=%v; repeated transaction debited twice", balance, err)
	}
	var count int
	var payload []byte
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM ledger_entries WHERE transaction_id=$1", txID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("entries=%d, want 2", count)
	}
	if err := pool.QueryRow(ctx, "SELECT payload FROM outbox WHERE subject='ledger.posted' AND key=$1", txID.String()).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var event struct {
		TransactionID string `json:"transaction_id"`
		Source        string `json:"source_account_id"`
		Destination   string `json:"destination_account_id"`
		Amount        int64  `json:"amount_minor"`
		Currency      string `json:"currency"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatal(err)
	}
	if event.TransactionID != txID.String() || event.Source != source.String() || event.Destination != destination.String() || event.Amount != 250 || event.Currency != "USD" {
		t.Fatalf("wrong notification: %+v", event)
	}
	debit, credit, _ := domain.NewLedgerEntryPair(txID, source, destination, 300, "USD")
	if err := repo.PostEntries(ctx, txID, []domain.LedgerEntry{debit, credit}); err == nil {
		t.Fatal("reused transaction with different amount must fail")
	}
}

func TestDepositAndOutboxCommitTogether(t *testing.T) {
	pool, repo := ledgerTestDB(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "ALTER TABLE outbox ADD CONSTRAINT reject_outbox CHECK (false)"); err != nil {
		t.Fatal(err)
	}
	account := uuid.New()
	if err := repo.Deposit(ctx, account, 1000, "USD"); err == nil {
		t.Fatal("deposit must fail when its outbox write fails")
	}
	if balance, err := repo.GetBalance(ctx, account); err != nil || balance != 0 {
		t.Fatalf("partial commit: balance=%d error=%v", balance, err)
	}
}

func TestConcurrentTransfersCannotOverdraw(t *testing.T) {
	_, repo := ledgerTestDB(t)
	ctx := context.Background()
	source := uuid.New()
	if err := repo.Deposit(ctx, source, 300, "USD"); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			txID := uuid.New()
			debit, credit, _ := domain.NewLedgerEntryPair(txID, source, uuid.New(), 250, "USD")
			results <- repo.PostEntries(ctx, txID, []domain.LedgerEntry{debit, credit})
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("successful transfers=%d, want 1", success)
	}
	if balance, err := repo.GetBalance(ctx, source); err != nil || balance != 50 {
		t.Fatalf("balance=%d error=%v, want 50", balance, err)
	}
}
