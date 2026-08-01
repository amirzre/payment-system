package repository

import (
	"context"
	"fmt"
	"sort"

	"github.com/amirzre/payment-system/services/ledger/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LedgerRepository defines the persistence interface for ledger entries.
type LedgerRepository interface {
	PostEntries(ctx context.Context, txID uuid.UUID, entries []domain.LedgerEntry) error
	GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error)
	Deposit(ctx context.Context, accountID uuid.UUID, amount int64, currency string) error
}

// ledgerRepository implements the ledger repository using Postgres.
type ledgerRepository struct {
	pool *pgxpool.Pool
}

// NewLedgerRepository creates a new LedgerRepository.
func NewLedgerRepository(pool *pgxpool.Pool) LedgerRepository {
	return &ledgerRepository{pool: pool}
}

// PostEntries inserts a set of ledger entries atomically.
func (r *ledgerRepository) PostEntries(ctx context.Context, txID uuid.UUID, entries []domain.LedgerEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("no entries to post")
	}

	accountIDs := collectAccountIDs(entries)
	sort.Slice(accountIDs, func(i, j int) bool {
		return accountIDs[i].String() < accountIDs[j].String()
	})

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, accountID := range accountIDs {
		_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1::text))", accountID.String())
		if err != nil {
			return fmt.Errorf("acquiring advisory lock for account %s: %w", accountID, err)
		}
	}

	query := `
		INSERT INTO ledger_entries (id, transaction_id, account_id, direction, amount_minor, currency)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	for _, entry := range entries {
		_, err := tx.Exec(ctx, query,
			entry.ID, entry.TransactionID, entry.AccountID,
			entry.Direction, entry.AmountMinor, entry.Currency,
		)
		if err != nil {
			return fmt.Errorf("inserting ledger entry: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// GetBalance computes the current balance for an account by summing all its ledger entries.
func (r *ledgerRepository) GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	query := `
		SELECT COALESCE(
			SUM(CASE
				WHEN direction = 'credit' THEN amount_minor
				WHEN direction = 'debit'  THEN -amount_minor
				ELSE 0
			END),
			0
		) AS balance
		FROM ledger_entries
		WHERE account_id = $1
	`
	var balance int64
	err := r.pool.QueryRow(ctx, query, accountID).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("querying balance for account %s: %w", accountID, err)
	}

	return balance, nil
}

// Deposit inserts a single credit ledger entry.
func (r *ledgerRepository) Deposit(ctx context.Context, accountID uuid.UUID, amount int64, currency string) error {
	query := `
		INSERT INTO ledger_entries (id, transaction_id, account_id, direction, amount_minor, currency)
		VALUES ($1, $2, $3, 'credit', $4, $5)
	`
	_, err := r.pool.Exec(ctx, query, uuid.New(), uuid.New(), accountID, amount, currency)
	if err != nil {
		return fmt.Errorf("inserting deposit entry: %w", err)
	}
	return nil
}

// collectAccountIDs extracts unique account IDs from a set of entries.
func collectAccountIDs(entries []domain.LedgerEntry) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{})
	var ids []uuid.UUID
	for _, e := range entries {
		if _, ok := seen[e.AccountID]; !ok {
			seen[e.AccountID] = struct{}{}
			ids = append(ids, e.AccountID)
		}
	}
	return ids
}
