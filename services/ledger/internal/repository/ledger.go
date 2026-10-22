package repository

import (
	"context"
	"fmt"
	"sort"

	"github.com/amirzre/payment-system/services/ledger/internal/domain"
	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LedgerRepository interface {
	PostEntries(context.Context, uuid.UUID, []domain.LedgerEntry) error
	GetBalance(context.Context, uuid.UUID) (int64, error)
	Deposit(context.Context, uuid.UUID, int64, string) error
}

type ledgerRepository struct{ pool *pgxpool.Pool }

func NewLedgerRepository(pool *pgxpool.Pool) LedgerRepository { return &ledgerRepository{pool: pool} }

// Ledger notifications describe committed facts, not requests to move money.
type ledgerPostedEvent struct {
	TransactionID        string `json:"transaction_id"`
	SourceAccountID      string `json:"source_account_id,omitempty"`
	DestinationAccountID string `json:"destination_account_id"`
	AmountMinor          int64  `json:"amount_minor"`
	Currency             string `json:"currency"`
}

const balanceSQL = `SELECT COALESCE(SUM(CASE WHEN direction='credit' THEN amount_minor ELSE -amount_minor END),0) FROM ledger_entries WHERE account_id=$1`

func (r *ledgerRepository) PostEntries(ctx context.Context, txID uuid.UUID, entries []domain.LedgerEntry) error {
	if txID == uuid.Nil || len(entries) != 2 {
		return domain.ErrDoubleEntryViolation
	}
	var debit, credit *domain.LedgerEntry
	for i := range entries {
		e := &entries[i]
		if e.TransactionID != txID || e.AccountID == uuid.Nil || e.AmountMinor <= 0 || len(e.Currency) != 3 {
			return domain.ErrDoubleEntryViolation
		}
		switch e.Direction {
		case domain.DirectionDebit:
			if debit != nil {
				return domain.ErrDoubleEntryViolation
			}
			debit = e
		case domain.DirectionCredit:
			if credit != nil {
				return domain.ErrDoubleEntryViolation
			}
			credit = e
		default:
			return domain.ErrDoubleEntryViolation
		}
	}
	if debit == nil || credit == nil || debit.AccountID == credit.AccountID || debit.AmountMinor != credit.AmountMinor || debit.Currency != credit.Currency {
		return domain.ErrDoubleEntryViolation
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning ledger transaction: %w", err)
	}
	defer tx.Rollback(context.Background())
	// Serialize a transaction ID before checking for a previous successful call.
	if err := lock(ctx, tx, "ledger:tx:"+txID.String()); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT account_id,direction,amount_minor,currency FROM ledger_entries WHERE transaction_id=$1`, txID)
	if err != nil {
		return err
	}
	var existing []domain.LedgerEntry
	for rows.Next() {
		var e domain.LedgerEntry
		if err := rows.Scan(&e.AccountID, &e.Direction, &e.AmountMinor, &e.Currency); err != nil {
			rows.Close()
			return err
		}
		existing = append(existing, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(existing) > 0 {
		if len(existing) != 2 {
			return domain.ErrTransactionConflict
		}
		for _, old := range existing {
			match := false
			for _, e := range entries {
				if old.AccountID == e.AccountID && old.Direction == e.Direction && old.AmountMinor == e.AmountMinor && old.Currency == e.Currency {
					match = true
				}
			}
			if !match {
				return domain.ErrTransactionConflict
			}
		}
		return nil // Existing committed transaction: no entries or events added.
	}
	ids := collectAccountIDs(entries)
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		if err := lock(ctx, tx, "ledger:account:"+id.String()); err != nil {
			return err
		}
		if err := checkCurrency(ctx, tx, id, debit.Currency); err != nil {
			return err
		}
	}
	var balance int64
	if err := tx.QueryRow(ctx, balanceSQL, debit.AccountID).Scan(&balance); err != nil {
		return err
	}
	if balance < debit.AmountMinor {
		return domain.ErrInsufficientFunds
	}
	for _, e := range entries {
		if _, err := tx.Exec(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_id,direction,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, e.ID, txID, e.AccountID, e.Direction, e.AmountMinor, e.Currency); err != nil {
			return err
		}
	}
	event := ledgerPostedEvent{txID.String(), debit.AccountID.String(), credit.AccountID.String(), debit.AmountMinor, debit.Currency}
	if err := eventbus.InsertOutbox(ctx, tx, "ledger.posted", txID.String(), event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *ledgerRepository) GetBalance(ctx context.Context, id uuid.UUID) (int64, error) {
	var balance int64
	err := r.pool.QueryRow(ctx, balanceSQL, id).Scan(&balance)
	return balance, err
}

func (r *ledgerRepository) Deposit(ctx context.Context, id uuid.UUID, amount int64, currency string) error {
	if id == uuid.Nil || amount <= 0 || len(currency) != 3 {
		return domain.ErrDoubleEntryViolation
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err := lock(ctx, tx, "ledger:account:"+id.String()); err != nil {
		return err
	}
	if err := checkCurrency(ctx, tx, id, currency); err != nil {
		return err
	}
	txID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_id,direction,amount_minor,currency) VALUES($1,$2,$3,'credit',$4,$5)`, uuid.New(), txID, id, amount, currency); err != nil {
		return err
	}
	event := ledgerPostedEvent{TransactionID: txID.String(), DestinationAccountID: id.String(), AmountMinor: amount, Currency: currency}
	if err := eventbus.InsertOutbox(ctx, tx, "ledger.posted", txID.String(), event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func lock(ctx context.Context, tx pgx.Tx, key string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", key)
	return err
}
func checkCurrency(ctx context.Context, tx pgx.Tx, id uuid.UUID, currency string) error {
	var mismatch bool
	err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ledger_entries WHERE account_id=$1 AND currency<>$2)", id, currency).Scan(&mismatch)
	if err != nil {
		return err
	}
	if mismatch {
		return domain.ErrCurrencyMismatch
	}
	return nil
}
func collectAccountIDs(entries []domain.LedgerEntry) []uuid.UUID {
	seen := make(map[uuid.UUID]bool)
	var ids []uuid.UUID
	for _, e := range entries {
		if !seen[e.AccountID] {
			seen[e.AccountID] = true
			ids = append(ids, e.AccountID)
		}
	}
	return ids
}
