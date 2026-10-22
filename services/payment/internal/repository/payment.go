package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/amirzre/payment-system/services/payment/internal/domain"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PaymentRepository interface {
	Create(context.Context, *domain.PaymentIntent) error
	GetByID(context.Context, uuid.UUID) (*domain.PaymentIntent, error)
	GetByIdempotencyKey(context.Context, string) (*domain.PaymentIntent, error)
	UpdateStatus(context.Context, uuid.UUID, string) error
	Process(context.Context, uuid.UUID, func(pgx.Tx, *domain.PaymentIntent) error) (*domain.PaymentIntent, error)
	Complete(context.Context, pgx.Tx, *domain.PaymentIntent) error
	Reconcile(context.Context, pgx.Tx, uuid.UUID) error
}

type paymentRepository struct{ pool *pgxpool.Pool }

func NewPaymentRepository(pool *pgxpool.Pool) PaymentRepository {
	return &paymentRepository{pool: pool}
}

const paymentColumns = `id,idempotency_key,source_account_id,destination_account_id,amount_minor,currency,status,transaction_id,created_at,updated_at`

func scanPayment(row pgx.Row) (*domain.PaymentIntent, error) {
	var p domain.PaymentIntent
	err := row.Scan(&p.ID, &p.IdempotencyKey, &p.SourceAccountID, &p.DestinationAccountID, &p.AmountMinor, &p.Currency, &p.Status, &p.TransactionID, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// The stable transaction ID is committed before calling ledger-service.
func (r *paymentRepository) Create(ctx context.Context, p *domain.PaymentIntent) error {
	if p.TransactionID == nil {
		p.SetTransactionID(uuid.New())
	}
	tag, err := r.pool.Exec(ctx, `INSERT INTO payment_intents (`+paymentColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(idempotency_key) DO NOTHING`, p.ID, p.IdempotencyKey, p.SourceAccountID, p.DestinationAccountID, p.AmountMinor, p.Currency, p.Status, p.TransactionID, p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		existing, err := r.GetByIdempotencyKey(ctx, p.IdempotencyKey)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("conflicting payment disappeared")
		}
		*p = *existing
	}
	return nil
}
func (r *paymentRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.PaymentIntent, error) {
	return scanPayment(r.pool.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payment_intents WHERE id=$1`, id))
}
func (r *paymentRepository) GetByIdempotencyKey(ctx context.Context, key string) (*domain.PaymentIntent, error) {
	return scanPayment(r.pool.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payment_intents WHERE idempotency_key=$1`, key))
}
func (r *paymentRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status string) error {
	_, err := r.pool.Exec(ctx, `UPDATE payment_intents SET status=$1,updated_at=now() WHERE id=$2`, status, id)
	return err
}

// Process serializes concurrent retries of a payment. Remote ledger writes use
// the already persisted ID, so a rollback after a lost reply remains recoverable.
func (r *paymentRepository) Process(ctx context.Context, id uuid.UUID, h func(pgx.Tx, *domain.PaymentIntent) error) (*domain.PaymentIntent, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	p, err := scanPayment(tx.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payment_intents WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("payment not found")
	}
	if p.TransactionID == nil && !p.IsTerminal() {
		if p.Status != domain.StatusCreated {
			return nil, sharederrors.New(sharederrors.KindFailedPrecondition, "legacy payment has an unknown ledger transaction; reconcile it before retrying")
		}
		// Legacy pending rows also need an ID that survives a local rollback.
		p.SetTransactionID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("payment-ledger:"+p.ID.String())))
		if _, err := tx.Exec(ctx, `UPDATE payment_intents SET transaction_id=$1 WHERE id=$2`, p.TransactionID, p.ID); err != nil {
			return nil, err
		}
	}
	if err := h(tx, p); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// Complete records an acknowledged ledger commit and its notification together.
func (r *paymentRepository) Complete(ctx context.Context, tx pgx.Tx, p *domain.PaymentIntent) error {
	if p.Status == domain.StatusCompleted {
		return nil
	}
	if p.TransactionID == nil {
		return fmt.Errorf("completed payment requires a ledger transaction ID")
	}
	if err := tx.QueryRow(ctx, `UPDATE payment_intents SET status='completed',transaction_id=$1,updated_at=now() WHERE id=$2 RETURNING updated_at`, p.TransactionID, p.ID).Scan(&p.UpdatedAt); err != nil {
		return err
	}
	event := map[string]any{"payment_id": p.ID.String(), "payment_key": p.IdempotencyKey, "transaction_id": p.TransactionID.String(), "source_account_id": p.SourceAccountID.String(), "destination_account_id": p.DestinationAccountID.String(), "amount_minor": p.AmountMinor, "currency": p.Currency}
	if err := eventbus.InsertOutbox(ctx, tx, "payment.completed", p.ID.String(), event); err != nil {
		return err
	}
	p.Status = domain.StatusCompleted
	return nil
}

// Reconcile is called inside the consumer's processed-events transaction.
func (r *paymentRepository) Reconcile(ctx context.Context, tx pgx.Tx, txID uuid.UUID) error {
	p, err := scanPayment(tx.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payment_intents WHERE transaction_id=$1 FOR UPDATE`, txID))
	if err != nil {
		return err
	}
	if p == nil {
		return nil
	} // Deposits and unrelated ledger transactions.
	return r.Complete(ctx, tx, p)
}
