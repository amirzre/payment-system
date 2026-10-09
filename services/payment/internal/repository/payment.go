package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/amirzre/payment-system/services/payment/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PaymentRepository defines the persistence interface for payments.
type PaymentRepository interface {
	Create(ctx context.Context, payment *domain.PaymentIntent) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.PaymentIntent, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.PaymentIntent, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status string) error
}

// paymentRepository implements PaymentRepository using Postgres.
type paymentRepository struct {
	pool *pgxpool.Pool
}

// NewPaymentRepository creates a new PaymentRepository.
func NewPaymentRepository(pool *pgxpool.Pool) PaymentRepository {
	return &paymentRepository{pool: pool}
}

// Create inserts a new payment intent.
func (r *paymentRepository) Create(ctx context.Context, payment *domain.PaymentIntent) error {
	query := `
		INSERT INTO payment_intents (id, idempotency_key, source_account_id, destination_account_id,
			amount_minor, currency, status, transaction_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := r.pool.Exec(
		ctx,
		query,
		payment.ID,
		payment.IdempotencyKey,
		payment.SourceAccountID,
		payment.DestinationAccountID,
		payment.AmountMinor,
		payment.Currency,
		payment.Status,
		payment.TransactionID,
		payment.CreatedAt,
		payment.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("inserting payment intent: %w", err)
	}

	return nil
}

// GetByID retrieves a payment intent by its ID.
func (r *paymentRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.PaymentIntent, error) {
	query := `
		SELECT id, idempotency_key, source_account_id, destination_account_id,
			amount_minor, currency, status, transaction_id, created_at, updated_at
		FROM payment_intents
		WHERE id = $1
	`
	var payment domain.PaymentIntent
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&payment.ID,
		&payment.IdempotencyKey,
		&payment.SourceAccountID,
		&payment.DestinationAccountID,
		&payment.AmountMinor,
		&payment.Currency,
		&payment.Status,
		&payment.TransactionID,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("querying payment intent by id: %w", err)
	}

	return &payment, nil
}

// GetByIdempotencyKey retrieves a payment intent by its idempotency key.
func (r *paymentRepository) GetByIdempotencyKey(ctx context.Context, key string) (*domain.PaymentIntent, error) {
	query := `
		SELECT id, idempotency_key, source_account_id, destination_account_id,
			amount_minor, currency, status, transaction_id, created_at, updated_at
		FROM payment_intents
		WHERE idempotency_key = $1
	`
	var payment domain.PaymentIntent
	err := r.pool.QueryRow(ctx, query, key).Scan(
		&payment.ID,
		&payment.IdempotencyKey,
		&payment.SourceAccountID,
		&payment.DestinationAccountID,
		&payment.AmountMinor,
		&payment.Currency,
		&payment.Status,
		&payment.TransactionID,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("querying payment intent by key: %w", err)
	}

	return &payment, nil
}

// UpdateStatus updates the status (and transaction_id) of a payment intent.
func (r *paymentRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status string) error {
	query := `
		UPDATE payment_intents
		SET status = $1, updated_at = now()
		WHERE id = $2
	`
	_, err := r.pool.Exec(ctx, query, status, id)
	if err != nil {
		return fmt.Errorf("updating payment status: %w", err)
	}

	return nil
}
