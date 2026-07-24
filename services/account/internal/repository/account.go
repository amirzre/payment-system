package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/amirzre/payment-system/services/account/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AccountRepository defines the persistence interface for accounts.
type AccountRepository interface {
	Create(ctx context.Context, account *domain.Account) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Account, error)
}

// accountRepository implements repository.
type accountRepository struct {
	pool *pgxpool.Pool
}

// NewAccountRepository creates a new AccountRepo.
func NewAccountRepository(pool *pgxpool.Pool) AccountRepository {
	return &accountRepository{pool: pool}
}

// Create inserts a new account into the database.
func (r *accountRepository) Create(ctx context.Context, account *domain.Account) error {
	query := `
		INSERT INTO accounts (id, owner_ref, currency, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.pool.Exec(
		ctx,
		query,
		account.ID,
		account.OwnerRef,
		account.Currency,
		account.Status,
		account.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("inserting account: %w", err)
	}

	return nil
}

// GetByID retrieves an account by its ID.
func (r *accountRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	query := `
		SELECT id, owner_ref, currency, status, created_at
		FROM accounts
		WHERE id = $1
	`
	var account domain.Account
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&account.ID,
		&account.OwnerRef,
		&account.Currency,
		&account.Status,
		&account.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("querying account by id: %w", err)
	}

	return &account, nil
}
