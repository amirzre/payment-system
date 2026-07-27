package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/amirzre/payment-system/services/auth/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UserRepository defines the persistence interface for users.
type UserRepository interface {
	Create(ctx context.Context, user *domain.User) error
	GetByEmail(ctx context.Context, email string) (*domain.User, error)
}

// userRepository implements the user repository using Postgres.
type userRepository struct {
	pool *pgxpool.Pool
}

// NewUserRepository creates a new UserRepository.
func NewUserRepository(pool *pgxpool.Pool) UserRepository {
	return &userRepository{pool: pool}
}

// Create inserts a new user.
func (r *userRepository) Create(ctx context.Context, user *domain.User) error {
	query := `
		INSERT INTO users (id, email, password_hash, created_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err := r.pool.Exec(ctx, query, user.ID, user.Email, user.PasswordHash, user.CreatedAt)
	if err != nil {
		return fmt.Errorf("inserting user: %w", err)
	}

	return nil
}

// GetByEmail retrieves a user by email.
func (r *userRepository) GetByEmail(ctx context.Context, email string) (*domain.User, error) {
	query := `
		SELECT id, email, created_at
		FROM users
		WHERE email = $1
	`
	var user domain.User
	err := r.pool.QueryRow(ctx, query, email).Scan(
		&user.ID,
		&user.Email,
		&user.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("querying user by email: %w", err)
	}

	return &user, nil
}
