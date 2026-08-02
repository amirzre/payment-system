package app

import (
	"context"

	"github.com/amirzre/payment-system/services/auth/internal/domain"
	"github.com/amirzre/payment-system/services/auth/internal/repository"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
)

// AuthService defines the interface for auth business logic.
type AuthService interface {
	RegisterUser(ctx context.Context, email, password string) (*domain.TokenPair, error)
	Login(ctx context.Context, email, password string) (*domain.TokenPair, error)
	Verify(token string) (bool, string)
}

// authService implements AuthService interface.
type authService struct {
	repo         repository.UserRepository
	tokenService *domain.TokenService
}

// NewAuthService creates a new auth service instance.
func NewAuthService(repo repository.UserRepository, tokenService *domain.TokenService) AuthService {
	return &authService{repo: repo, tokenService: tokenService}
}

// RegisterUser registers a new user and returns a token pair.
func (s *authService) RegisterUser(ctx context.Context, email, password string) (*domain.TokenPair, error) {
	existing, err := s.repo.GetByEmail(ctx, email)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "checking existing user", err)
	}
	if existing != nil {
		return nil, sharederrors.New(sharederrors.KindAlreadyExists, "email already registered")
	}

	user, err := domain.NewUser(email, password)
	if err != nil {
		return nil, sharederrors.New(sharederrors.KindInvalidArgument, err.Error())
	}

	if err := s.repo.Create(ctx, user); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "persisting user", err)
	}

	tokens, err := s.tokenService.IssueToken(user.ID)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "issuing tokens", err)
	}

	return tokens, nil
}

// Login authenticates a user and returns a token pair.
func (s *authService) Login(ctx context.Context, email, password string) (*domain.TokenPair, error) {
	user, err := s.repo.GetByEmail(ctx, email)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "querying user", err)
	}
	if user == nil {
		return nil, sharederrors.New(sharederrors.KindNotFound, "invalid email or password")
	}

	if !user.VerifyPassword(password) {
		return nil, sharederrors.New(sharederrors.KindInvalidArgument, "invalid email or password")
	}

	tokens, err := s.tokenService.IssueToken(user.ID)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "issuing tokens", err)
	}

	return tokens, nil
}

// Verify verifies a token and returns the user ID if valid.
func (s *authService) Verify(token string) (bool, string) {
	return s.tokenService.VerifyToken(token)
}
