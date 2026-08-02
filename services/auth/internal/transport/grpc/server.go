package grpc

import (
	"context"

	"github.com/amirzre/payment-system/services/auth/internal/app"

	authv1 "github.com/amirzre/payment-system/gen/go/auth/v1"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
)

// AuthServer implements the generated AuthServiceServer interface.
type AuthServer struct {
	authv1.UnimplementedAuthServiceServer
	service app.AuthService
}

// NewAuthServer creates a new AuthServer.
func NewAuthServer(service app.AuthService) *AuthServer {
	return &AuthServer{service: service}
}

// Register handles the Register gRPC request.
func (s *AuthServer) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	_, err := s.service.RegisterUser(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &authv1.RegisterResponse{}, nil
}

// Login handles the Login gRPC request.
func (s *AuthServer) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	tokens, err := s.service.Login(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &authv1.LoginResponse{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresIn:    tokens.ExpiresIn,
	}, nil
}

// VerifyToken handles the VerifyToken gRPC request.
func (s *AuthServer) VerifyToken(ctx context.Context, req *authv1.VerifyTokenRequest) (*authv1.VerifyTokenResponse, error) {
	valid, userID := s.service.Verify(req.GetToken())

	return &authv1.VerifyTokenResponse{
		Valid:  valid,
		UserId: userID,
	}, nil
}
