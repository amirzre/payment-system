package grpc

import (
	"context"

	accountv1 "github.com/amirzre/payment-system/gen/go/account/v1"
	"github.com/amirzre/payment-system/services/account/internal/app"
	"github.com/amirzre/payment-system/services/account/internal/domain"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
	"github.com/google/uuid"
)

// AccountServer implements the generated AccountServiceServer interface.
type AccountServer struct {
	accountv1.UnimplementedAccountServiceServer
	service app.AccountService
}

// NewAccountServer creates a new AccountServer.
func NewAccountServer(service app.AccountService) *AccountServer {
	return &AccountServer{service: service}
}

// CreateAccount handles the CreateAccount gRPC request.
func (s *AccountServer) CreateAccount(ctx context.Context, req *accountv1.CreateAccountRequest) (*accountv1.CreateAccountResponse, error) {
	account, err := s.service.Create(ctx, app.CreateAccountReq{
		OwnerRef: req.OwnerRef,
		Currency: req.Currency,
	})
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &accountv1.CreateAccountResponse{
		Account: domainToProto(account),
	}, nil
}

// GetAccount handles the GetAccount gRPC request.
func (s *AccountServer) GetAccount(ctx context.Context, req *accountv1.GetAccountRequest) (*accountv1.GetAccountResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "invalid account id"),
		)
	}

	account, err := s.service.GetByID(ctx, id)
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &accountv1.GetAccountResponse{
		Account: domainToProto(account),
	}, nil
}

// domainToProto converts a domain Account to its proto representation.
func domainToProto(a *domain.Account) *accountv1.Account {
	return &accountv1.Account{
		Id:        a.ID.String(),
		OwnerRef:  a.OwnerRef,
		Currency:  a.Currency,
		Status:    a.Status,
		CreatedAt: a.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}
