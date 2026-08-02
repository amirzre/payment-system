package grpc

import (
	"context"

	paymentv1 "github.com/amirzre/payment-system/gen/go/payment/v1"
	"github.com/amirzre/payment-system/services/payment/internal/app"
	"github.com/amirzre/payment-system/services/payment/internal/domain"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
	"github.com/google/uuid"
)

// PaymentServer implements the generated PaymentServiceServer interface.
type PaymentServer struct {
	paymentv1.UnimplementedPaymentServiceServer
	service app.PaymentService
}

// NewPaymentServer creates a new PaymentServer.
func NewPaymentServer(service app.PaymentService) *PaymentServer {
	return &PaymentServer{
		service: service,
	}
}

// CreatePayment handles the CreatePayment gRPC request.
func (s *PaymentServer) CreatePayment(ctx context.Context, req *paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	sourceID, err := uuid.Parse(req.GetSourceAccountId())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "invalid source_account_id"),
		)
	}
	destID, err := uuid.Parse(req.GetDestinationAccountId())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "invalid destination_account_id"),
		)
	}

	intent, err := s.service.Create(ctx, app.CreatePaymentInput{
		IdempotencyKey:       req.GetIdempotencyKey(),
		SourceAccountID:      sourceID,
		DestinationAccountID: destID,
		AmountMinor:          req.GetAmountMinor(),
		Currency:             req.GetCurrency(),
	})
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &paymentv1.CreatePaymentResponse{
		Payment: domainToProto(intent),
	}, nil
}

// GetPayment handles the GetPayment gRPC request.
func (s *PaymentServer) GetPayment(ctx context.Context, req *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "invalid payment id"),
		)
	}

	intent, err := s.service.GetByID(ctx, id)
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &paymentv1.GetPaymentResponse{
		Payment: domainToProto(intent),
	}, nil
}

// domainToProto converts a domain PaymentIntent to its proto representation.
func domainToProto(p *domain.PaymentIntent) *paymentv1.Payment {
	proto := &paymentv1.Payment{
		Id:                   p.ID.String(),
		IdempotencyKey:       p.IdempotencyKey,
		SourceAccountId:      p.SourceAccountID.String(),
		DestinationAccountId: p.DestinationAccountID.String(),
		AmountMinor:          p.AmountMinor,
		Currency:             p.Currency,
		Status:               p.Status,
		CreatedAt:            p.CreatedAt.Format("2006-01-02T15:04:05Z"),
		UpdatedAt:            p.UpdatedAt.Format("2006-01-02T15:04:05Z"),
	}
	return proto
}
