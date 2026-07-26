package grpc

import (
	"context"

	"github.com/amirzre/payment-system/services/ledger/internal/app"
	"github.com/google/uuid"

	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
)

// LedgerServer implements the generated LedgerServiceServer interface.
type LedgerServer struct {
	ledgerv1.UnimplementedLedgerServiceServer
	service app.LedgerService
}

// NewLedgerServer creates a new LedgerServer.
func NewLedgerServer(service app.LedgerService) *LedgerServer {
	return &LedgerServer{
		service: service,
	}
}

// PostEntries handles the PostEntries gRPC request.
func (s *LedgerServer) PostEntries(ctx context.Context, req *ledgerv1.PostEntriesRequest) (*ledgerv1.PostEntriesResponse, error) {
	txID, err := uuid.Parse(req.GetTransactionId())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "invalid transaction_id"),
		)
	}

	entries := req.GetEntries()
	if len(entries) != 2 {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "exactly 2 entries required (one debit, one credit)"),
		)
	}

	var sourceID, destID uuid.UUID
	var amountMinor int64
	var currency string

	for _, e := range entries {
		switch e.GetDirection() {
		case ledgerv1.LedgerDirection_LEDGER_DIRECTION_DEBIT:
			sourceID, err = uuid.Parse(e.GetAccountId())
			if err != nil {
				return nil, sharederrors.ToGRPCStatus(
					sharederrors.New(sharederrors.KindInvalidArgument, "invalid source account_id"),
				)
			}
			amountMinor = e.GetAmountMinor()
			currency = e.GetCurrency()
		case ledgerv1.LedgerDirection_LEDGER_DIRECTION_CREDIT:
			destID, err = uuid.Parse(e.GetAccountId())
			if err != nil {
				return nil, sharederrors.ToGRPCStatus(
					sharederrors.New(sharederrors.KindInvalidArgument, "invalid destination account_id"),
				)
			}
		default:
			return nil, sharederrors.ToGRPCStatus(
				sharederrors.New(sharederrors.KindInvalidArgument, "direction must be 'debit' or 'credit'"),
			)
		}
	}

	if sourceID == uuid.Nil || destID == uuid.Nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "both debit and credit entries are required"),
		)
	}

	if sourceID == uuid.Nil || destID == uuid.Nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "both debit and credit entries are required"),
		)
	}

	err = s.service.PostEntries(ctx, app.PostEntriesInput{
		TransactionID:   txID,
		SourceAccountID: sourceID,
		DestAccountID:   destID,
		AmountMinor:     amountMinor,
		Currency:        currency,
	})
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &ledgerv1.PostEntriesResponse{Success: true}, nil
}

// GetBalance handles the GetBalance gRPC request.
func (s *LedgerServer) GetBalance(ctx context.Context, req *ledgerv1.GetBalanceRequest) (*ledgerv1.GetBalanceResponse, error) {
	accountID, err := uuid.Parse(req.GetAccountId())
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(
			sharederrors.New(sharederrors.KindInvalidArgument, "invalid account_id"),
		)
	}

	balance, err := s.service.GetBalance(ctx, accountID)
	if err != nil {
		return nil, sharederrors.ToGRPCStatus(err)
	}

	return &ledgerv1.GetBalanceResponse{
		AccountId:    accountID.String(),
		BalanceMinor: balance,
	}, nil
}
