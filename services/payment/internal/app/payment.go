package app

import (
	"context"
	"fmt"
	"time"

	accountv1 "github.com/amirzre/payment-system/gen/go/account/v1"
	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	"github.com/amirzre/payment-system/services/payment/internal/domain"
	"github.com/amirzre/payment-system/services/payment/internal/repository"
	sharederrors "github.com/amirzre/payment-system/shared/errors"
	"github.com/amirzre/payment-system/shared/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PaymentService interface {
	Create(context.Context, CreatePaymentInput) (*domain.PaymentIntent, error)
	GetByID(context.Context, uuid.UUID) (*domain.PaymentIntent, error)
}
type paymentService struct {
	repo          repository.PaymentRepository
	accountClient accountv1.AccountServiceClient
	ledgerClient  ledgerv1.LedgerServiceClient
	idempotency   *middleware.RedisIdempotency
}

func NewPaymentService(repo repository.PaymentRepository, account accountv1.AccountServiceClient, ledger ledgerv1.LedgerServiceClient, idempotency *middleware.RedisIdempotency) PaymentService {
	return &paymentService{repo, account, ledger, idempotency}
}

type CreatePaymentInput struct {
	IdempotencyKey       string
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	AmountMinor          int64
	Currency             string
}

func (s *paymentService) Create(ctx context.Context, input CreatePaymentInput) (*domain.PaymentIntent, error) {
	ctx, operationSpan := otel.Tracer("payment-system/payment").Start(ctx, "payment.create")
	defer operationSpan.End()
	p, err := domain.NewPaymentIntent(
		input.IdempotencyKey,
		input.SourceAccountID,
		input.DestinationAccountID,
		input.AmountMinor,
		input.Currency,
	)
	if err != nil {
		return nil, sharederrors.New(sharederrors.KindInvalidArgument, err.Error())
	}
	// Redis is advisory only. PostgreSQL serializes creation and processing.
	if s.idempotency != nil {
		_, _ = s.idempotency.CheckAndSet(ctx, input.IdempotencyKey)
	}
	if err := s.repo.Create(ctx, p); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "creating payment intent", err)
	}
	if p.SourceAccountID != input.SourceAccountID || p.DestinationAccountID != input.DestinationAccountID || p.AmountMinor != input.AmountMinor || p.Currency != input.Currency {
		return nil, sharederrors.New(
			sharederrors.KindAlreadyExists,
			"idempotency key already used with different payment parameters",
		)
	}
	var businessErr error
	result, err := s.repo.Process(ctx, p.ID, func(tx pgx.Tx, current *domain.PaymentIntent) error {
		if current.IsTerminal() || current.Status == domain.StatusPosting {
			return nil
		}
		if current.Status != domain.StatusCreated {
			// A legacy attempt with a known transaction ID can safely replay it.
			return nil
		}
		for _, id := range []uuid.UUID{current.SourceAccountID, current.DestinationAccountID} {
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			response, callErr := s.accountClient.GetAccount(callCtx, &accountv1.GetAccountRequest{Id: id.String()})
			cancel()
			if callErr != nil {
				return callErr
			}
			account := response.GetAccount()
			if account == nil || account.GetStatus() != "active" || account.GetCurrency() != current.Currency {
				businessErr = sharederrors.New(
					sharederrors.KindFailedPrecondition,
					"both accounts must be active and use the payment currency",
				)
				_, err := tx.Exec(ctx, `UPDATE payment_intents SET status='failed',updated_at=now() WHERE id=$1`, current.ID)
				current.Status = domain.StatusFailed
				return err
			}
		}
		// Commit validation before any remote write. A retry after a lost ledger
		// reply must replay that write, even if account metadata has since changed.
		_, err := tx.Exec(ctx, `UPDATE payment_intents SET status='posting',updated_at=now() WHERE id=$1`, current.ID)
		current.Status = domain.StatusPosting
		return err
	})
	if err != nil {
		return nil, err
	}
	if businessErr != nil {
		return nil, businessErr
	}
	if result.IsTerminal() {
		return result, nil
	}
	result, err = s.repo.Process(ctx, p.ID, func(tx pgx.Tx, current *domain.PaymentIntent) error {
		if current.IsTerminal() {
			return nil
		}
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, callErr := s.ledgerClient.PostEntries(callCtx, &ledgerv1.PostEntriesRequest{
			TransactionId: current.TransactionID.String(),
			Entries: []*ledgerv1.LedgerEntry{
				{
					AccountId:   current.SourceAccountID.String(),
					Direction:   ledgerv1.LedgerDirection_LEDGER_DIRECTION_DEBIT,
					AmountMinor: current.AmountMinor,
					Currency:    current.Currency,
				},
				{
					AccountId:   current.DestinationAccountID.String(),
					Direction:   ledgerv1.LedgerDirection_LEDGER_DIRECTION_CREDIT,
					AmountMinor: current.AmountMinor,
					Currency:    current.Currency,
				},
			},
		})
		if callErr != nil {
			// A timeout/unavailable response says nothing about whether ledger
			// committed. Leave the payment retryable and reconcile via its event.
			if status.Code(callErr) == codes.FailedPrecondition || status.Code(callErr) == codes.InvalidArgument {
				businessErr = sharederrors.New(sharederrors.KindFailedPrecondition, status.Convert(callErr).Message())
				_, err := tx.Exec(ctx, `UPDATE payment_intents SET status='failed',updated_at=now() WHERE id=$1`, current.ID)
				current.Status = domain.StatusFailed
				return err
			}
			return callErr
		}
		return s.repo.Complete(ctx, tx, current)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, sharederrors.Wrap(
			sharederrors.KindInternal,
			"processing payment; retry using the same idempotency key",
			err,
		)
	}
	if businessErr != nil {
		return nil, businessErr
	}
	return result, nil
}

func (s *paymentService) GetByID(ctx context.Context, id uuid.UUID) (*domain.PaymentIntent, error) {
	ctx, operationSpan := otel.Tracer("payment-system/payment").Start(ctx, "payment.get")
	defer operationSpan.End()
	p, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "failed to get payment", err)
	}
	if p == nil {
		return nil, sharederrors.New(sharederrors.KindNotFound, fmt.Sprintf("payment %s not found", id))
	}
	return p, nil
}
