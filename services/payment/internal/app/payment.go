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
	"github.com/google/uuid"
)

// PaymentService defines the interface for payment business logic.
type PaymentService interface {
	Create(ctx context.Context, input CreatePaymentInput) (*domain.PaymentIntent, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.PaymentIntent, error)
}

// paymentService implements PaymentService interface.
type paymentService struct {
	repo          repository.PaymentRepository
	accountClient accountv1.AccountServiceClient
	ledgerClient  ledgerv1.LedgerServiceClient
}

// NewLedgerEntryService creates a new ledger service instance.
func NewLedgerEntryService(
	repo repository.PaymentRepository,
	accountClient accountv1.AccountServiceClient,
	ledgerClient ledgerv1.LedgerServiceClient,
) PaymentService {
	return &paymentService{
		repo:          repo,
		accountClient: accountClient,
		ledgerClient:  ledgerClient,
	}
}

// CreatePaymentInput holds the data needed to create a payment.
type CreatePaymentInput struct {
	IdempotencyKey       string
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	AmountMinor          int64
	Currency             string
}

// Create orchestrates the synchronous payment flow.
func (s *paymentService) Create(ctx context.Context, input CreatePaymentInput) (*domain.PaymentIntent, error) {
	existing, err := s.repo.GetByIdempotencyKey(ctx, input.IdempotencyKey)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "checking idempotency", err)
	}
	if existing != nil {
		return existing, nil
	}

	intent, err := domain.NewPaymentIntent(
		input.IdempotencyKey,
		input.SourceAccountID,
		input.DestinationAccountID,
		input.AmountMinor,
		input.Currency,
	)
	if err != nil {
		return nil, sharederrors.New(sharederrors.KindInvalidArgument, err.Error())
	}

	if err := s.repo.Create(ctx, intent); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "creating payment intent", err)
	}

	balanceCtx, balanceCancel := context.WithTimeout(ctx, 2*time.Second)
	defer balanceCancel()

	balanceResp, err := s.accountClient.GetBalance(balanceCtx, &accountv1.GetBalanceRequest{
		AccountId: input.SourceAccountID.String(),
	})
	if err != nil {
		s.repo.UpdateStatus(ctx, intent.ID, domain.StatusFailed)
		return nil, sharederrors.Wrap(sharederrors.KindFailedPrecondition,
			fmt.Sprintf("checking balance for account %s", input.SourceAccountID), err)
	}

	if balanceResp.GetBalanceMinor() < input.AmountMinor {
		s.repo.UpdateStatus(ctx, intent.ID, domain.StatusFailed)
		return nil, sharederrors.New(sharederrors.KindFailedPrecondition,
			fmt.Sprintf("insufficient funds: available %d, required %d",
				balanceResp.GetBalanceMinor(), input.AmountMinor))
	}

	if err := intent.Transition(domain.StatusFundsReserved); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "state transition", err)
	}
	if err := s.repo.UpdateStatus(ctx, intent.ID, intent.Status); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "updating status to funds_reserved", err)
	}

	ledgerCtx, ledgerCancel := context.WithTimeout(ctx, 2*time.Second)
	defer ledgerCancel()

	txID := uuid.New()
	_, err = s.ledgerClient.PostEntries(ledgerCtx, &ledgerv1.PostEntriesRequest{
		TransactionId: txID.String(),
		Entries: []*ledgerv1.LedgerEntry{
			{
				AccountId:   input.SourceAccountID.String(),
				Direction:   ledgerv1.LedgerDirection_LEDGER_DIRECTION_DEBIT,
				AmountMinor: input.AmountMinor,
				Currency:    input.Currency,
			},
			{
				AccountId:   input.DestinationAccountID.String(),
				Direction:   ledgerv1.LedgerDirection_LEDGER_DIRECTION_CREDIT,
				AmountMinor: input.AmountMinor,
				Currency:    input.Currency,
			},
		},
	})
	if err != nil {
		s.repo.UpdateStatus(ctx, intent.ID, domain.StatusPaymentFailed)
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "posting ledger entries", err)
	}

	intent.SetTransactionID(txID)

	if err := intent.Transition(domain.StatusLedgerPosted); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "state transition", err)
	}
	if err := s.repo.UpdateStatus(ctx, intent.ID, intent.Status); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "updating status to ledger_posted", err)
	}

	if err := intent.Transition(domain.StatusCompleted); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "state transition", err)
	}
	if err := s.repo.UpdateStatus(ctx, intent.ID, intent.Status); err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "updating status to completed", err)
	}

	return intent, nil
}

// GetByID retrieves a payment by its ID.
func (s *paymentService) GetByID(ctx context.Context, id uuid.UUID) (*domain.PaymentIntent, error) {
	intent, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, sharederrors.Wrap(sharederrors.KindInternal, "failed to get payment", err)
	}
	if intent == nil {
		return nil, sharederrors.New(sharederrors.KindNotFound, fmt.Sprintf("payment %s not found", id))
	}

	return intent, nil
}
