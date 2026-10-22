package grpc

import (
	"context"
	"testing"

	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	"github.com/amirzre/payment-system/services/ledger/internal/app"
	"github.com/amirzre/payment-system/services/ledger/internal/domain"
	"github.com/amirzre/payment-system/services/ledger/internal/repository"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingLedgerService struct {
	app.LedgerService
	posts []app.PostEntriesInput
}

func (s *recordingLedgerService) PostEntries(_ context.Context, input app.PostEntriesInput) error {
	s.posts = append(s.posts, input)
	return nil
}

func validPostEntries() *ledgerv1.PostEntriesRequest {
	return &ledgerv1.PostEntriesRequest{TransactionId: "11111111-1111-4111-8111-111111111111", Entries: []*ledgerv1.LedgerEntry{
		{AccountId: "22222222-2222-4222-8222-222222222222", Direction: ledgerv1.LedgerDirection_LEDGER_DIRECTION_DEBIT, AmountMinor: 100, Currency: "USD"},
		{AccountId: "33333333-3333-4333-8333-333333333333", Direction: ledgerv1.LedgerDirection_LEDGER_DIRECTION_CREDIT, AmountMinor: 100, Currency: "USD"},
	}}
}

func TestPostEntriesRejectsMalformedPairsBeforePosting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ledgerv1.PostEntriesRequest)
	}{
		{"credit amount differs", func(r *ledgerv1.PostEntriesRequest) { r.Entries[1].AmountMinor = 101 }},
		{"credit currency differs", func(r *ledgerv1.PostEntriesRequest) { r.Entries[1].Currency = "EUR" }},
		{"credit amount negative", func(r *ledgerv1.PostEntriesRequest) { r.Entries[1].AmountMinor = -100 }},
		{"zero amounts", func(r *ledgerv1.PostEntriesRequest) { r.Entries[0].AmountMinor = 0; r.Entries[1].AmountMinor = 0 }},
		{"invalid currency", func(r *ledgerv1.PostEntriesRequest) { r.Entries[0].Currency = ""; r.Entries[1].Currency = "" }},
		{"two debits", func(r *ledgerv1.PostEntriesRequest) {
			r.Entries[1].Direction = ledgerv1.LedgerDirection_LEDGER_DIRECTION_DEBIT
		}},
		{"two credits", func(r *ledgerv1.PostEntriesRequest) {
			r.Entries[0].Direction = ledgerv1.LedgerDirection_LEDGER_DIRECTION_CREDIT
		}},
		{"nil entry", func(r *ledgerv1.PostEntriesRequest) { r.Entries[1] = nil }},
		{"invalid direction", func(r *ledgerv1.PostEntriesRequest) {
			r.Entries[1].Direction = ledgerv1.LedgerDirection_LEDGER_DIRECTION_UNSPECIFIED
		}},
		{"nil transaction UUID", func(r *ledgerv1.PostEntriesRequest) { r.TransactionId = uuid.Nil.String() }},
		{"nil account UUID", func(r *ledgerv1.PostEntriesRequest) { r.Entries[1].AccountId = uuid.Nil.String() }},
		{"invalid account UUID", func(r *ledgerv1.PostEntriesRequest) { r.Entries[1].AccountId = "bad-id" }},
		{"missing credit", func(r *ledgerv1.PostEntriesRequest) { r.Entries = r.Entries[:1] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := validPostEntries()
			tc.change(req)
			service := &recordingLedgerService{}
			resp, err := NewLedgerServer(service).PostEntries(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument || resp != nil {
				t.Fatalf("response = %v, error = %v, want InvalidArgument", resp, err)
			}
			if len(service.posts) != 0 {
				t.Fatalf("malformed request posted %d transactions", len(service.posts))
			}
		})
	}
}

func TestPostEntriesAcceptsBalancedPairInEitherOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		req := validPostEntries()
		if reverse {
			req.Entries[0], req.Entries[1] = req.Entries[1], req.Entries[0]
		}
		service := &recordingLedgerService{}
		resp, err := NewLedgerServer(service).PostEntries(context.Background(), req)
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("response = %v, error = %v", resp, err)
		}
		if len(service.posts) != 1 {
			t.Fatalf("posts = %d, want 1", len(service.posts))
		}
		post := service.posts[0]
		if post.TransactionID.String() != "11111111-1111-4111-8111-111111111111" || post.SourceAccountID.String() != "22222222-2222-4222-8222-222222222222" || post.DestAccountID.String() != "33333333-3333-4333-8333-333333333333" || post.AmountMinor != 100 || post.Currency != "USD" {
			t.Fatalf("wrong posting: %+v", post)
		}
	}
}

type recordingLedgerRepository struct {
	repository.LedgerRepository
	posts int
}

func (r *recordingLedgerRepository) PostEntries(context.Context, uuid.UUID, []domain.LedgerEntry) error {
	r.posts++
	return nil
}

func TestPostEntriesRejectsSameAccountThroughApplicationWithoutPersisting(t *testing.T) {
	req := validPostEntries()
	req.Entries[1].AccountId = req.Entries[0].AccountId
	repo := &recordingLedgerRepository{}
	server := NewLedgerServer(app.NewLedgerEntryService(repo))
	resp, err := server.PostEntries(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument || resp != nil {
		t.Fatalf("response = %v, error = %v, want InvalidArgument", resp, err)
	}
	if repo.posts != 0 {
		t.Fatalf("invalid same-account pair persisted %d times", repo.posts)
	}
}

func TestPostEntriesRejectsNilRequestWithoutPosting(t *testing.T) {
	service := &recordingLedgerService{}
	resp, err := NewLedgerServer(service).PostEntries(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument || resp != nil {
		t.Fatalf("response = %v, error = %v, want InvalidArgument", resp, err)
	}
	if len(service.posts) != 0 {
		t.Fatal("nil request posted a transaction")
	}
}
