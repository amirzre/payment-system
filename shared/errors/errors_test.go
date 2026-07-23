package errors

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "NotFound",
			err:      New(KindNotFound, "resource not found"),
			wantCode: codes.NotFound,
		},
		{
			name:     "InvalidArgument",
			err:      New(KindInvalidArgument, "invalid input"),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "AlreadyExists",
			err:      New(KindAlreadyExists, "already exists"),
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "FailedPrecondition",
			err:      New(KindFailedPrecondition, "precondition failed"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "Internal",
			err:      New(KindInternal, "internal error"),
			wantCode: codes.Internal,
		},
		{
			name:     "non-Error falls back to Internal",
			err:      errors.New("plain error"),
			wantCode: codes.Internal,
		},
		{
			name:     "wrapped Error",
			err:      Wrap(KindNotFound, "account not found", errors.New("db error")),
			wantCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grpcErr := ToGRPCStatus(tt.err)
			st, ok := status.FromError(grpcErr)
			if !ok {
				t.Fatal("expected gRPC status error")
			}
			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
			}
		})
	}
}

func TestErrorUnwrap(t *testing.T) {
	cause := errors.New("original")
	err := Wrap(KindInternal, "wrapped", cause)

	if !errors.Is(err, cause) {
		t.Error("expected errors.Is to find the wrapped cause")
	}

	var e *Error
	if !errors.As(err, &e) {
		t.Fatal("expected errors.As to find *Error")
	}
	if e.Kind != KindInternal {
		t.Errorf("expected Kind=Internal, got %v", e.Kind)
	}
}

func TestIs(t *testing.T) {
	err := New(KindNotFound, "not found")

	if !Is(err, KindNotFound) {
		t.Error("expected Is with KindNotFound to return true")
	}
	if Is(err, KindInternal) {
		t.Error("expected Is with KindInternal to return false")
	}
}
