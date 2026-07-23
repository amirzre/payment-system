package errors

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Kind represents the category of an error, shared across all services.
type Kind int

const (
	KindNotFound Kind = iota
	KindInvalidArgument
	KindAlreadyExists
	KindFailedPrecondition
	KindInternal
)

// Error is a structured domain error used across service boundaries.
type Error struct {
	Kind    Kind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}

	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Err
}

// New creates a new Error.
func New(kind Kind, message string) *Error {
	return &Error{Kind: kind, Message: message}
}

// Wrap creates a new Error wrapping an existing error.
func Wrap(kind Kind, message string, err error) *Error {
	return &Error{Kind: kind, Message: message, Err: err}
}

// Is checks if an error is of a given Kind.
func Is(err error, kind Kind) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind == kind
	}
	return false
}

// ToGRPCStatus maps a shared Error Kind to a gRPC status error.
func ToGRPCStatus(err error) error {
	var e *Error
	if !errors.As(err, &e) {
		return status.Error(codes.Internal, "internal error")
	}

	var code codes.Code
	switch e.Kind {
	case KindNotFound:
		code = codes.NotFound
	case KindInvalidArgument:
		code = codes.InvalidArgument
	case KindAlreadyExists:
		code = codes.AlreadyExists
	case KindFailedPrecondition:
		code = codes.FailedPrecondition
	case KindInternal:
		code = codes.Internal
	default:
		code = codes.Internal
	}

	return status.Error(code, e.Message)
}
