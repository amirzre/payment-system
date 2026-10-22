package middleware

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// MetadataKeyRequestID is the gRPC metadata key for the request ID.
	MetadataKeyRequestID = "x-request-id"

	// MetadataKeyUserID is the gRPC metadata key for the authenticated user ID.
	MetadataKeyUserID = "x-user-id"
)

// RecoveryInterceptor catches panics in gRPC handlers, logs them, and returns
// codes.Internal instead of crashing the process.
func RecoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorContext(ctx, "panic recovered in gRPC handler",
					slog.String("method", metrics.Method(info.FullMethod)),
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

// RequestIDInterceptor ensures every gRPC request has an x-request-id in its metadata.
// If the incoming request already has one, it's preserved. Otherwise, a new UUID is generated.
func RequestIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}

		requestID := ""
		if vals := md.Get(MetadataKeyRequestID); len(vals) > 0 {
			requestID = vals[0]
		}
		if !validRequestID(requestID) {
			requestID = uuid.NewString()
			md.Set(MetadataKeyRequestID, requestID)
			ctx = metadata.NewIncomingContext(ctx, md)
		}

		ctx = metadata.AppendToOutgoingContext(ctx, MetadataKeyRequestID, requestID)

		return handler(ctx, req)
	}
}

// LoggingInterceptor logs every gRPC request with method, duration, and status.
func LoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if excludedMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		code := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				code = st.Code()
			} else {
				code = codes.Internal
			}
		}

		level := slog.LevelInfo
		if code == codes.Internal {
			level = slog.LevelError
		}

		requestID := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(MetadataKeyRequestID); len(vals) > 0 {
				requestID = vals[0]
			}
		}

		logger.Log(ctx, level, "gRPC request",
			slog.String("method", metrics.Method(info.FullMethod)),
			slog.String("code", code.String()),
			slog.Float64("duration_seconds", duration.Seconds()),
			slog.String("request_id", requestID),
		)

		return resp, err
	}
}

// UnaryServerChain returns a grpc.ServerOption that composes recovery, request ID,
// and logging interceptors in the correct order.
func UnaryServerChain(logger *slog.Logger) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		RecoveryInterceptor(logger),
		RequestIDInterceptor(),
		LoggingInterceptor(logger),
	)
}

func validRequestID(v string) bool {
	if len(v) == 0 || len(v) > 64 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func excludedMethod(v string) bool {
	return strings.HasPrefix(v, "/grpc.health.v1.Health/") || strings.HasPrefix(v, "/grpc.reflection.")
}
