package middleware

import (
	"context"
	"math"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryConfig configures the retry interceptor.
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts.
	MaxRetries int
	// BaseDelay is the initial delay before the first retry.
	BaseDelay time.Duration
	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration
	// RetryableCodes are the gRPC status codes that trigger a retry.
	RetryableCodes []codes.Code
}

// DefaultRetryConfig returns sensible defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   5 * time.Second,
		RetryableCodes: []codes.Code{
			codes.Unavailable,
			codes.DeadlineExceeded,
			codes.ResourceExhausted,
			codes.Internal,
			codes.Aborted,
		},
	}
}

// UnaryClientRetry returns a client-side gRPC interceptor that retries failed
// calls with exponential backoff and jitter.
func UnaryClientRetry(cfg RetryConfig) grpc.UnaryClientInterceptor {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 100 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 5 * time.Second
	}
	if len(cfg.RetryableCodes) == 0 {
		cfg.RetryableCodes = DefaultRetryConfig().RetryableCodes
	}

	retryableSet := make(map[codes.Code]bool, len(cfg.RetryableCodes))
	for _, c := range cfg.RetryableCodes {
		retryableSet[c] = true
	}

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var lastErr error

		for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
			// Check context before each attempt
			if ctx.Err() != nil {
				return ctx.Err()
			}

			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}

			lastErr = err

			// Don't retry if this is the last attempt
			if attempt == cfg.MaxRetries {
				break
			}

			// Only retry on retryable codes
			st, ok := status.FromError(err)
			if !ok || !retryableSet[st.Code()] {
				break
			}

			// Exponential backoff with jitter
			delay := time.Duration(float64(cfg.BaseDelay) * math.Pow(2, float64(attempt)))
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
			// Add jitter: ±25%
			jitter := time.Duration(float64(delay) * 0.25 * (2*rand.Float64() - 1))
			delay += jitter

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return lastErr
	}
}
