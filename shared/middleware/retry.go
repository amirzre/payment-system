package middleware

import (
	"context"
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
	// RetryableMethods explicitly lists RPCs safe to repeat. Nil selects the
	// default allowlist; an empty non-nil slice disables retries for all methods.
	// Only include writes whose server enforces a stable idempotency key.
	RetryableMethods []string
}

// DefaultRetryConfig returns sensible defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   5 * time.Second,
		RetryableCodes: []codes.Code{
			codes.Unavailable,
			codes.ResourceExhausted,
		},
		RetryableMethods: []string{
			"/account.v1.AccountService/GetAccount",
			"/account.v1.AccountService/GetBalance",
			"/ledger.v1.LedgerService/GetBalance",
			"/payment.v1.PaymentService/GetPayment",
			"/auth.v1.AuthService/VerifyToken",
			"/ledger.v1.LedgerService/PostEntries",
			"/payment.v1.PaymentService/CreatePayment",
		},
	}
}

// UnaryClientRetry returns a client-side gRPC interceptor that retries failed
// calls with exponential backoff and jitter.
func UnaryClientRetry(cfg RetryConfig) grpc.UnaryClientInterceptor {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
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
	if cfg.RetryableMethods == nil {
		cfg.RetryableMethods = DefaultRetryConfig().RetryableMethods
	}

	retryableSet := make(map[codes.Code]bool, len(cfg.RetryableCodes))
	for _, c := range cfg.RetryableCodes {
		retryableSet[c] = true
	}
	retryableMethods := make(map[string]bool, len(cfg.RetryableMethods))
	for _, method := range cfg.RetryableMethods {
		retryableMethods[method] = true
	}

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		for attempt := 0; ; attempt++ {
			// Check context before each attempt
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}

			err := invoker(ctx, method, req, reply, cc, opts...)
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			if err == nil {
				return nil
			}

			// Don't retry if this is the last attempt
			if attempt == cfg.MaxRetries || !retryableMethods[method] {
				return err
			}

			// Only retry on retryable codes
			st, ok := status.FromError(err)
			if !ok || !retryableSet[st.Code()] {
				return err
			}

			// Exponential backoff with jitter
			timer := time.NewTimer(retryDelay(cfg, attempt))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return status.FromContextError(ctx.Err()).Err()
			}
		}
	}
}

func retryDelay(cfg RetryConfig, attempt int) time.Duration {
	delay := cfg.BaseDelay
	if delay > cfg.MaxDelay {
		delay = cfg.MaxDelay
	}
	for i := 0; i < attempt && delay < cfg.MaxDelay; i++ {
		if delay > cfg.MaxDelay/2 {
			delay = cfg.MaxDelay
		} else {
			delay *= 2
		}
	}
	// Clamp after jitter as well as before it, including duration-overflow cases.
	jittered := float64(delay) * (0.75 + 0.5*rand.Float64())
	if jittered >= float64(cfg.MaxDelay) {
		return cfg.MaxDelay
	}
	return time.Duration(jittered)
}
