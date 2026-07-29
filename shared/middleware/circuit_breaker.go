package middleware

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// CircuitBreakerConfig configures the circuit breaker interceptor.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before opening the circuit.
	FailureThreshold int
	// SuccessThreshold is the number of consecutive successes in half-open state to close.
	SuccessThreshold int
	// OpenTimeout is how long the circuit stays open before transitioning to half-open.
	OpenTimeout time.Duration
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
	}
}

// circuitBreaker tracks state for a single downstream service.
type circuitBreaker struct {
	mu              sync.Mutex
	state           CircuitState
	failureCount    int
	successCount    int
	lastFailureTime time.Time
	cfg             CircuitBreakerConfig
}

func newCircuitBreaker(cfg CircuitBreakerConfig) *circuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 2
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	return &circuitBreaker{state: CircuitClosed, cfg: cfg}
}

func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) > cb.cfg.OpenTimeout {
			cb.state = CircuitHalfOpen
			cb.successCount = 0
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	}
	return false
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount = 0
	if cb.state == CircuitHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.cfg.SuccessThreshold {
			cb.state = CircuitClosed
		}
	}
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailureTime = time.Now()

	if cb.state == CircuitHalfOpen || cb.failureCount >= cb.cfg.FailureThreshold {
		cb.state = CircuitOpen
	}
}

// UnaryClientCircuitBreaker returns a client-side gRPC interceptor that implements
// the circuit breaker pattern. It tracks failures per service (per interceptor instance)
// and fast-fails when the circuit is open, returning codes.Unavailable.
func UnaryClientCircuitBreaker(cfg CircuitBreakerConfig) grpc.UnaryClientInterceptor {
	cb := newCircuitBreaker(cfg)

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if !cb.allow() {
			return status.Error(codes.Unavailable, "circuit breaker open")
		}

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			if isServerError(err) {
				cb.recordFailure()
			}
		} else {
			cb.recordSuccess()
		}
		return err
	}
}

// isServerError returns true if the gRPC error is a server-side failure
// (not a client error like InvalidArgument or NotFound).
func isServerError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.Internal, codes.Unavailable, codes.DeadlineExceeded,
		codes.ResourceExhausted, codes.DataLoss, codes.Unknown:
		return true
	}
	return false
}
