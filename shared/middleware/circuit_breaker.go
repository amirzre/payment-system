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
	generation      uint64
	probeInFlight   bool
	now             func() time.Time
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
	return &circuitBreaker{state: CircuitClosed, cfg: cfg, now: time.Now}
}

// admit returns a generation ticket, so calls admitted before a state transition
// cannot change the failure or recovery counters of a newer circuit generation.
func (cb *circuitBreaker) admit() (generation uint64, probe, allowed bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return cb.generation, false, true
	case CircuitOpen:
		if cb.now().Sub(cb.lastFailureTime) < cb.cfg.OpenTimeout {
			return 0, false, false
		}
		cb.state = CircuitHalfOpen
		cb.generation++
		cb.successCount = 0
	}
	if cb.state == CircuitHalfOpen && !cb.probeInFlight {
		cb.probeInFlight = true
		return cb.generation, true, true
	}
	return 0, false, false
}

// record treats caller cancellation as neutral: it releases a recovery permit
// without adding a dependency failure or claiming a successful recovery.
func (cb *circuitBreaker) record(generation uint64, probe, failed, canceled bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if generation != cb.generation {
		return
	}
	if probe {
		cb.probeInFlight = false
	}
	if canceled {
		return
	}
	if failed {
		cb.failureCount++
		if cb.state == CircuitHalfOpen || cb.failureCount >= cb.cfg.FailureThreshold {
			cb.state = CircuitOpen
			cb.generation++
			cb.successCount = 0
			cb.lastFailureTime = cb.now()
		}
		return
	}
	cb.failureCount = 0
	if cb.state == CircuitHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.cfg.SuccessThreshold {
			cb.state = CircuitClosed
			cb.generation++
			cb.successCount = 0
		}
	}
}

// UnaryClientCircuitBreaker returns a client-side gRPC interceptor that implements
// the circuit breaker pattern. It tracks failures per service (per interceptor instance)
// and fast-fails when the circuit is open, returning codes.Unavailable.
// Chain this interceptor before UnaryClientRetry to count each logical call once.
func UnaryClientCircuitBreaker(cfg CircuitBreakerConfig) grpc.UnaryClientInterceptor {
	return unaryClientCircuitBreaker(newCircuitBreaker(cfg))
}

func unaryClientCircuitBreaker(cb *circuitBreaker) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		generation, probe, allowed := cb.admit()
		if !allowed {
			return status.Error(codes.Unavailable, "circuit breaker open")
		}

		err := invoker(ctx, method, req, reply, cc, opts...)
		canceled := ctx.Err() != nil
		cb.record(generation, probe, err != nil && isServerError(err), canceled)
		if canceled {
			return status.FromContextError(ctx.Err()).Err()
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
