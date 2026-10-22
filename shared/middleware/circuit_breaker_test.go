package middleware

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func breakerCall(interceptor grpc.UnaryClientInterceptor, ctx context.Context, action func() error) error {
	return interceptor(ctx, "/account.v1.AccountService/GetBalance", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return action() })
}

func deterministicBreaker(cfg CircuitBreakerConfig) (grpc.UnaryClientInterceptor, func(time.Duration)) {
	cb := newCircuitBreaker(cfg)
	var elapsed atomic.Int64
	cb.now = func() time.Time { return time.Unix(0, 0).Add(time.Duration(elapsed.Load())) }
	return unaryClientCircuitBreaker(cb), func(duration time.Duration) { elapsed.Add(int64(duration)) }
}

func assertBreakerClosed(t *testing.T, cb grpc.UnaryClientInterceptor) {
	t.Helper()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- breakerCall(cb, context.Background(), func() error { close(started); <-release; return nil })
	}()
	<-started
	called := false
	err := breakerCall(cb, context.Background(), func() error { called = true; return nil })
	close(release)
	if firstErr := <-done; firstErr != nil {
		t.Fatal(firstErr)
	}
	if !called || err != nil {
		t.Fatalf("closed circuit rejected concurrent request: %v", err)
	}
}

func TestCircuitBreakerNonServerResponseResetsConsecutiveFailures(t *testing.T) {
	cb := UnaryClientCircuitBreaker(CircuitBreakerConfig{FailureThreshold: 2, SuccessThreshold: 2, OpenTimeout: time.Hour})
	for _, code := range []codes.Code{codes.Unavailable, codes.InvalidArgument, codes.Unavailable} {
		breakerCall(cb, context.Background(), func() error { return status.Error(code, "failure") })
	}
	called := false
	err := breakerCall(cb, context.Background(), func() error { called = true; return nil })
	if !called || err != nil {
		t.Fatalf("breaker opened across a healthy client-error response: %v", err)
	}
}

func TestCircuitBreakerOnlyOneHalfOpenProbe(t *testing.T) {
	cb, advance := deterministicBreaker(CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 2, OpenTimeout: time.Minute})
	breakerCall(cb, context.Background(), func() error { return status.Error(codes.Unavailable, "outage") })
	advance(time.Minute)
	for probe := 0; probe < 2; probe++ {
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			done <- breakerCall(cb, context.Background(), func() error { close(started); <-release; return nil })
		}()
		<-started
		called := false
		err := breakerCall(cb, context.Background(), func() error { called = true; return nil })
		close(release)
		if probeErr := <-done; probeErr != nil {
			t.Fatal(probeErr)
		}
		if called || status.Code(err) != codes.Unavailable {
			t.Fatalf("probe %d admitted a concurrent request: called=%v, err=%v", probe, called, err)
		}
	}
	assertBreakerClosed(t, cb)
}

func TestCircuitBreakerIgnoresStaleSuccess(t *testing.T) {
	cb, advance := deterministicBreaker(CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 2, OpenTimeout: time.Minute})
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- breakerCall(cb, context.Background(), func() error { close(started); <-release; return nil })
	}()
	<-started
	breakerCall(cb, context.Background(), func() error { return status.Error(codes.Unavailable, "outage") })
	advance(time.Minute)
	probeStarted, probeRelease, probeDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		probeDone <- breakerCall(cb, context.Background(), func() error { close(probeStarted); <-probeRelease; return nil })
	}()
	<-probeStarted
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	called := false
	err := breakerCall(cb, context.Background(), func() error { called = true; return nil })
	close(probeRelease)
	if err := <-probeDone; err != nil {
		t.Fatal(err)
	}
	if called || status.Code(err) != codes.Unavailable {
		t.Fatalf("stale success admitted request: %v", err)
	}
	// Only the recovery probe counted: the next probe must still be exclusive.
	started, release, done = make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- breakerCall(cb, context.Background(), func() error { close(started); <-release; return nil })
	}()
	<-started
	called = false
	err = breakerCall(cb, context.Background(), func() error { called = true; return nil })
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if called || status.Code(err) != codes.Unavailable {
		t.Fatalf("stale success counted toward recovery: %v", err)
	}
}

func TestCircuitBreakerIgnoresStaleFailureAfterRecovery(t *testing.T) {
	cb, advance := deterministicBreaker(CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 2, OpenTimeout: time.Minute})
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- breakerCall(cb, context.Background(), func() error { close(started); <-release; return status.Error(codes.Unavailable, "late failure") })
	}()
	<-started
	breakerCall(cb, context.Background(), func() error { return status.Error(codes.Unavailable, "outage") })
	advance(time.Minute)
	for range 2 {
		if err := breakerCall(cb, context.Background(), func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	called := false
	err := breakerCall(cb, context.Background(), func() error { called = true; return nil })
	if !called || err != nil {
		t.Fatalf("stale failure reopened recovered circuit: %v", err)
	}
}

func TestCircuitBreakerCallerCancellationDoesNotTrip(t *testing.T) {
	cb := UnaryClientCircuitBreaker(CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	err := breakerCall(cb, ctx, func() error { cancel(); return status.Error(codes.Unavailable, "transport canceled") })
	if status.Code(err) != codes.Canceled {
		t.Fatalf("got %v, want Canceled", status.Code(err))
	}
	called := false
	err = breakerCall(cb, context.Background(), func() error { called = true; return nil })
	if !called || err != nil {
		t.Fatalf("caller cancellation opened circuit: %v", err)
	}
}

func TestCircuitBreakerEndedContextDoesNotInvoke(t *testing.T) {
	cb := UnaryClientCircuitBreaker(CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := breakerCall(cb, ctx, func() error { t.Fatal("expired request reached dependency"); return nil })
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("got %v, want DeadlineExceeded", status.Code(err))
	}
	assertBreakerClosed(t, cb)
}

func TestCircuitBreakerFailedRecoveryReopens(t *testing.T) {
	cb, advance := deterministicBreaker(CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 2, OpenTimeout: time.Minute})
	fail := func() error { return status.Error(codes.Unavailable, "outage") }
	breakerCall(cb, context.Background(), fail)
	advance(time.Minute)
	if err := breakerCall(cb, context.Background(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	breakerCall(cb, context.Background(), fail)
	called := false
	err := breakerCall(cb, context.Background(), func() error { called = true; return nil })
	if called || status.Code(err) != codes.Unavailable {
		t.Fatalf("failed recovery did not reopen: %v", err)
	}
	advance(time.Minute)
	// Recovery starts again with zero successes.
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- breakerCall(cb, context.Background(), func() error { close(started); <-release; return nil })
	}()
	<-started
	called = false
	err = breakerCall(cb, context.Background(), func() error { called = true; return nil })
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if called || status.Code(err) != codes.Unavailable {
		t.Fatalf("recovery probe was not exclusive: %v", err)
	}
	if err := breakerCall(cb, context.Background(), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	assertBreakerClosed(t, cb)
}

func TestCircuitBreakerCancellationReleasesProbe(t *testing.T) {
	cb, advance := deterministicBreaker(CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 2, OpenTimeout: time.Minute})
	breakerCall(cb, context.Background(), func() error { return status.Error(codes.Unavailable, "outage") })
	advance(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	err := breakerCall(cb, ctx, func() error { cancel(); return context.Canceled })
	if status.Code(err) != codes.Canceled {
		t.Fatalf("got %v, want Canceled", status.Code(err))
	}
	if err := breakerCall(cb, context.Background(), func() error { return nil }); err != nil {
		t.Fatalf("canceled probe retained permit: %v", err)
	}
}

func TestCircuitBreakerCountsOneFailurePerLogicalRetriedRPC(t *testing.T) {
	var attempts atomic.Int32
	conn := resilienceTestConn(t, []grpc.UnaryClientInterceptor{
		UnaryClientCircuitBreaker(CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour}), UnaryClientRetry(fastRetryConfig()),
	}, func(string) error { attempts.Add(1); return status.Error(codes.Unavailable, "outage") })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		err := conn.Invoke(ctx, "/account.v1.AccountService/GetBalance", &emptypb.Empty{}, &emptypb.Empty{})
		if status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
	}
	if got := attempts.Load(); got != 8 {
		t.Fatalf("server attempts = %d, want 8 (two logical failures, then fast fail)", got)
	}
}
