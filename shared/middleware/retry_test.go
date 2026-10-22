package middleware

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func fastRetryConfig() RetryConfig {
	cfg := DefaultRetryConfig()
	cfg.BaseDelay = time.Nanosecond
	cfg.MaxDelay = time.Nanosecond
	return cfg
}

func TestRetryDefaultMethodSafety(t *testing.T) {
	for _, tc := range []struct {
		method string
		want   int
	}{
		{"/account.v1.AccountService/GetAccount", 4},
		{"/account.v1.AccountService/GetBalance", 4},
		{"/ledger.v1.LedgerService/GetBalance", 4},
		{"/payment.v1.PaymentService/GetPayment", 4},
		{"/auth.v1.AuthService/VerifyToken", 4},
		{"/ledger.v1.LedgerService/PostEntries", 4},
		{"/payment.v1.PaymentService/CreatePayment", 4},
		{"/account.v1.AccountService/Deposit", 1},
		{"/account.v1.AccountService/CreateAccount", 1},
		{"/auth.v1.AuthService/Register", 1},
		{"/auth.v1.AuthService/Login", 1},
		{"/unknown.Service/Get", 1},
	} {
		t.Run(tc.method, func(t *testing.T) {
			attempts := 0
			err := UnaryClientRetry(fastRetryConfig())(context.Background(), tc.method, nil, nil, nil,
				func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
					attempts++
					return status.Error(codes.Unavailable, "response lost")
				})
			if status.Code(err) != codes.Unavailable || attempts != tc.want {
				t.Fatalf("got code %v, %d attempts; want Unavailable, %d attempts", status.Code(err), attempts, tc.want)
			}
		})
	}
}

func TestRetryDefaultCodes(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want int
	}{
		{codes.Unavailable, 4}, {codes.ResourceExhausted, 4}, {codes.Internal, 1},
		{codes.Aborted, 1}, {codes.DeadlineExceeded, 1}, {codes.Canceled, 1}, {codes.InvalidArgument, 1},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			attempts := 0
			UnaryClientRetry(fastRetryConfig())(context.Background(), "/account.v1.AccountService/GetBalance", nil, nil, nil,
				func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
					attempts++
					return status.Error(tc.code, "failure")
				})
			if attempts != tc.want {
				t.Fatalf("got %d attempts, want %d", attempts, tc.want)
			}
		})
	}
}

func TestRetryZeroAndNegativeRetriesDisable(t *testing.T) {
	for _, retries := range []int{0, -1} {
		cfg := fastRetryConfig()
		cfg.MaxRetries = retries
		attempts := 0
		UnaryClientRetry(cfg)(context.Background(), "/account.v1.AccountService/GetBalance", nil, nil, nil,
			func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				attempts++
				return status.Error(codes.Unavailable, "failure")
			})
		if attempts != 1 {
			t.Fatalf("MaxRetries=%d made %d attempts, want 1", retries, attempts)
		}
	}
}

func TestRetryContextStatus(t *testing.T) {
	for _, expired := range []bool{false, true} {
		var ctx context.Context
		var cancel context.CancelFunc
		want := codes.Canceled
		if expired {
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			want = codes.DeadlineExceeded
		} else {
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
		}
		defer cancel()
		err := UnaryClientRetry(fastRetryConfig())(ctx, "/account.v1.AccountService/GetBalance", nil, nil, nil,
			func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				t.Fatal("invoker called after context ended")
				return nil
			})
		if status.Code(err) != want {
			t.Fatalf("got %v, want %v", status.Code(err), want)
		}
	}
}

func TestRetryCancellationAfterAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := fastRetryConfig()
	cfg.BaseDelay = time.Hour
	cfg.MaxDelay = time.Hour
	attempts := 0
	err := UnaryClientRetry(cfg)(ctx, "/account.v1.AccountService/GetBalance", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			attempts++
			cancel()
			return status.Error(codes.Unavailable, "failure")
		})
	if status.Code(err) != codes.Canceled || attempts != 1 {
		t.Fatalf("got %v, %d attempts; want Canceled, 1 attempt", status.Code(err), attempts)
	}
}

func TestRetryExplicitMethodPolicy(t *testing.T) {
	for _, tc := range []struct {
		methods []string
		method  string
		want    int
	}{
		{[]string{}, "/account.v1.AccountService/GetBalance", 1},
		{[]string{"/custom.Service/Idempotent"}, "/custom.Service/Idempotent", 4},
		{[]string{"/custom.Service/Idempotent"}, "/account.v1.AccountService/GetBalance", 1},
	} {
		cfg := fastRetryConfig()
		cfg.RetryableMethods = tc.methods
		attempts := 0
		UnaryClientRetry(cfg)(context.Background(), tc.method, nil, nil, nil,
			func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				attempts++
				return status.Error(codes.Unavailable, "failure")
			})
		if attempts != tc.want {
			t.Fatalf("method %s got %d attempts, want %d", tc.method, attempts, tc.want)
		}
	}
}

func TestRetryBackoffAlwaysRespectsCap(t *testing.T) {
	for _, cfg := range []RetryConfig{
		{BaseDelay: time.Second, MaxDelay: time.Second},
		{BaseDelay: time.Hour, MaxDelay: time.Second},
		{BaseDelay: 1 << 61, MaxDelay: time.Duration(1<<63 - 1)},
	} {
		for _, attempt := range []int{0, 1, 63, 10000} {
			for range 100 {
				delay := retryDelay(cfg, attempt)
				if delay < 0 || delay > cfg.MaxDelay {
					t.Fatalf("attempt %d delay %v outside [0,%v]", attempt, delay, cfg.MaxDelay)
				}
			}
		}
	}
}

// This service simulates a response lost after applying a deposit, and a read
// whose first two replies are unavailable. The transport is real in-process gRPC.
func TestRetryGRPCResponseLossDoesNotRepeatDeposit(t *testing.T) {
	var deposits, reads atomic.Int32
	conn := resilienceTestConn(t, []grpc.UnaryClientInterceptor{UnaryClientRetry(fastRetryConfig())}, func(method string) error {
		if method == "Deposit" {
			deposits.Add(1)
			return status.Error(codes.Unavailable, "response lost after deposit")
		}
		if reads.Add(1) < 3 {
			return status.Error(codes.Unavailable, "temporary outage")
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Invoke(ctx, "/account.v1.AccountService/Deposit", &emptypb.Empty{}, &emptypb.Empty{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("deposit code = %v", status.Code(err))
	}
	if got := deposits.Load(); got != 1 {
		t.Fatalf("deposit applied %d times, want 1", got)
	}
	if err := conn.Invoke(ctx, "/account.v1.AccountService/GetBalance", &emptypb.Empty{}, &emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if got := reads.Load(); got != 3 {
		t.Fatalf("read attempts = %d, want 3", got)
	}
}

func TestWaitForReadyColdConnectionDoesNotRetryDepositOrTripBreaker(t *testing.T) {
	var ready atomic.Bool
	var deposits, attempts atomic.Int32
	countAttempt := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		attempts.Add(1)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	conn := resilienceTestConnOptions(t, []grpc.UnaryClientInterceptor{
		UnaryClientCircuitBreaker(CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}),
		UnaryClientRetry(fastRetryConfig()), countAttempt,
	}, func(string) error { deposits.Add(1); return nil }, func(listener *bufconn.Listener) []grpc.DialOption {
		return []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: time.Hour, Multiplier: 1, MaxDelay: time.Hour},
				MinConnectTimeout: time.Second,
			}),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				if !ready.Load() {
					return nil, errors.New("service starting")
				}
				return listener.DialContext(ctx)
			}),
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn.Connect()
	for state := conn.GetState(); state != connectivity.TransientFailure; state = conn.GetState() {
		if !conn.WaitForStateChange(ctx, state) {
			t.Fatal("connection did not reach TransientFailure")
		}
	}
	for range 2 {
		waiting, stop := context.WithTimeout(ctx, 20*time.Millisecond)
		err := conn.Invoke(waiting, "/account.v1.AccountService/Deposit", &emptypb.Empty{}, &emptypb.Empty{})
		stop()
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("waiting deposit error = %v, want DeadlineExceeded", err)
		}
	}
	if got := deposits.Load(); got != 0 {
		t.Fatalf("disconnected deposits applied %d times, want 0", got)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("waiting calls made %d attempts, want 2", got)
	}
	ready.Store(true)
	conn.ResetConnectBackoff()
	if err := conn.Invoke(ctx, "/account.v1.AccountService/Deposit", &emptypb.Empty{}, &emptypb.Empty{}); err != nil {
		t.Fatalf("recovered deposit failed (breaker must stay closed): %v", err)
	}
	if got := deposits.Load(); got != 1 {
		t.Fatalf("recovered deposit applied %d times, want 1", got)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("logical calls made %d attempts, want 3", got)
	}
}

func resilienceTestConn(t *testing.T, interceptors []grpc.UnaryClientInterceptor, invoke func(string) error) *grpc.ClientConn {
	t.Helper()
	return resilienceTestConnOptions(t, interceptors, invoke, nil)
}

func resilienceTestConnOptions(t *testing.T, interceptors []grpc.UnaryClientInterceptor, invoke func(string) error, extraOptions func(*bufconn.Listener) []grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	methods := make([]grpc.MethodDesc, 0, 2)
	for _, method := range []string{"Deposit", "GetBalance"} {
		methods = append(methods, grpc.MethodDesc{MethodName: method, Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := &emptypb.Empty{}
			if err := dec(req); err != nil {
				return nil, err
			}
			handler := func(context.Context, any) (any, error) { return &emptypb.Empty{}, invoke(method) }
			if interceptor == nil {
				return handler(ctx, req)
			}
			return interceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: "/account.v1.AccountService/" + method}, handler)
		}})
	}
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "account.v1.AccountService", HandlerType: (*interface{})(nil), Methods: methods}, struct{}{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithChainUnaryInterceptor(interceptors...)}
	if extraOptions != nil {
		options = append(options, extraOptions(listener)...)
	}
	conn, err := grpc.NewClient("passthrough:///bufconn", options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
