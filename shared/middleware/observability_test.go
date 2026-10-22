package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLogicalCallsCountRetriesAndBreakerRejection(t *testing.T) {
	r := metrics.New("payment-service")
	metrics.Activate(r)
	defer metrics.Deactivate(r)
	cfg := DefaultCircuitBreakerConfig()
	cfg.FailureThreshold = 1
	cb := UnaryClientCircuitBreaker(cfg)
	retry := UnaryClientRetry(fastRetryConfig())
	method := "/account.v1.AccountService/GetBalance"
	invoke := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "unavailable")
	}
	combined := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return retry(ctx, method, req, reply, cc, invoke, opts...)
	}
	for range 2 {
		_ = cb(context.Background(), method, nil, nil, nil, combined)
	}
	f, err := r.Prometheus.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, family := range f {
		switch family.GetName() {
		case "grpc_client_logical_calls_total":
			found = true
			if got := family.Metric[0].Counter.GetValue(); got != 2 {
				t.Fatalf("logical count %v", got)
			}
		case "grpc_retry_attempts_total":
			if got := family.Metric[0].Counter.GetValue(); got != 3 {
				t.Fatalf("retries %v", got)
			}
		case "grpc_circuit_breaker_transitions_total":
			if got := family.Metric[0].Counter.GetValue(); got != 1 {
				t.Fatalf("transitions %v", got)
			}
		}
	}
	if !found {
		t.Fatal("logical call counter missing")
	}
}

func TestSharedBreakerGaugeRecoversAcrossMethods(t *testing.T) {
	r := metrics.New("payment-service")
	metrics.Activate(r)
	defer metrics.Deactivate(r)
	cfg := DefaultCircuitBreakerConfig()
	cfg.FailureThreshold = 1
	cfg.SuccessThreshold = 1
	now := time.Now()
	breaker := newCircuitBreaker(cfg)
	breaker.now = func() time.Time { return now }
	intercept := unaryClientCircuitBreaker(breaker)
	fail := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "unavailable")
	}
	success := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil }
	if err := intercept(context.Background(), "/account.v1.AccountService/GetBalance", nil, nil, nil, fail); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected breaker-opening failure: %v", err)
	}
	now = now.Add(cfg.OpenTimeout + time.Second)
	if err := intercept(context.Background(), "/account.v1.AccountService/GetAccount", nil, nil, nil, success); err != nil {
		t.Fatalf("recovery probe: %v", err)
	}
	families, err := r.Prometheus.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, family := range families {
		if family.GetName() != "grpc_circuit_breaker_state" {
			continue
		}
		found = true
		if len(family.Metric) != 1 {
			t.Fatalf("one shared breaker must expose one current state, got %d", len(family.Metric))
		}
		metric := family.Metric[0]
		if metric.Gauge.GetValue() != 0 {
			t.Fatalf("recovered breaker still reported open: %v", metric.Gauge.GetValue())
		}
		dependency := ""
		for _, label := range metric.Label {
			if label.GetName() == "dependency" {
				dependency = label.GetValue()
			}
			if label.GetName() == "method" {
				t.Fatal("shared breaker metric must not use per-method state")
			}
		}
		if dependency != "account.v1.AccountService" {
			t.Fatalf("dependency label=%q", dependency)
		}
	}
	if !found {
		t.Fatal("breaker state missing")
	}
}
