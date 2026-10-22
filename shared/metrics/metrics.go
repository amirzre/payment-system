// Package metrics owns bounded, process-local Prometheus instrumentation.
// Recording helpers are safe no-ops until observability initializes a registry.
package metrics

import (
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Registry struct {
	Prometheus *prometheus.Registry

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	inflight     *prometheus.GaugeVec

	grpcRequests    *prometheus.CounterVec
	grpcDuration    *prometheus.HistogramVec
	logicalCalls    *prometheus.CounterVec
	logicalDuration *prometheus.HistogramVec

	events         *prometheus.CounterVec
	eventDuration  *prometheus.HistogramVec
	outbox         *prometheus.CounterVec
	outboxDuration *prometheus.HistogramVec
	pending        *prometheus.GaugeVec
	oldest         *prometheus.GaugeVec

	cache              *prometheus.CounterVec
	retries            *prometheus.CounterVec
	breakerTransitions *prometheus.CounterVec
	breaker            *prometheus.GaugeVec
}

var active atomic.Pointer[Registry]

func Activate(r *Registry) { active.Store(r) }

func Deactivate(r *Registry) { active.CompareAndSwap(r, nil) }

func New(service string) *Registry {
	r := &Registry{Prometheus: prometheus.NewRegistry()}
	labels := prometheus.Labels{"service": service}
	counter := func(name, help string, keys ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: labels}, keys)
		r.Prometheus.MustRegister(v)
		return v
	}
	hist := func(name, help string, keys ...string) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:        name,
				Help:        help,
				ConstLabels: labels,
				Buckets:     prometheus.DefBuckets,
			},
			keys,
		)
		r.Prometheus.MustRegister(v)
		return v
	}
	gauge := func(name, help string, keys ...string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: labels}, keys)
		r.Prometheus.MustRegister(v)
		return v
	}
	r.Prometheus.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	r.httpRequests = counter("http_requests_total", "Completed HTTP requests.", "method", "route", "status")
	r.httpDuration = hist(
		"http_request_duration_seconds",
		"HTTP request duration in seconds.",
		"method",
		"route",
		"status",
	)
	r.inflight = gauge("http_requests_in_flight", "HTTP requests currently in flight.")
	r.logicalCalls = counter(
		"grpc_client_logical_calls_total",
		"Logical client calls including breaker rejections, before retries.",
		"method",
		"code",
	)
	r.logicalDuration = hist(
		"grpc_client_logical_call_duration_seconds",
		"Logical call duration including retries/backoff in seconds.",
		"method",
		"code",
	)
	r.grpcRequests = counter(
		"grpc_requests_total",
		"Completed gRPC calls; client records actual transport attempts.",
		"side",
		"method",
		"code",
	)
	r.grpcDuration = hist("grpc_request_duration_seconds", "gRPC call duration in seconds.", "side", "method", "code")
	r.events = counter("event_processing_total", "Consumer processing outcomes.", "subject", "consumer", "outcome")
	r.eventDuration = hist(
		"event_processing_duration_seconds",
		"Consumer processing duration in seconds.",
		"subject",
		"consumer",
		"outcome",
	)
	r.outbox = counter("outbox_publish_total", "Outbox publish attempts.", "subject", "outcome")
	r.outboxDuration = hist("outbox_publish_duration_seconds", "Outbox publish duration in seconds.", "subject", "outcome")
	r.pending = gauge("outbox_pending", "Unpublished outbox rows.", "subject")
	r.oldest = gauge(
		"outbox_oldest_pending_age_seconds",
		"Age of oldest unpublished outbox row in seconds.",
		"subject",
	)
	r.cache = counter("cache_operations_total", "Cache operation outcomes.", "operation", "outcome")
	r.retries = counter(
		"grpc_retry_attempts_total",
		"Additional retry attempts, excluding the initial attempt.",
		"method",
	)
	r.breakerTransitions = counter(
		"grpc_circuit_breaker_transitions_total",
		"Circuit breaker state changes per dependency.",
		"dependency",
		"state",
	)
	r.breaker = gauge(
		"grpc_circuit_breaker_state",
		"Circuit breaker state per dependency: closed=0, half_open=1, open=2.",
		"dependency",
	)
	return r
}

func bounded(v string, allowed ...string) string {
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	return "other"
}

func Method(v string) string {
	return bounded(v,
		"/account.v1.AccountService/CreateAccount",
		"/account.v1.AccountService/GetAccount",
		"/account.v1.AccountService/GetBalance",
		"/account.v1.AccountService/Deposit",
		"/auth.v1.AuthService/Register",
		"/auth.v1.AuthService/Login",
		"/auth.v1.AuthService/VerifyToken",
		"/ledger.v1.LedgerService/PostEntries",
		"/ledger.v1.LedgerService/GetBalance",
		"/ledger.v1.LedgerService/Deposit",
		"/payment.v1.PaymentService/CreatePayment",
		"/payment.v1.PaymentService/GetPayment",
	)
}

func Route(v string) string {
	return bounded(
		v,
		"/v1/auth/register",
		"/v1/auth/login",
		"/v1/accounts",
		"/v1/accounts/{id}",
		"/v1/accounts/{id}/deposit",
		"/v1/payments",
		"/v1/payments/{id}",
	)
}

func subject(v string) string {
	return bounded(v, "ledger.posted", "payment.completed", "account.created")
}

func outcome(v string) string {
	return bounded(v, "success", "error", "duplicate", "skipped", "canceled", "hit", "miss")
}

func RecordHTTP(method, route string, status int, seconds float64) {
	if r := active.Load(); r != nil {
		code := "other"
		if status >= 100 && status <= 599 {
			code = strconv.Itoa(status)
		}
		keys := []string{
			bounded(method, "GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"),
			Route(route),
			code,
		}
		r.httpRequests.WithLabelValues(keys...).Inc()
		r.httpDuration.WithLabelValues(keys...).Observe(seconds)
	}
}

// HTTPInFlight returns a completion function tied to this registry, even across shutdown.
func HTTPInFlight() func() {
	if r := active.Load(); r != nil {
		g := r.inflight.WithLabelValues()
		g.Inc()
		return g.Dec
	}
	return func() {}
}

func RecordGRPC(side, method, code string, seconds float64) {
	if r := active.Load(); r != nil {
		keys := []string{
			bounded(side, "server", "client"),
			Method(method),
			bounded(
				code,
				"OK",
				"Canceled",
				"Unknown",
				"InvalidArgument",
				"DeadlineExceeded",
				"NotFound",
				"AlreadyExists",
				"PermissionDenied",
				"ResourceExhausted",
				"FailedPrecondition",
				"Aborted",
				"OutOfRange",
				"Unimplemented",
				"Internal",
				"Unavailable",
				"DataLoss",
				"Unauthenticated",
			),
		}
		r.grpcRequests.WithLabelValues(keys...).Inc()
		r.grpcDuration.WithLabelValues(keys...).Observe(seconds)
	}
}

func RecordEvent(sub, consumer, result string, seconds float64) {
	if r := active.Load(); r != nil {
		keys := []string{
			subject(sub),
			bounded(consumer, "account-service-cache", "payment-service", "payment-ledger-posted"),
			outcome(result),
		}
		r.events.WithLabelValues(keys...).Inc()
		r.eventDuration.WithLabelValues(keys...).Observe(seconds)
	}
}

func RecordOutbox(sub, result string, seconds float64) {
	if r := active.Load(); r != nil {
		keys := []string{subject(sub), outcome(result)}
		r.outbox.WithLabelValues(keys...).Inc()
		r.outboxDuration.WithLabelValues(keys...).Observe(seconds)
	}
}

func SetOutbox(sub string, pending, oldestSeconds float64) {
	if r := active.Load(); r != nil {
		r.pending.WithLabelValues(subject(sub)).Set(pending)
		r.oldest.WithLabelValues(subject(sub)).Set(oldestSeconds)
	}
}

func RecordCache(operation, result string) {
	if r := active.Load(); r != nil {
		r.cache.WithLabelValues(
			bounded(operation, "get", "set", "delete", "invalidate", "balance"),
			outcome(result),
		).Inc()
	}
}

func RecordRetry(method string) {
	if r := active.Load(); r != nil {
		r.retries.WithLabelValues(Method(method)).Inc()
	}
}

func RecordBreaker(method, state string) {
	if r := active.Load(); r != nil {
		state = bounded(state, "closed", "half_open", "open")
		dependency := breakerDependency(method)
		r.breakerTransitions.WithLabelValues(dependency, state).Inc()
		v := float64(0)
		if state == "half_open" {
			v = 1
		}
		if state == "open" {
			v = 2
		}
		r.breaker.WithLabelValues(dependency).Set(v)
	}
}

func RegisterPoolStats(name string, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	r := active.Load()
	if r == nil {
		return
	}
	name = bounded(name, "account", "auth", "ledger", "payment", "main", "database")
	for _, spec := range []struct {
		name, help string
		value      func() float64
	}{
		{
			"pgxpool_connections",
			"Total pool connections.",
			func() float64 { return float64(pool.Stat().TotalConns()) },
		},
		{
			"pgxpool_acquired_connections",
			"Acquired pool connections.",
			func() float64 { return float64(pool.Stat().AcquiredConns()) },
		},
		{
			"pgxpool_idle_connections",
			"Idle pool connections.",
			func() float64 { return float64(pool.Stat().IdleConns()) },
		},
		{
			"pgxpool_max_connections",
			"Maximum pool connections.",
			func() float64 { return float64(pool.Stat().MaxConns()) },
		},
		{
			"pgxpool_acquire_duration_seconds_total",
			"Cumulative connection acquire duration in seconds.",
			func() float64 { return pool.Stat().AcquireDuration().Seconds() },
		},
		{
			"pgxpool_empty_acquire_total",
			"Acquires that waited for a connection.",
			func() float64 { return float64(pool.Stat().EmptyAcquireCount()) },
		},
		{
			"pgxpool_canceled_acquire_total",
			"Canceled connection acquires.",
			func() float64 { return float64(pool.Stat().CanceledAcquireCount()) },
		},
	} {
		_ = r.Prometheus.Register(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: spec.name, Help: spec.help, ConstLabels: prometheus.Labels{"pool": name}},
			spec.value,
		))
	}
}

// RecordLogical records one client operation outside breaker/retry handling.
func RecordLogical(method, code string, seconds float64) {
	if r := active.Load(); r != nil {
		keys := []string{
			Method(method),
			bounded(
				code,
				"OK",
				"Canceled",
				"Unknown",
				"InvalidArgument",
				"DeadlineExceeded",
				"NotFound",
				"AlreadyExists",
				"PermissionDenied",
				"ResourceExhausted",
				"FailedPrecondition",
				"Aborted",
				"OutOfRange",
				"Unimplemented",
				"Internal",
				"Unavailable",
				"DataLoss",
				"Unauthenticated",
			),
		}
		r.logicalCalls.WithLabelValues(keys...).Inc()
		r.logicalDuration.WithLabelValues(keys...).Observe(seconds)
	}
}

// The breaker belongs to one downstream connection shared by its RPC methods.
// Derive its bounded identity from the method allowlist to avoid stale per-method
// states when a different method supplies the successful recovery probe.
func breakerDependency(method string) string {
	method = Method(method)
	if method == "other" {
		return "other"
	}
	dependency, _, _ := strings.Cut(strings.TrimPrefix(method, "/"), "/")
	return dependency
}
