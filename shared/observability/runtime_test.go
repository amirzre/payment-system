package observability

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sharedlog "github.com/amirzre/payment-system/shared/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func cleanEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"METRICS_ADDR",
		"METRICS_TLS_CERT_FILE",
		"METRICS_TLS_KEY_FILE",
		"METRICS_CLIENT_CA_FILE",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_EXPORTER_OTLP_TRACES_INSECURE",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
	} {
		t.Setenv(k, "")
	}
}

func TestDisabledRuntimeAndBoundedShutdown(t *testing.T) {
	cleanEnv(t)
	r, err := Init(context.Background(), "payment-service")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRejectInvalidEnabledConfig(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"METRICS_ADDR", "127.0.0.1:0"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT", "https://user:password@localhost:4317"},
		{"OTEL_EXPORTER_OTLP_ENDPOINT", "https://localhost:4317/path?secret=x"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			cleanEnv(t)
			t.Setenv(tc.key, tc.value)
			r, err := Init(context.Background(), "payment-service")
			if r != nil {
				_ = r.Shutdown(context.Background())
			}
			if err == nil {
				t.Fatal("insecure config accepted")
			}
			if strings.Contains(err.Error(), "password") {
				t.Fatal("config error leaked credential")
			}
		})
	}
}

func TestHTTPNestedRoutePropagationPrivacyAndPanic(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(safeExporter{exp}))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(old)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	var logs bytes.Buffer
	l := slog.New(sharedlog.ContextHandler(slog.NewJSONHandler(&logs, nil)))
	sub := http.NewServeMux()
	sub.HandleFunc("GET /v1/accounts/{id}", func(w http.ResponseWriter, r *http.Request) {
		l.InfoContext(r.Context(), "request")
		_, span := otel.Tracer("test").Start(r.Context(), "account.balance")
		span.SetAttributes(
			attribute.String("url.full", "sentinel"),
			attribute.String("db.statement", "sentinel"),
			attribute.String("password", "sentinel"),
		)
		span.AddEvent("sentinel")
		span.End()
		w.WriteHeader(201)
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/accounts/", sub)
	mux.HandleFunc("GET /panic", func(http.ResponseWriter, *http.Request) { panic("sentinel") })
	h := HTTPHandler(l, mux)
	req := httptest.NewRequest("GET", "http://example/v1/accounts/sentinel?password=sentinel", nil)
	req.Header.Set("Authorization", "sentinel")
	req.Header.Set("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	req.Header.Set("baggage", "secret=sentinel")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 201 || w.Header().Get("X-Trace-ID") != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("headers/status: %v %d", w.Header(), w.Code)
	}
	if !strings.Contains(logs.String(), "0123456789abcdef0123456789abcdef") || strings.Contains(logs.String(), "sentinel") {
		t.Fatalf("logs: %s", logs.String())
	}
	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("spans %d", len(spans))
	}
	for _, s := range spans {
		all := s.Name + stringifyAttrs(s.Attributes)
		for _, e := range s.Events {
			all += e.Name
		}
		if strings.Contains(all, "sentinel") {
			t.Fatalf("export leaked: %s", all)
		}
	}
	if spans[1].Name != "GET /v1/accounts/{id}" {
		t.Fatalf("bad nested route %s", spans[1].Name)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/panic", nil))
	if w.Code != 500 || strings.Contains(logs.String(), "sentinel") {
		t.Fatalf("panic handling %d %s", w.Code, logs.String())
	}
	before := len(exp.GetSpans())
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/healthz", nil))
	if len(exp.GetSpans()) != before {
		t.Fatal("health traced")
	}
}

func stringifyAttrs(a []attribute.KeyValue) string {
	var b strings.Builder
	for _, v := range a {
		b.WriteString(string(v.Key))
		b.WriteString(v.Value.String())
	}
	return b.String()
}

func TestInvalidSamplerAndInsecureOverrides(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"OTEL_TRACES_SAMPLER", "unsupported"},
		{"OTEL_TRACES_SAMPLER_ARG", "NaN"},
		{"OTEL_EXPORTER_OTLP_INSECURE", "true"},
		{"OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true"},
		{"OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			cleanEnv(t)
			if tc.key == "OTEL_TRACES_SAMPLER_ARG" {
				t.Setenv("OTEL_TRACES_SAMPLER", "traceidratio")
			} else if strings.Contains(tc.key, "EXPORTER") {
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://localhost:4317")
			}
			t.Setenv(tc.key, tc.value)
			r, err := Init(context.Background(), "payment-service")
			if r != nil {
				_ = r.Shutdown(context.Background())
			}
			if err == nil {
				t.Fatalf("accepted invalid %s", tc.key)
			}
		})
	}
}
