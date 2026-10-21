package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestContextCorrelationAndNestedRedaction(t *testing.T) {
	var b bytes.Buffer
	l := slog.New(ContextHandler(slog.NewJSONHandler(&b, nil)))
	id, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	sid, _ := trace.SpanIDFromHex("0123456789abcdef")
	ctx := trace.ContextWithSpanContext(
		context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: sid}),
	)
	l.With(slog.Any(
		"details",
		map[string]any{"password": "sentinel", "safe": map[string]any{"authorization": "sentinel"}},
	)).WithGroup("nested").InfoContext(
		ctx,
		"request",
		slog.Group("group", slog.String("dsn", "sentinel")),
		slog.Any("request", struct{ Password string }{"sentinel"}),
		slog.String("url", "postgres://sentinel"),
	)
	if strings.Contains(b.String(), "sentinel") {
		t.Fatalf("secret leaked: %s", b.String())
	}
	if !strings.Contains(b.String(), id.String()) || !strings.Contains(b.String(), sid.String()) {
		t.Fatalf("trace correlation missing: %s", b.String())
	}
}

func TestSensitiveGroupWithAttrsCannotLeak(t *testing.T) {
	var b bytes.Buffer
	l := slog.New(ContextHandler(slog.NewJSONHandler(&b, nil)))
	l.WithGroup("headers").With(slog.String("custom", "sentinel")).Info(
		"event",
		slog.String("another", "sentinel"),
	)
	if strings.Contains(b.String(), "sentinel") {
		t.Fatalf("sensitive group leaked: %s", b.String())
	}
}

func TestCorrelationRemainsAtRootWithGroups(t *testing.T) {
	var b bytes.Buffer
	l := slog.New(ContextHandler(slog.NewJSONHandler(&b, nil)))
	id, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	sid, _ := trace.SpanIDFromHex("0123456789abcdef")
	ctx := trace.ContextWithSpanContext(
		context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: sid}),
	)
	l.With("service", "payment-service").WithGroup("operation").With("name", "create").InfoContext(
		ctx,
		"event",
		"outcome",
		"success",
	)
	var record map[string]any
	if err := json.Unmarshal(b.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["trace_id"] != id.String() {
		t.Fatalf("trace correlation nested instead of root: %s", b.String())
	}
	if record["operation"].(map[string]any)["name"] != "create" {
		t.Fatal("WithAttrs lost")
	}
}
