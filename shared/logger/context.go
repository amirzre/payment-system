package logger

import (
	"context"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// ContextHandler adds valid trace correlation and redacts structured sensitive
// values. Free-text messages must still follow the application's logging policy.
func ContextHandler(next slog.Handler) slog.Handler { return &contextHandler{next: next} }

type contextHandler struct {
	next           slog.Handler
	attrs          []slog.Attr
	groups         []string
	sensitiveGroup bool
}

func (h *contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	clean := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	attrs := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool { attrs = append(attrs, h.redact(a)); return true })
	clean.AddAttrs(groupAttrs(h.attrs, h.groups, attrs)...)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		clean.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, clean)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	safe := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		safe[i] = h.redact(a)
	}
	next := *h
	next.attrs = groupAttrs(h.attrs, h.groups, safe)
	return &next
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := *h
	next.groups = append(append([]string(nil), h.groups...), name)
	next.sensitiveGroup = h.sensitiveGroup || sensitive(name)
	return &next
}

// groupAttrs copies the attribute tree before extending it so derived loggers
// are immutable and safe to share among concurrent requests.
func groupAttrs(base []slog.Attr, groups []string, attrs []slog.Attr) []slog.Attr {
	out := append([]slog.Attr(nil), base...)
	if len(groups) == 0 {
		return append(out, attrs...)
	}
	name := groups[0]
	for i, a := range out {
		if a.Key == name && a.Value.Kind() == slog.KindGroup {
			out[i] = slog.Attr{Key: name, Value: slog.GroupValue(groupAttrs(a.Value.Group(), groups[1:], attrs)...)}
			return out
		}
	}
	return append(out, slog.Attr{Key: name, Value: slog.GroupValue(groupAttrs(nil, groups[1:], attrs)...)})
}

func sensitive(key string) bool {
	key = strings.ToLower(key)
	for _, part := range []string{
		"password",
		"passwd",
		"secret",
		"token",
		"authorization",
		"cookie",
		"private",
		"dsn",
		"url",
		"uri",
		"sql",
		"query",
		"parameter",
		"payload",
		"body",
		"request",
		"response",
		"idempotency",
		"header",
		"credential",
	} {
		if strings.Contains(key, part) && key != "request_id" {
			return true
		}
	}
	return false
}

func redact(a slog.Attr) slog.Attr { return redactDepth(a, 0) }

func redactDepth(a slog.Attr, depth int) slog.Attr {
	if depth >= 8 {
		return slog.String(a.Key, "[REDACTED]")
	}
	if sensitive(a.Key) {
		return slog.String(a.Key, "[REDACTED]")
	}
	a.Value = a.Value.Resolve()
	switch a.Value.Kind() {
	case slog.KindGroup:
		attrs := a.Value.Group()
		out := make([]slog.Attr, len(attrs))
		for i, v := range attrs {
			out[i] = redactDepth(v, depth+1)
		}
		a.Value = slog.GroupValue(out...)
	case slog.KindAny:
		// Arbitrary objects (including errors and request objects) may serialize
		// secrets through String/MarshalJSON methods. Accept only recursively checked maps.
		if m, ok := a.Value.Any().(map[string]any); ok {
			attrs := make([]slog.Attr, 0, len(m))
			for k, v := range m {
				attrs = append(attrs, redactDepth(slog.Any(k, v), depth+1))
			}
			a.Value = slog.GroupValue(attrs...)
		} else {
			a.Value = slog.StringValue("[REDACTED]")
		}
	case slog.KindString:
		v := a.Value.String()
		if strings.Contains(v, "://") {
			a.Value = slog.StringValue("[REDACTED]")
		}
	}
	return a
}

func (h *contextHandler) redact(a slog.Attr) slog.Attr {
	if h.sensitiveGroup {
		return slog.String(a.Key, "[REDACTED]")
	}
	return redact(a)
}
