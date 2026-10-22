package eventbus

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/amirzre/payment-system/shared/eventbus"

// TraceContext is deliberately used directly: a globally configured composite
// propagator must never cause event headers or durable rows to carry baggage.
func extractEventContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	// An absent or invalid event parent starts a new trace rather than inheriting
	// an unrelated long-running worker span. Keep the worker's values/deadline.
	ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	return propagation.TraceContext{}.Extract(ctx, carrier)
}

func restoreOutboxContext(ctx context.Context, data []byte) context.Context {
	carrier := propagation.MapCarrier{}
	var stored map[string]json.RawMessage
	if json.Unmarshal(data, &stored) == nil {
		for _, key := range []string{"traceparent", "tracestate"} {
			var value string
			if json.Unmarshal(stored[key], &value) == nil {
				carrier[key] = value
			}
		}
	}
	return extractEventContext(ctx, carrier)
}

func boundedSubject(subject string) string {
	switch subject {
	case "ledger.posted", "payment.completed", "account.created":
		return subject
	default:
		return "other"
	}
}

func eventAttributes(subject string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", boundedSubject(subject)),
	}
}
