package observability

import (
	"context"
	"strings"

	"github.com/amirzre/payment-system/shared/metrics"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// safeExporter is the last privacy boundary, including automatically produced
// instrumentation. Unknown attributes and arbitrary error/event text are dropped.
type safeExporter struct{ sdktrace.SpanExporter }

func (e safeExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	out := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, s := range spans {
		out[i] = safeSpan{s}
	}
	return e.SpanExporter.ExportSpans(ctx, out)
}

type safeSpan struct{ sdktrace.ReadOnlySpan }

func (s safeSpan) Attributes() []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, 8)
	for _, a := range s.ReadOnlySpan.Attributes() {
		switch string(a.Key) {
		case "http.route":
			a = attribute.String("http.route", metrics.Route(a.Value.AsString()))
		case "http.request.method", "http.method":
			v := a.Value.AsString()
			switch v {
			case "GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD":
			default:
				v = "other"
			}
			a = attribute.String(string(a.Key), v)
		case "http.response.status_code", "http.status_code", "rpc.grpc.status_code":
			v := a.Value.AsInt64()
			if v < 0 || v > 599 {
				continue
			}
		case "rpc.system":
			if a.Value.AsString() != "grpc" {
				continue
			}
		case "rpc.service", "rpc.method": // retain only known application RPC names
			full := ""
			for _, v := range s.ReadOnlySpan.Attributes() {
				if (a.Key == "rpc.service" && v.Key == "rpc.method") || (a.Key == "rpc.method" && v.Key == "rpc.service") {
					if a.Key == "rpc.service" {
						full = "/" + a.Value.AsString() + "/" + v.Value.AsString()
					} else {
						full = "/" + v.Value.AsString() + "/" + a.Value.AsString()
					}
				}
			}
			if metrics.Method(full) == "other" {
				continue
			}
		case "messaging.system":
			if a.Value.AsString() != "nats" {
				continue
			}
		case "messaging.destination.name":
			v := a.Value.AsString()
			if v != "ledger.posted" && v != "payment.completed" && v != "account.created" {
				a = attribute.String(string(a.Key), "other")
			}
		case "messaging.operation.name", "messaging.operation.type":
			v := a.Value.AsString()
			switch v {
			case "publish", "process", "send", "receive":
			default:
				continue
			}
		default:
			continue
		}
		out = append(out, a)
	}
	return out
}

func (s safeSpan) Events() []sdktrace.Event { return nil }

func (s safeSpan) Links() []sdktrace.Link {
	links := s.ReadOnlySpan.Links()
	out := make([]sdktrace.Link, len(links))
	for i, l := range links {
		l.Attributes = nil
		out[i] = l
	}
	return out
}

func (s safeSpan) Status() sdktrace.Status {
	status := s.ReadOnlySpan.Status()
	status.Description = ""
	return status
}

func (s safeSpan) Name() string {
	n := s.ReadOnlySpan.Name()
	if metrics.Method("/"+strings.TrimPrefix(n, "/")) != "other" {
		return strings.TrimPrefix(n, "/")
	}
	for _, method := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD", "other"} {
		if strings.HasPrefix(n, method+" ") {
			return method + " " + metrics.Route(strings.TrimPrefix(n, method+" "))
		}
	}
	switch n {
	case "nats publish", "nats process", "outbox publish", "account.balance", "account.create", "account.deposit", "ledger.post", "ledger.deposit", "payment.create", "auth.register", "auth.login", "outbox.publish", "messaging.publish", "messaging.process", "eventbus.publish", "eventbus.process", "eventbus.consume", "ledger.posted publish", "ledger.posted process", "payment.completed publish", "payment.completed process":
		return n
	}
	return "operation"
}
