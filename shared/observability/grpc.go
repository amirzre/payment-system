package observability

import (
	"context"
	"strings"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// GRPCServerOption adds transport spans/metrics independently of the service's
// recovery/authentication interceptors.
func GRPCServerOption() grpc.ServerOption { return grpc.StatsHandler(&rpcStats{side: "server"}) }

// GRPCClientOptions observes each actual transport attempt, including attempts
// made by the existing application retry interceptor, without changing its order.
func GRPCClientOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithStatsHandler(&rpcStats{side: "client"})}
}

type rpcStats struct{ side string }
type rpcState struct {
	start  time.Time
	method string
	span   trace.Span
}
type rpcKey struct{}

func (h *rpcStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (h *rpcStats) HandleConn(context.Context, stats.ConnStats) {}

func (h *rpcStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if excludedRPC(info.FullMethodName) {
		return ctx
	}
	method := metrics.Method(info.FullMethodName)
	name := strings.TrimPrefix(method, "/")
	kind := trace.SpanKindClient
	if h.side == "server" {
		kind = trace.SpanKindServer
		md, _ := metadata.FromIncomingContext(ctx)
		ctx = propagation.TraceContext{}.Extract(ctx, metadataCarrier(md))
	}
	attrs := []attribute.KeyValue{attribute.String("rpc.system", "grpc")}
	if method != "other" {
		parts := strings.Split(strings.TrimPrefix(method, "/"), "/")
		attrs = append(attrs, attribute.String("rpc.service", parts[0]), attribute.String("rpc.method", parts[1]))
	}
	ctx, span := otel.Tracer("github.com/amirzre/payment-system/shared/observability").Start(
		ctx,
		name,
		trace.WithSpanKind(kind),
		trace.WithAttributes(attrs...),
	)
	if h.side == "client" {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		delete(md, "baggage")
		propagation.TraceContext{}.Inject(ctx, metadataCarrier(md))
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return context.WithValue(ctx, rpcKey{}, rpcState{start: time.Now(), method: method, span: span})
}

func (h *rpcStats) HandleRPC(ctx context.Context, s stats.RPCStats) {
	end, ok := s.(*stats.End)
	if !ok {
		return
	}
	state, ok := ctx.Value(rpcKey{}).(rpcState)
	if !ok {
		return
	}
	code := status.Code(end.Error)
	state.span.SetAttributes(attribute.Int("rpc.grpc.status_code", int(code)))
	if end.Error != nil {
		state.span.SetStatus(otelcodes.Error, "")
	}
	metrics.RecordGRPC(h.side, state.method, code.String(), time.Since(state.start).Seconds())
	state.span.End()
}

func excludedRPC(method string) bool {
	return strings.HasPrefix(method, "/grpc.health.v1.Health/") || strings.HasPrefix(method, "/grpc.reflection.")
}

type metadataCarrier metadata.MD

func (c metadataCarrier) Get(key string) string {
	v := metadata.MD(c).Get(key)
	if len(v) > 0 {
		return v[0]
	}
	return ""
}

func (c metadataCarrier) Set(key, value string) { metadata.MD(c).Set(key, value) }

func (c metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
