package observability

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/amirzre/payment-system/shared/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestGRPCPropagationPrivacyAndHealthExclusion(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(safeExporter{exp}))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(old)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	reg := metrics.New("payment-service")
	metrics.Activate(reg)
	defer metrics.Deactivate(reg)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(GRPCServerOption())
	defer server.Stop()
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "account.v1.AccountService", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{{MethodName: "GetBalance", Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		if e := dec(&emptypb.Empty{}); e != nil {
			return nil, e
		}
		if md, _ := metadata.FromIncomingContext(ctx); len(md.Get("baggage")) != 0 {
			t.Error("baggage propagated")
		}
		return nil, status.Error(codes.Unavailable, "sentinel-password")
	}}}}, struct{}{})
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	opts := append(
		GRPCClientOptions(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	conn, err := grpc.NewClient("passthrough:///bufconn", opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx, parent := tp.Tracer("test").Start(ctx, "payment.create")
	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"authorization",
		"sentinel-password",
		"baggage",
		"secret=sentinel-password",
	)
	for range 2 {
		if err = conn.Invoke(ctx, "/account.v1.AccountService/GetBalance", &emptypb.Empty{}, &emptypb.Empty{}); status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
	}
	parent.End()
	spans := exp.GetSpans()
	if len(spans) != 5 {
		t.Fatalf("got %d spans want parent and 2 client/server pairs", len(spans))
	}
	for _, s := range spans {
		if s.SpanContext.TraceID() != parent.SpanContext().TraceID() {
			t.Fatal("trace continuity lost")
		}
		if strings.Contains(s.Status.Description+stringifyAttrs(s.Attributes), "sentinel") {
			t.Fatal("sensitive error exported")
		}
	}
	if _, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if len(exp.GetSpans()) != len(spans) {
		t.Fatal("health generated spans")
	}
	families, err := reg.Prometheus.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "grpc_requests_total" {
			for _, m := range f.Metric {
				if m.GetCounter().GetValue() != 2 {
					t.Fatalf("attempt count %v", m)
				}
			}
		}
	}
}
