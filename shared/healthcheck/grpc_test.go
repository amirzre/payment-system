package healthcheck

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCHealthProtocol(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	health := RegisterGRPC(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///health",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := healthpb.NewHealthClient(conn)
	check := func(want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		response, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if response.Status != want {
			t.Fatalf("health status = %v, want %v", response.Status, want)
		}
	}
	check(healthpb.HealthCheckResponse_SERVING)
	health.Shutdown()
	check(healthpb.HealthCheckResponse_NOT_SERVING)
}
