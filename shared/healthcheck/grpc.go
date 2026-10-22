package healthcheck

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RegisterGRPC exposes the standard health protocol used by Kubernetes probes.
// Register after startup dependencies are initialized; shut down before draining.
func RegisterGRPC(server grpc.ServiceRegistrar) *health.Server {
	h := health.NewServer()
	healthpb.RegisterHealthServer(server, h)
	return h
}
