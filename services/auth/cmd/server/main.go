package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	authv1 "github.com/amirzre/payment-system/gen/go/auth/v1"
	"github.com/amirzre/payment-system/services/auth/internal/app"
	"github.com/amirzre/payment-system/services/auth/internal/config"
	"github.com/amirzre/payment-system/services/auth/internal/domain"
	"github.com/amirzre/payment-system/services/auth/internal/repository"
	authgrpc "github.com/amirzre/payment-system/services/auth/internal/transport/grpc"
	"github.com/amirzre/payment-system/shared/healthcheck"
	"github.com/amirzre/payment-system/shared/logger"
	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/amirzre/payment-system/shared/middleware"
	"github.com/amirzre/payment-system/shared/observability"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Initialize configuration
	cfg := config.Load()

	// Initialize logger
	log := logger.New(cfg.Server.Environment, "auth-service")
	log.Info("starting auth-service", slog.Int("port", cfg.Server.GRPCPort))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	telemetry, err := observability.Init(ctx, "auth-service")
	if err != nil {
		log.Error("invalid observability configuration", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		flushCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := telemetry.Shutdown(flushCtx); err != nil {
			log.Error("telemetry shutdown failed", slog.Any("error", err))
		}
	}()

	// Connect to Database
	pool, err := pgxpool.New(ctx, cfg.Database.DatabaseURL)
	if err != nil {
		log.Error("failed to create pgx pool", slog.Any("error", err))
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("failed to ping database", slog.Any("error", err))
		os.Exit(1)
	}
	log.Info("connected to database")
	metrics.RegisterPoolStats("database", pool)

	// Initialize token service with RSA keys
	tokenService, err := domain.NewTokenService(cfg.Server.PrivateKeyPEM, cfg.Server.PublicKeyPEM)
	if err != nil {
		log.Error("failed to initialize token service", slog.Any("error", err))
		os.Exit(1)
	}

	// Construct dependency graph
	authRepo := repository.NewUserRepository(pool)
	authSrv := app.NewAuthService(authRepo, tokenService)

	// Set up gRPC server with middleware chain
	grpcServer := grpc.NewServer(observability.GRPCServerOption(), middleware.UnaryServerChain(log))
	authServer := authgrpc.NewAuthServer(authSrv)
	authv1.RegisterAuthServiceServer(grpcServer, authServer)

	healthServer := healthcheck.RegisterGRPC(grpcServer)
	reflection.Register(grpcServer)

	// Start listening
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.GRPCPort))
	if err != nil {
		log.Error("failed to listen", slog.Any("error", err))
		os.Exit(1)
	}

	// Drain requests before flushing telemetry. Never let a long-lived RPC
	// consume the entire Kubernetes termination grace period.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Info("received signal, shutting down", slog.String("signal", sig.String()))
		cancel()
		healthServer.Shutdown()
		drained := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(drained) }()
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case <-drained:
		case <-timer.C:
			grpcServer.Stop()
			<-drained
		}
	}()

	log.Info("gRPC server listening", slog.Int("port", cfg.Server.GRPCPort))
	if err := grpcServer.Serve(listener); err != nil {
		log.Error("gRPC server failed", slog.Any("error", err))
		os.Exit(1)
	}
	<-shutdownDone
}
