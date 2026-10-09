package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	authv1 "github.com/amirzre/payment-system/gen/go/auth/v1"
	"github.com/amirzre/payment-system/services/auth/internal/app"
	"github.com/amirzre/payment-system/services/auth/internal/config"
	"github.com/amirzre/payment-system/services/auth/internal/domain"
	"github.com/amirzre/payment-system/services/auth/internal/repository"
	authgrpc "github.com/amirzre/payment-system/services/auth/internal/transport/grpc"
	"github.com/amirzre/payment-system/shared/healthcheck"
	"github.com/amirzre/payment-system/shared/logger"
	"github.com/amirzre/payment-system/shared/middleware"
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

	// Connect to Database
	ctx := context.Background()
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
	grpcServer := grpc.NewServer(middleware.UnaryServerChain(log))
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

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Info("received signal, shutting down", slog.String("signal", sig.String()))
		healthServer.Shutdown()
		grpcServer.GracefulStop()
	}()

	log.Info("gRPC server listening", slog.Int("port", cfg.Server.GRPCPort))
	if err := grpcServer.Serve(listener); err != nil {
		log.Error("gRPC server failed", slog.Any("error", err))
		os.Exit(1)
	}
}
