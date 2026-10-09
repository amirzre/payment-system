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

	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	"github.com/amirzre/payment-system/services/ledger/internal/app"
	"github.com/amirzre/payment-system/services/ledger/internal/config"
	"github.com/amirzre/payment-system/services/ledger/internal/repository"
	ledgertransport "github.com/amirzre/payment-system/services/ledger/internal/transport/grpc"
	"github.com/amirzre/payment-system/shared/eventbus"
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
	log := logger.New(cfg.Server.Environment, "ledger-service")
	log.Info("starting ledger-service", slog.Int("port", cfg.Server.GRPCPort))

	// Connect to Database
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	// Connect to NATS
	var eb *eventbus.NatsEventBus
	if cfg.Nats.NatsURL != "" {
		var err error
		eb, err = eventbus.NewNatsEventBus(cfg.Nats.NatsURL, log)
		if err != nil {
			log.Error("NATS setup failed", slog.Any("error", err))
			os.Exit(1)
		} else {
			defer eb.Close()
		}
	}

	// Construct dependency graph
	ledgerRepo := repository.NewLedgerRepository(pool)
	ledgerSrv := app.NewLedgerEntryService(ledgerRepo)
	ledgerServer := ledgertransport.NewLedgerServer(ledgerSrv)

	// Outbox + consumers
	if eb != nil {
		go eventbus.RunOutboxPublisher(ctx, pool, eb, log, 1*time.Second)

	}

	// Set up gRPC server
	grpcServer := grpc.NewServer(middleware.UnaryServerChain(log))
	ledgerv1.RegisterLedgerServiceServer(grpcServer, ledgerServer)

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
		cancel()
		healthServer.Shutdown()
		grpcServer.GracefulStop()
	}()

	log.Info("gRPC server listening", slog.Int("port", cfg.Server.GRPCPort))
	if err := grpcServer.Serve(listener); err != nil {
		log.Error("gRPC server failed", slog.Any("error", err))
		os.Exit(1)
	}
}
