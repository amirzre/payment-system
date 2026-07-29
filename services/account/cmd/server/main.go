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

	accountv1 "github.com/amirzre/payment-system/gen/go/account/v1"
	ledgerv1 "github.com/amirzre/payment-system/gen/go/ledger/v1"
	"github.com/amirzre/payment-system/services/account/internal/app"
	"github.com/amirzre/payment-system/services/account/internal/config"
	"github.com/amirzre/payment-system/services/account/internal/repository"
	accountconsumer "github.com/amirzre/payment-system/services/account/internal/transport/consumer"
	accounttransport "github.com/amirzre/payment-system/services/account/internal/transport/grpc"
	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/amirzre/payment-system/shared/logger"
	"github.com/amirzre/payment-system/shared/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Initialize configuration
	cfg := config.Load()

	// Initialize logger
	log := logger.New(cfg.Server.Environment, "account-service")
	log.Info("starting account-service", slog.Int("port", cfg.Server.GRPCPort))

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

	// Connect to Redis
	var rdb *redis.Client
	if cfg.Redis.RedisURL != "" {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.Redis.RedisURL})
		if rdb.Ping(ctx).Err() != nil {
			rdb = nil
		}
	}

	// Connect to Nats
	var eb *eventbus.NatsEventBus
	if cfg.Nats.NatsURL != "" {
		var err error
		eb, err = eventbus.NewNatsEventBus(cfg.Nats.NatsURL, log)
		if err != nil {
			log.Warn("NATS unavailable", slog.Any("error", err))
		} else {
			defer eb.Close()
		}
	}

	// gRPC client to ledger-service
	ledgerConn, _ := grpc.NewClient(cfg.Service.LedgerGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			middleware.UnaryClientCircuitBreaker(middleware.DefaultCircuitBreakerConfig()),
			middleware.UnaryClientRetry(middleware.DefaultRetryConfig()),
		),
	)
	defer ledgerConn.Close()
	ledgerClient := ledgerv1.NewLedgerServiceClient(ledgerConn)

	// Construct dependency graph
	accountRepo := repository.NewAccountRepository(pool)
	accountSrv := app.NewAccountService(accountRepo, ledgerClient, rdb)
	accountServer := accounttransport.NewAccountServer(accountSrv)

	// Outbox + consumers
	if eb != nil {
		go eventbus.RunOutboxPublisher(context.Background(), pool, eb, log, 1*time.Second)

		selfConn, _ := grpc.NewClient(fmt.Sprintf("localhost:%d", cfg.Server.GRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		selfClient := accountv1.NewAccountServiceClient(selfConn)

		payReqHandler := eventbus.WithIdempotency(pool, "account-payment-requested",
			accountconsumer.PaymentRequestedHandler(selfClient, eb, log))
		eb.Subscribe(context.Background(), "payment.requested", "account-service", payReqHandler)

		cacheHandler := eventbus.WithIdempotency(pool, "account-cache-invalidation",
			accountconsumer.CacheInvalidationHandler(rdb, log))
		eb.Subscribe(context.Background(), "ledger.posted", "account-service-cache", cacheHandler)
	}

	// Set up gRPC server
	grpcServer := grpc.NewServer(middleware.UnaryServerChain(log))
	accountv1.RegisterAccountServiceServer(grpcServer, accountServer)

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
		grpcServer.GracefulStop()
	}()

	log.Info("gRPC server listening", slog.Int("port", cfg.Server.GRPCPort))
	if err := grpcServer.Serve(listener); err != nil {
		log.Error("gRPC server failed", slog.Any("error", err))
		os.Exit(1)
	}
}
