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
	paymentv1 "github.com/amirzre/payment-system/gen/go/payment/v1"
	"github.com/amirzre/payment-system/services/payment/internal/app"
	"github.com/amirzre/payment-system/services/payment/internal/config"
	"github.com/amirzre/payment-system/services/payment/internal/repository"
	paymentconsumer "github.com/amirzre/payment-system/services/payment/internal/transport/consumer"
	paymentgrpc "github.com/amirzre/payment-system/services/payment/internal/transport/grpc"
	"github.com/amirzre/payment-system/shared/eventbus"
	"github.com/amirzre/payment-system/shared/healthcheck"
	"github.com/amirzre/payment-system/shared/logger"
	"github.com/amirzre/payment-system/shared/metrics"
	"github.com/amirzre/payment-system/shared/middleware"
	"github.com/amirzre/payment-system/shared/observability"
	"github.com/amirzre/payment-system/shared/redisclient"
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
	log := logger.New(cfg.Server.Environment, "payment-service")
	log.Info("starting payment-service", slog.Int("port", cfg.Server.GRPCPort))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	telemetry, err := observability.Init(ctx, "payment-service")
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

	// Connect to Redis
	var redisIdempotency *middleware.RedisIdempotency
	if cfg.Redis.RedisURL != "" {
		redisOptions, err := redisclient.Options(cfg.Redis.RedisURL)
		if err != nil {
			log.Error("invalid Redis configuration", slog.Any("error", err))
			os.Exit(1)
		}
		rdb := redis.NewClient(redisOptions)
		if err := rdb.Ping(ctx).Err(); err == nil {
			defer func() { _ = rdb.Close() }()
			log.Info("connected to Redis")
			redisIdempotency = middleware.NewRedisIdempotency(rdb, 24*time.Hour, "idempotency")
		} else {
			log.Warn("Redis unavailable, idempotency cache disabled", slog.Any("error", err))
			_ = rdb.Close()
		}
	}

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
			log.Info("connected to NATS")
		}
	}

	// gRPC clients with circuit breaker + retry
	accountConn, _ := grpc.NewClient(cfg.Service.AccountGRPCAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithChainUnaryInterceptor(
			middleware.UnaryClientCircuitBreaker(middleware.DefaultCircuitBreakerConfig()),
			middleware.UnaryClientRetry(middleware.DefaultRetryConfig()),
		),
	}, observability.GRPCClientOptions()...)...)
	defer func() { _ = accountConn.Close() }()
	accountClient := accountv1.NewAccountServiceClient(accountConn)

	ledgerConn, _ := grpc.NewClient(cfg.Service.LedgerGRPCAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithChainUnaryInterceptor(
			middleware.UnaryClientCircuitBreaker(middleware.DefaultCircuitBreakerConfig()),
			middleware.UnaryClientRetry(middleware.DefaultRetryConfig()),
		),
	}, observability.GRPCClientOptions()...)...)
	defer func() { _ = ledgerConn.Close() }()
	ledgerClient := ledgerv1.NewLedgerServiceClient(ledgerConn)

	// Construct dependency graph
	paymentRepo := repository.NewPaymentRepository(pool)
	paymentSrv := app.NewPaymentService(paymentRepo, accountClient, ledgerClient, redisIdempotency)

	// Outbox publisher
	if eb != nil {
		go eventbus.RunOutboxPublisher(ctx, pool, eb, log, 1*time.Second)

		// Event consumers with idempotency
		ledgerPostedHandler := eventbus.WithTransactionalIdempotency(pool, "payment-ledger-posted",
			paymentconsumer.LedgerPostedHandler(paymentRepo, log))
		if err := eb.Subscribe(ctx, "ledger.posted", "payment-service", ledgerPostedHandler); err != nil {
			log.Error("ledger notification subscription failed", slog.Any("error", err))
			os.Exit(1)
		}

	}

	// Set up gRPC server
	grpcServer := grpc.NewServer(observability.GRPCServerOption(), middleware.UnaryServerChain(log))
	paymentv1.RegisterPaymentServiceServer(grpcServer, paymentgrpc.NewPaymentServer(paymentSrv))
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
