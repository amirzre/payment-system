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
	log := logger.New(cfg.Server.Environment, "payment-service")
	log.Info("starting payment-service", slog.Int("port", cfg.Server.GRPCPort))

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
	var redisIdempotency *middleware.RedisIdempotency
	if cfg.Redis.RedisURL != "" {
		rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.RedisURL})
		if rdb.Ping(ctx).Err() == nil {
			redisIdempotency = middleware.NewRedisIdempotency(rdb, 24*time.Hour, "idempotency")
		}
	}

	// Connect to NATS
	var eb *eventbus.NatsEventBus
	if cfg.Nats.NatsURL != "" {
		var err error
		eb, err = eventbus.NewNatsEventBus(cfg.Nats.NatsURL, log)
		if err != nil {
			log.Warn("NATS unavailable, event-driven saga disabled", slog.Any("error", err))
		} else {
			defer eb.Close()
			log.Info("connected to NATS")
		}
	}

	// gRPC clients with circuit breaker + retry
	accountConn, _ := grpc.NewClient(cfg.Service.AccountGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer accountConn.Close()
	accountClient := accountv1.NewAccountServiceClient(accountConn)

	ledgerConn, _ := grpc.NewClient(cfg.Service.LedgerGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer ledgerConn.Close()
	ledgerClient := ledgerv1.NewLedgerServiceClient(ledgerConn)

	// Construct dependency graph
	paymentRepo := repository.NewPaymentRepository(pool)
	paymentSrv := app.NewPaymentService(paymentRepo, accountClient, ledgerClient, redisIdempotency)

	// Outbox publisher
	if eb != nil {
		go eventbus.RunOutboxPublisher(context.Background(), pool, eb, log, 1*time.Second)

		// Event consumers with idempotency
		ledgerPostedHandler := eventbus.WithIdempotency(pool, "payment-ledger-posted",
			paymentconsumer.LedgerPostedHandler(paymentRepo, log))
		eb.Subscribe(context.Background(), "ledger.posted", "payment-service", ledgerPostedHandler)

		fundsFailHandler := eventbus.WithIdempotency(pool, "payment-funds-failed",
			paymentconsumer.FundsReservationFailedHandler(paymentRepo, log))
		eb.Subscribe(context.Background(), "funds.reservation.failed", "payment-service", fundsFailHandler)
	}

	// Set up gRPC server
	grpcServer := grpc.NewServer(middleware.UnaryServerChain(log))
	paymentv1.RegisterPaymentServiceServer(grpcServer, paymentgrpc.NewPaymentServer(paymentSrv))
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
