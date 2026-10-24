package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	accountv1 "github.com/amirzre/payment-system/gen/go/account/v1"
	authv1 "github.com/amirzre/payment-system/gen/go/auth/v1"
	paymentv1 "github.com/amirzre/payment-system/gen/go/payment/v1"
	"github.com/amirzre/payment-system/services/api-gateway/internal/config"
	gatewayhttp "github.com/amirzre/payment-system/services/api-gateway/internal/transport/http"
	"github.com/amirzre/payment-system/shared/logger"
	"github.com/amirzre/payment-system/shared/middleware"
	"github.com/amirzre/payment-system/shared/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Initialize configuration
	cfg := config.Load()

	// Initialize logger
	log := logger.New(cfg.Server.Environment, "api-gateway")
	log.Info("starting api-gateway", slog.Int("port", cfg.Server.HTTPPort))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	telemetry, err := observability.Init(ctx, "api-gateway")
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

	// gRPC connections
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

	paymentConn, _ := grpc.NewClient(cfg.Service.PaymentGRPCAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithChainUnaryInterceptor(
			middleware.UnaryClientCircuitBreaker(middleware.DefaultCircuitBreakerConfig()),
			middleware.UnaryClientRetry(middleware.DefaultRetryConfig()),
		),
	}, observability.GRPCClientOptions()...)...)
	defer func() { _ = paymentConn.Close() }()
	paymentClient := paymentv1.NewPaymentServiceClient(paymentConn)

	authConn, _ := grpc.NewClient(cfg.Service.AuthGRPCAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithChainUnaryInterceptor(
			middleware.UnaryClientCircuitBreaker(middleware.DefaultCircuitBreakerConfig()),
			middleware.UnaryClientRetry(middleware.DefaultRetryConfig()),
		),
	}, observability.GRPCClientOptions()...)...)
	defer func() { _ = authConn.Close() }()
	authClient := authv1.NewAuthServiceClient(authConn)

	// Handlers
	accountHandler := gatewayhttp.NewAccountHandler(accountClient)
	paymentHandler := gatewayhttp.NewPaymentHandler(paymentClient)
	authHandler := gatewayhttp.NewAuthHandler(authClient)

	mux := http.NewServeMux()

	// Public
	mux.HandleFunc("POST /v1/auth/register", authHandler.Register)
	mux.HandleFunc("POST /v1/auth/login", authHandler.Login)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Protected
	protected := http.NewServeMux()
	protected.HandleFunc("POST /v1/accounts", accountHandler.CreateAccount)
	protected.HandleFunc("GET /v1/accounts/{id}", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/v1/accounts/" + r.PathValue("id")
		accountHandler.GetAccount(w, r)
	})
	protected.HandleFunc("POST /v1/payments", paymentHandler.CreatePayment)
	protected.HandleFunc("GET /v1/payments/{id}", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/v1/payments/" + r.PathValue("id")
		paymentHandler.GetPayment(w, r)
	})
	protected.HandleFunc("POST /v1/accounts/{id}/deposit", accountHandler.Deposit)

	protectedHandler := gatewayhttp.AuthMiddleware(authClient)(protected)

	mux.Handle("/v1/accounts", protectedHandler)
	mux.Handle("/v1/accounts/", protectedHandler)
	mux.Handle("/v1/payments", protectedHandler)
	mux.Handle("/v1/payments/", protectedHandler)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.HTTPPort),
		Handler:           observability.HTTPHandler(log, mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info("shutting down")
		cancel()
		drainCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if err := server.Shutdown(drainCtx); err != nil {
			_ = server.Close()
		}
	}()

	log.Info("HTTP server listening", slog.Int("port", cfg.Server.HTTPPort))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("HTTP server failed", slog.Any("error", err))
		os.Exit(1)
	}
	<-shutdownDone
}
