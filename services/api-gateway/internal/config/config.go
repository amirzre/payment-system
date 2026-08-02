package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds the configuration for the account service.
type Config struct {
	Server   ServerConfig
	Service  ServiceConfig
}

// ServerConfig holds server configuration.
type ServerConfig struct {
	Environment string
	HTTPPort    int
}

// ServiceConfig holds services configuration.
type ServiceConfig struct {
	AccountGRPCAddr string
	PaymentGRPCAddr string
	AuthGRPCAddr    string
}

// Load loads configuration from environment variables.
func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		ServerConfig{
			Environment: getEnv("SERVER_ENVIRONMENT", "development"),
			HTTPPort:    getIntEnv("SERVER_PORT", 8080),
		},
		ServiceConfig{
			AccountGRPCAddr: getEnv("ACCOUNT_GRPC_ADDR", "http://localhost:50051"),
			PaymentGRPCAddr:  getEnv("PAYMENT_GRPC_ADDR", "http://localhost:50053"),
			AuthGRPCAddr: getEnv("AUTH_GRPC_ADDR", "http://localhost:50054"),
		},
	}
}

// getEnv gets string environment variable with default value.
func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

// getIntEnv gets integer environment variable with default value.
func getIntEnv(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}

	return fallback
}
