package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds the configuration for the account service.
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Service  ServiceConfig
	Redis    RedisConfig
	Nats     NatsConfig
}

// ServerConfig holds server configuration.
type ServerConfig struct {
	Environment string
	GRPCPort    int
}

// DatabaseConfig holds database configuration.
type DatabaseConfig struct {
	DatabaseURL string
}

// ServiceConfig holds services configuration.
type ServiceConfig struct {
	AccountGRPCAddr string
	LedgerGRPCAddr  string
}

// RedisConfig holds Redis configuration.
type RedisConfig struct {
	RedisURL string
}

// NatsConfig holds Nats configuration.
type NatsConfig struct {
	NatsURL string
}

// Load loads configuration from environment variables.
func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		ServerConfig{
			Environment: getEnv("SERVER_ENVIRONMENT", "development"),
			GRPCPort:    getIntEnv("SERVER_PORT", 50053),
		},
		DatabaseConfig{
			DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgresql@localhost:5434/payment?sslmode=disable"),
		},
		ServiceConfig{
			AccountGRPCAddr: getEnv("ACCOUNT_GRPC_ADDR", "http://localhost:50051"),
			LedgerGRPCAddr:  getEnv("LEDGER_GRPC_ADDR", "http://localhost:50052"),
		},
		RedisConfig{
			RedisURL: getEnv("REDIS_URL", "redis://localhost:6379/0"),
		},
		NatsConfig{
			NatsURL: getEnv("NATS_URL", "nats://localhost:4222"),
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
