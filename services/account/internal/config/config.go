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
	Database DatabaseConfig
	Redis    RedisConfig
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
	LedgerGRPCAddr string
}

// RedisConfig holds Redis configuration.
type RedisConfig struct {
	RedisURL string
}

// Load loads configuration from environment variables.
func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		ServerConfig{
			Environment: getEnv("SERVER_ENVIRONMENT", "development"),
			GRPCPort:    getIntEnv("SERVER_PORT", 50051),
		},
		ServiceConfig{
			LedgerGRPCAddr: getEnv("LEDGER_GRPC_ADDR", "http://localhost:50052"),
		},
		DatabaseConfig{
			DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgresql@localhost:5432/account?sslmode=disable"),
		},
		RedisConfig{
			RedisURL: getEnv("REDIS_URL", "redis://localhost:6379/0"),
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
