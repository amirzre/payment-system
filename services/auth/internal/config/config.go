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
}

// ServerConfig holds server configuration.
type ServerConfig struct {
	Environment   string
	GRPCPort      int
	PrivateKeyPEM string
	PublicKeyPEM  string
}

// DatabaseConfig holds database configuration.
type DatabaseConfig struct {
	DatabaseURL string
}

// Load loads configuration from environment variables.
func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		ServerConfig{
			Environment:   getEnv("SERVER_ENVIRONMENT", "development"),
			GRPCPort:      getIntEnv("SERVER_PORT", 50054),
			PrivateKeyPEM: getEnv("PRIVATE_KEY_PEM", ""),
			PublicKeyPEM:  getEnv("PUBLIC_KEY_PEM", ""),
		},
		DatabaseConfig{
			DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgresql@localhost:5435/auth?sslmode=disable"),
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
