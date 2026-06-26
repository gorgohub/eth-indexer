package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all configuration parameters for the application
type Config struct {
	DatabaseURL string
	EthRPCURL   string
	RPS         int
	WorkerCount int
}

// Load reads configuration from .env file and environment variables
func Load() (*Config, error) {
	// Load .env file if it exists.
	// In production environments, variables are usually set directly in the OS,
	// so we don't fail if the .env file itself is missing.
	if err := godotenv.Load(); err != nil {
		// We do not return an error here, just proceed to check system env vars
		slog.Info("Notice: .env file not found, relying on system environment variables")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is missing")
	}

	ethURL := os.Getenv("ETH_RPC_URL")
	if ethURL == "" {
		return nil, fmt.Errorf("ETH_RPC_URL environment variable is missing")
	}

	// Safe default: 15 requests per second if not specified
	rps := 15
	if rpsEnv := os.Getenv("ETH_RPS_LIMIT"); rpsEnv != "" {
		if val, err := strconv.Atoi(rpsEnv); err == nil && val > 0 {
			rps = val
		}
	}

	// Safe default: 3 concurrent workers if not specified
	workerCount := 3
	if workerEnv := os.Getenv("SYNCER_WORKER_COUNT"); workerEnv != "" {
		if val, err := strconv.Atoi(workerEnv); err == nil && val > 0 {
			workerCount = val
		}
	}

	return &Config{
		DatabaseURL: dbURL,
		EthRPCURL:   ethURL,
		RPS:         rps,
		WorkerCount: workerCount,
	}, nil
}
