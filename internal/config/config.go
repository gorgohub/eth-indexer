package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Config holds all configuration parameters for the application
type Config struct {
	DatabaseURL string
	EthRPCURL   string
}

// Load reads configuration from .env file and environment variables
func Load() (*Config, error) {
	// Load .env file if it exists.
	// In production environments, variables are usually set directly in the OS,
	// so we don't fail if the .env file itself is missing.
	if err := godotenv.Load(); err != nil {
		// We do not return an error here, just proceed to check system env vars
		log.Printf("Notice: .env file not found, relying on system environment variables")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is missing")
	}

	ethURL := os.Getenv("ETH_RPC_URL")
	if ethURL == "" {
		return nil, fmt.Errorf("ETH_RPC_URL environment variable is missing")
	}

	return &Config{
		DatabaseURL: dbURL,
		EthRPCURL:   ethURL,
	}, nil
}
