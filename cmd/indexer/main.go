package main

import (
	"log"

	"github.com/gorgohub/eth-indexer/internal/config"
	"github.com/gorgohub/eth-indexer/internal/storage"
)

func main() {
	log.Println("Starting Web3 Indexer service...")

	// Step 1: Load application configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}
	log.Println("Configuration successfully loaded.")

	// Step 2: Initialize database connection pool using loaded config
	db, err := storage.NewConnect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}

	// Professional defer block: log error if connection pool fails to close cleanly
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Warning: failed to close database connection pool: %v", closeErr)
		}
	}()

	log.Println("Database connection pool established from configuration parameters.")
	log.Printf("Ready to index Ethereum blockchain node at: %s", cfg.EthRPCURL)
}
