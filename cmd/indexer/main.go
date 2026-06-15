package main

import (
	"context"
	"log"
	"time"

	"github.com/gorgohub/eth-indexer/internal/blockchain"
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

	// Step 2: Initialize database connection pool
	db, err := storage.NewConnect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Warning: failed to close database connection pool: %v", closeErr)
		}
	}()
	log.Println("Database connection pool established.")

	// Step 3: Initialize Ethereum RPC Web3 Client
	ethClient, err := blockchain.NewClient(cfg.EthRPCURL)
	if err != nil {
		log.Fatalf("Blockchain client initialization failed: %v", err)
	}
	// Gracefully close RPC connection on application shutdown
	defer ethClient.Close()
	log.Printf("Connected to Ethereum RPC node at: %s", cfg.EthRPCURL)

	// Step 4: Test network connectivity by fetching the latest block number
	// Set a strict 5-second timeout for the network request to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	latestBlock, err := ethClient.GetLatestBlockNumber(ctx)
	if err != nil {
		log.Fatalf("Network test failed: could not query latest block: %v", err)
	}

	log.Printf("🔥 SUCCESS! Current Ethereum Block Height: #%d", latestBlock)
	log.Println("Indexer infrastructure is fully ready for ETL stream processing.")
}
