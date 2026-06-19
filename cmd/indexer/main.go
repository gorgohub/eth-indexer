package main

import (
	"context"
	"log"

	"github.com/gorgohub/eth-indexer/internal/blockchain"
	"github.com/gorgohub/eth-indexer/internal/config"
	"github.com/gorgohub/eth-indexer/internal/storage"
	"github.com/gorgohub/eth-indexer/internal/sync"
)

func main() {
	log.Println("Starting Web3 Indexer service...")

	// Step 1: Load environment variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}

	// Step 2: Establish connection pool with PostgreSQL
	db, err := storage.NewConnect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Warning: failed to close database connection pool: %v", closeErr)
		}
	}()

	// Step 3: Initialize network Ethereum RPC client
	ethClient, err := blockchain.NewClient(cfg.EthRPCURL)
	if err != nil {
		log.Fatalf("Blockchain client initialization failed: %v", err)
	}
	defer ethClient.Close()

	// Step 4: Setup and run the background background synchronization loop
	syncer := sync.NewSyncer(db, ethClient)

	// Create a long-lived base context for the indexer worker daemon
	ctx := context.Background()

	log.Println("ETL Worker engine successfully armed. Entering infinite processing loop...")
	if err := syncer.Start(ctx); err != nil {
		log.Fatalf("Critical sync loop failure: %v", err)
	}
}
