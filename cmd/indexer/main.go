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

	// Step 1: Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}

	// Step 2: Connect to Database
	db, err := storage.NewConnect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Warning: failed to close database connection pool: %v", closeErr)
		}
	}()

	// Step 3: Connect to Ethereum
	ethClient, err := blockchain.NewClient(cfg.EthRPCURL)
	if err != nil {
		log.Fatalf("Blockchain client initialization failed: %v", err)
	}
	defer ethClient.Close()

	// Step 4: Run ETL Process for a specific historical block
	// Let's take a block close to the current height, for example, 25321800
	targetBlock := uint64(25321800)
	log.Printf("Starting ETL pipeline for Ethereum block #%d...", targetBlock)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// E & T: Extract and Transform transactions from blockchain
	txs, err := ethClient.GetBlockTransactions(ctx, targetBlock)
	if err != nil {
		log.Fatalf("ETL Extract failed: %v", err)
	}
	log.Printf("Extracted and normalized %d transactions from block #%d.", len(txs), targetBlock)

	// Исправлено: передаем 4 параметра, включая текущий Unix-timestamp (время)
	currentTimestamp := time.Now().Unix()
	err = db.SaveBlock(int64(targetBlock), "test_hash_25321800", "test_parent_hash", currentTimestamp)
	if err != nil {
		log.Fatalf("ETL Load failed: could not save block header: %v", err)
	}
	log.Printf("Block header #%d successfully registered in database.", targetBlock)

	// L: Load transactions into PostgreSQL within a single secure database transaction
	err = db.SaveTransactions(txs)
	if err != nil {
		log.Fatalf("ETL Load failed: could not save transactions to DB: %v", err)
	}

	log.Printf("🚀 ETL SUCCESS! %d transactions are now permanently indexed in PostgreSQL.", len(txs))
}
