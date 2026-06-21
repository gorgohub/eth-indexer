package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorgohub/eth-indexer/internal/api"
	"github.com/gorgohub/eth-indexer/internal/blockchain"
	"github.com/gorgohub/eth-indexer/internal/config"
	"github.com/gorgohub/eth-indexer/internal/storage"
	"github.com/gorgohub/eth-indexer/internal/sync"
)

func main() {
	log.Println("Starting Web3 Indexer service...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}

	db, err := storage.NewConnect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}

	ethClient, err := blockchain.NewClient(cfg.EthRPCURL)
	if err != nil {
		log.Fatalf("Blockchain client initialization failed: %v", err)
	}

	// Setup graceful shutdown handling for SIGINT (Ctrl+C) and SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize the HTTP API server
	apiServer := api.NewServer(db)

	// Launch the HTTP server asynchronously in a goroutine to prevent blocking the main execution flow
	go func() {
		if err := apiServer.Start(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Critical API HTTP server failure: %v", err)
		}
	}()

	syncer := sync.NewSyncer(db, ethClient)

	log.Println("ETL Worker engine successfully armed. Entering infinite processing loop...")

	if err := syncer.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Println("Indexer service stopped cleanly via graceful shutdown.")
		} else {
			log.Printf("Critical sync loop failure: %v", err)
		}
	}

	// Gracefully release active network resources and connection pools after loop termination
	log.Println("Closing underlying network connections and DB pools...")
	ethClient.Close()

	if err := db.Close(); err != nil {
		log.Printf("Failed to cleanly close database connection pool: %v", err)
	}

	log.Println("Shutdown complete. Bye!")
}
