package main

import (
	"log"
	"os"

	"github.com/gorgohub/eth-indexer/internal/storage"
)

func main() {
	log.Println("Starting Web3 Indexer service...")

	// Data Source Name (DSN) matching docker-compose.yaml credentials
	dsn := "postgres://indexer_user:indexer_password@localhost:5432/eth_indexer?sslmode=disable"

	// Initialize database connection
	db, err := storage.NewConnect(dsn)
	if err != nil {
		log.Fatalf("Initialization failed: %v", err)
	}
	defer db.Close()

	log.Println("Database connection successfully established and verified via Ping.")

	// Next steps will be triggered here
	os.Exit(0)
}
