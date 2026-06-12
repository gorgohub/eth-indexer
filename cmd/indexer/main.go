package main

import (
	"log"
	"time"

	"github.com/gorgohub/eth-indexer/internal/storage"
)

func main() {
	log.Println("Starting Indexer Test DB Write...")

	dsn := "postgres://indexer_user:indexer_password@localhost:5432/eth_indexer?sslmode=disable"

	// Initialize database connection
	db, err := storage.NewConnect(dsn)
	if err != nil {
		log.Fatalf("Initialization failed: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Warning: failed to close database connection cleanly: %v", err)
		}
	}()

	// Create fake/mock Ethereum block data for testing
	testBlock := storage.Block{
		BlockNumber:    18000000,
		BlockHash:      "0x61a8db8615bee4356cf9e31d471b402ebcf0ef0a9c68cf5b2c79f1df7ef95a86",
		ParentHash:     "0x411ed0398f623b08fa1160a2b535ff206a4b3d7cfdf331166418cf0377ee7144",
		BlockTimestamp: time.Now().UTC(),
	}

	// Try to execute the database insert
	err = db.SaveBlock(testBlock)
	if err != nil {
		log.Fatalf("Failed to save block to DB: %v", err)
	}

	log.Println("Success! Test block 18000000 was successfully written to PostgreSQL.")
}
