package sync

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gorgohub/eth-indexer/internal/blockchain"
	"github.com/gorgohub/eth-indexer/internal/storage"
)

// Syncer orchestrates the ETL pipeline loop between Ethereum and PostgreSQL
type Syncer struct {
	db     *storage.DB
	client *blockchain.Client
}

// NewSyncer creates a new synchronizer instance
func NewSyncer(db *storage.DB, client *blockchain.Client) *Syncer {
	return &Syncer{
		db:     db,
		client: client,
	}
}

// Start launches the infinite loop tracking and extracting new blocks
func (s *Syncer) Start(ctx context.Context) error {
	log.Println("Initializing synchronization loop...")

	// 1. Determine our starting point based on DB state
	startBlock, err := s.db.GetLatestSavedBlock()
	if err != nil {
		return fmt.Errorf("syncer startup failed: %w", err)
	}

	// If database is brand new, we can start from a recent historical hardcoded block
	// to avoid scanning from block zero (which took place in 2015)
	if startBlock == 0 {
		startBlock = 25321800 // Our baseline test block
		log.Printf("Database is empty. Starting synchronization from baseline block #%d", startBlock)
	} else {
		log.Printf("Resuming synchronization from last saved block #%d", startBlock)
		// Increment by 1 to start fetching the next unseen block
		startBlock++
	}

	currentBlock := uint64(startBlock)

	for {
		// Respect context cancellation (e.g. if someone presses Ctrl+C)
		select {
		case <-ctx.Done():
			log.Println("Synchronization worker received shutdown signal. Exiting loop.")
			return ctx.Err()
		default:
		}

		// 2. Query the network for the current highest block
		networkHeight, err := s.client.GetLatestBlockNumber(ctx)
		if err != nil {
			log.Printf("Network error: failed to query latest block height: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// 3. Check if we have caught up with the network
		if currentBlock > networkHeight {
			log.Printf("Fully synchronized with blockchain height #%d. Waiting for new blocks...", networkHeight)
			// Ethereum block time is ~12 seconds. Sleep to prevent spamming the node.
			time.Sleep(12 * time.Second)
			continue
		}

		// 4. Execute full ETL cycle for the current block
		log.Printf("Processing block #%d/%d...", currentBlock, networkHeight)

		// Set a strict timeout per individual block extraction to prevent network hanging
		blockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)

		// Принимаем реальные транзакции и данные заголовка блока
		txs, headerData, err := s.client.GetBlockTransactions(blockCtx, currentBlock)
		cancel()
		if err != nil {
			log.Printf("ETL Error: failed to extract data for block %d: %v. Retrying...", currentBlock, err)
			time.Sleep(3 * time.Second)
			continue
		}

		// Заглушки dummyHash, dummyParent и mockTime БОЛЬШЕ НЕ НУЖНЫ.
		// Передаем в базу настоящие данные из блокчейна:
		err = s.db.SaveBlock(int64(currentBlock), headerData.Hash, headerData.ParentHash, headerData.Time)
		if err != nil {
			log.Printf("Database Error: failed to write block header %d: %v. Retrying...", currentBlock, err)
			time.Sleep(3 * time.Second)
			continue
		}

		err = s.db.SaveTransactions(txs)
		if err != nil {
			log.Printf("Database Error: failed to commit transactions for block %d: %v. Retrying...", currentBlock, err)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("Successfully indexed block #%d (%d transactions committed).", currentBlock, len(txs))

		// Move to the next block sequentially
		currentBlock++
	}
}
