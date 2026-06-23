package sync

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gorgohub/eth-indexer/internal/blockchain"
	"github.com/gorgohub/eth-indexer/internal/notifier"
	"github.com/gorgohub/eth-indexer/internal/storage"
)

// blockResult wraps fetched block metadata and its normalized transactions
type blockResult struct {
	blockNumber uint64
	header      blockchain.BlockHeaderData
	txs         []storage.Transaction
	err         error
}

// Syncer orchestrates the ETL pipeline loop between Ethereum and PostgreSQL
// Добавьте импорт "github.com/gorgohub/eth-indexer/internal/notifier" в блок import
type Syncer struct {
	db       *storage.DB
	client   *blockchain.Client
	notifier *notifier.Notifier // Added notifier dependency
}

// NewSyncer creates a new synchronizer instance
func NewSyncer(db *storage.DB, client *blockchain.Client) *Syncer {
	return &Syncer{
		db:       db,
		client:   client,
		notifier: notifier.NewNotifier(), // Initialize notifier service
	}
}

// Start launches a concurrent worker pool to fast-track block extraction and storage
func (s *Syncer) Start(ctx context.Context) error {
	log.Println("Initializing concurrent synchronization loop...")

	startBlock, err := s.db.GetLatestSavedBlock()
	if err != nil {
		return fmt.Errorf("syncer startup failed: %w", err)
	}

	if startBlock == 0 {
		startBlock = 25321800
		log.Printf("Database is empty. Starting synchronization from baseline block #%d", startBlock)
	} else {
		log.Printf("Resuming synchronization from last saved block #%d", startBlock)
		startBlock++
	}

	networkHeight, err := s.client.GetLatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch initial network height: %w", err)
	}

	currentBlock := uint64(startBlock)

	// Define bounds for our concurrent worker pool
	workerCount := 5
	jobsChan := make(chan uint64, workerCount)
	resultsChan := make(chan blockResult, workerCount)

	// 1. Spawn concurrent downloader workers
	for w := 1; w <= workerCount; w++ {
		go func(workerID int) {
			for blockNum := range jobsChan {
				// Each block download has its own independent strict network timeout
				reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				txs, headerData, fetchErr := s.client.GetBlockTransactions(reqCtx, blockNum)
				cancel()

				resultsChan <- blockResult{
					blockNumber: blockNum,
					header:      headerData,
					txs:         txs,
					err:         fetchErr,
				}
			}
		}(w)
	}

	// Ensure channels close cleanly when we drop out of the control loop
	defer close(jobsChan)

	log.Printf("Spawned %d concurrent ETL execution workers.", workerCount)

	// 2. Control pipeline orchestration loop
	for {
		select {
		case <-ctx.Done():
			log.Println("Synchronization worker received shutdown signal. Exiting loop.")
			return ctx.Err()
		default:
		}

		// Update network height dynamically if we are catching up close to the tip
		if currentBlock > networkHeight {
			networkHeight, err = s.client.GetLatestBlockNumber(ctx)
			if err != nil {
				log.Printf("Network error: failed to query latest block height: %v. Retrying in 5s...", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if currentBlock > networkHeight {
				log.Printf("Fully synchronized with blockchain height #%d. Waiting for new blocks...", networkHeight)
				time.Sleep(12 * time.Second)
				continue
			}
		}

		// Feed the jobs channel up to worker capacity
		activeJobs := 0
		for i := 0; i < workerCount && (currentBlock+uint64(i)) <= networkHeight; i++ {
			jobsChan <- currentBlock + uint64(i)
			activeJobs++
		}

		// Collect and strictly commit results sequentially to maintain historical order
		hasErrors := false
		for i := 0; i < activeJobs; i++ {
			res := <-resultsChan
			if res.err != nil {
				log.Printf("ETL Error: worker failed to extract data for block %d: %v", res.blockNumber, res.err)
				hasErrors = true
				continue
			}

			// Commit to Database
			err = s.db.SaveBlock(int64(res.blockNumber), res.header.Hash, res.header.ParentHash, res.header.Time)
			if err != nil {
				log.Printf("Database Error: failed to write block header %d: %v", res.blockNumber, err)
				hasErrors = true
				continue
			}

			err = s.db.SaveTransactions(res.txs)
			if err != nil {
				log.Printf("Database Error: failed to commit transactions for block %d: %v", res.blockNumber, err)
				hasErrors = true
				continue
			}

			// Trigger the real-time notification layer for parsed token transfers
			s.notifier.CheckAndNotify(res.txs[i].TokenMoves)

			log.Printf("Successfully indexed block #%d (%d transactions committed).", res.blockNumber, len(res.txs))
		}

		// If any error occurred during this batch, we do not increment currentBlock, forcing a safe retry
		if hasErrors {
			time.Sleep(3 * time.Second)
			continue
		}

		// Advance our pointer by the size of the successfully processed batch
		currentBlock += uint64(activeJobs)
	}
}
