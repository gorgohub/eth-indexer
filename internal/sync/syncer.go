package sync

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/gorgohub/eth-indexer/internal/blockchain"
	"github.com/gorgohub/eth-indexer/internal/storage"
)

type Syncer struct {
	client      *blockchain.Client
	store       *storage.DB
	workerCount int
}

// blockResult wraps data fetched by background workers to pass into the main recording thread
type blockResult struct {
	blockNum uint64
	hash     string
	pHash    string
	time     uint64
	err      error
}

func NewSyncer(store *storage.DB, client *blockchain.Client, workerCount int) *Syncer {
	if workerCount <= 0 {
		workerCount = 3 // Safe local machine default to avoid CPU overheating
	}
	return &Syncer{
		client:      client,
		store:       store,
		workerCount: workerCount,
	}
}

// Start initiates the long-running pipeline for streaming block sync
func (s *Syncer) Start(ctx context.Context) error {
	log.Println("Initializing concurrent synchronization loop with Worker Pool...")

	// 1. Determine the exact block height to resume extraction from
	lastSavedBlock, err := s.store.GetLatestSavedBlock()
	if err != nil {
		return fmt.Errorf("syncer startup failed: failed to fetch max block number: %w", err)
	}

	var currentBlock uint64
	if lastSavedBlock > 0 {
		currentBlock = uint64(lastSavedBlock) + 1
	} else {
		currentBlock = 1
	}

	// 2. Initialize bound pipeline channels
	jobsChan := make(chan uint64, s.workerCount*2)
	resultsChan := make(chan blockResult, s.workerCount*2)

	// 3. Spawn long-lived worker goroutines
	for w := 1; w <= s.workerCount; w++ {
		go s.worker(ctx, jobsChan, resultsChan)
	}

	// 4. Run the dispatcher goroutine to stream block numbers into jobsChan
	go func() {
		defer close(jobsChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				latestBlock, err := s.client.BlockNumber(ctx)
				if err != nil {
					log.Printf("Error fetching latest block number: %v. Retrying in 2s...", err)
					time.Sleep(2 * time.Second)
					continue
				}

				if currentBlock > latestBlock {
					// Caught up with the chain tip, wait for a new block to be minted
					time.Sleep(3 * time.Second)
					continue
				}

				select {
				case <-ctx.Done():
					return
				case jobsChan <- currentBlock:
					currentBlock++
				}
			}
		}
	}()

	// 5. Main thread collects worker results and commits them into storage
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case res, ok := <-resultsChan:
			if !ok {
				return nil
			}
			if res.err != nil {
				log.Printf("Worker reported error for block %d: %v", res.blockNum, res.err)
				continue
			}

			// Save the block header metadata directly using the storage.DB instance
			err := s.store.SaveBlock(int64(res.blockNum), res.hash, res.pHash, int64(res.time))
			if err != nil {
				log.Printf("Failed to commit block %d to database: %v", res.blockNum, err)
				continue
			}
			log.Printf("Successfully synchronized block %d to storage", res.blockNum)
		}
	}
}

// worker listens to the job channel, processes the data, and pipes the outcome to resultsChan
func (s *Syncer) worker(ctx context.Context, jobs <-chan uint64, results chan<- blockResult) {
	for blockNum := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
			blockBig := new(big.Int).SetUint64(blockNum)
			ethBlock, err := s.client.BlockByNumber(ctx, blockBig)
			if err != nil {
				results <- blockResult{blockNum: blockNum, err: err}
				continue
			}

			results <- blockResult{
				blockNum: blockNum,
				hash:     ethBlock.Hash().Hex(),
				pHash:    ethBlock.ParentHash().Hex(),
				time:     ethBlock.Time(),
				err:      nil,
			}
		}
	}
}
