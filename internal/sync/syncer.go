package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
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

	// Use WaitGroup to synchronize clean termination of background worker routines
	var wg sync.WaitGroup

	// 3. Spawn long-lived worker goroutines
	for w := 1; w <= s.workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.worker(ctx, jobsChan, resultsChan)
		}()
	}

	// 4. Run the dispatcher goroutine to stream block numbers into jobsChan
	go func() {
		defer close(jobsChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Fetch current tip using exponential backoff to handle network fragility safely
				latestBlock, err := s.fetchBlockNumberWithRetry(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Critical timeout reaching blockchain tip node: %v", err)
					time.Sleep(2 * time.Second)
					continue
				}

				if currentBlock > latestBlock {
					// Caught up with the chain tip, wait for a new block to be minted
					select {
					case <-ctx.Done():
						return
					case <-time.After(3 * time.Second):
						continue
					}
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

	// Close resultsChan only after all workers completely finish pulling from jobsChan
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 5. Main thread collects worker results and commits them into storage
	for res := range resultsChan {
		if res.err != nil {
			log.Printf("Worker reported unrecoverable error for block %d: %v", res.blockNum, res.err)
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

	// Explicitly return context error if shutdown was triggered during execution loop
	if ctx.Err() != nil {
		return ctx.Err()
	}

	return nil
}

// worker listens to the job channel, processes the data, and pipes the outcome to resultsChan
func (s *Syncer) worker(ctx context.Context, jobs <-chan uint64, results chan<- blockResult) {
	for blockNum := range jobs {
		blockBig := new(big.Int).SetUint64(blockNum)

		// Pass the legitimate application context downwards
		ethBlock, err := s.fetchBlockWithRetry(ctx, blockBig)
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

// fetchBlockNumberWithRetry polls blockchain tip height using progressive backoff mechanics
func (s *Syncer) fetchBlockNumberWithRetry(ctx context.Context) (uint64, error) {
	delay := 1 * time.Second
	for {
		latestBlock, err := s.client.BlockNumber(ctx)
		if err == nil {
			return latestBlock, nil
		}

		if errors.Is(ctx.Err(), context.Canceled) {
			return 0, ctx.Err()
		}

		log.Printf("RPC error getting tip block number: %v. Retrying in %v...", err, delay)

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}

		delay *= 2
	}
}

// fetchBlockWithRetry handles block data collection safely protecting internal pipelines from crashing
func (s *Syncer) fetchBlockWithRetry(ctx context.Context, number *big.Int) (*types.Block, error) {
	delay := 1 * time.Second
	maxRetryTime := time.After(30 * time.Second)

	for {
		// If the main context was canceled (Ctrl+C), we switch to a background context
		// with a strict timeout to let this specific block finish saving without data loss.
		activeCtx := ctx
		if errors.Is(ctx.Err(), context.Canceled) {
			activeCtx = context.Background()
		}

		reqCtx, cancel := context.WithTimeout(activeCtx, 5*time.Second)
		ethBlock, err := s.client.BlockByNumber(reqCtx, number)
		cancel()

		if err == nil {
			return ethBlock, nil
		}

		// If it failed because the application is shutting down, and we exceeded limits, abort
		if errors.Is(ctx.Err(), context.Canceled) && delay > 4*time.Second {
			return nil, fmt.Errorf("shutdown requested, abandoning block %s: %w", number.String(), err)
		}

		log.Printf("RPC error retrieving block metrics for %s: %v. Retrying in %v...", number.String(), err, delay)

		select {
		case <-maxRetryTime:
			return nil, fmt.Errorf("failed to fetch block %s after 30s timeout: %w", number.String(), err)
		case <-time.After(delay):
		}

		delay *= 2
		if delay > 8*time.Second {
			delay = 8 * time.Second // Cap max delay
		}
	}
}
