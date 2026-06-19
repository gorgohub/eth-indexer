package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gorgohub/eth-indexer/internal/storage"
)

// Client wraps the official go-ethereum RPC client
type Client struct {
	rpcClient *ethclient.Client
}

// NewClient initializes a new connection to the Ethereum RPC node
func NewClient(rawURL string) (*Client, error) {
	// Connect to the Ethereum node via JSON-RPC
	client, err := ethclient.Dial(rawURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum RPC node: %w", err)
	}

	return &Client{
		rpcClient: client,
	}, nil
}

// Close gracefully closes the underlying RPC network connection
func (c *Client) Close() {
	c.rpcClient.Close()
}

// GetLatestBlockNumber fetches the index of the most recent block in the network
func (c *Client) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	// Query the node for the latest block header (passing nil means "latest")
	header, err := c.rpcClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch latest block header: %w", err)
	}

	// Verify that header and its number are not nil before accessing data
	if header == nil || header.Number == nil {
		return 0, fmt.Errorf("received empty or invalid block header from node")
	}

	return header.Number.Uint64(), nil
}

// GetBlockNumberByHash fetches the block number using its explicit cryptographic hash
func (c *Client) GetBlockNumberByHash(ctx context.Context, hashStr string) (*big.Int, error) {
	// This placeholder method will be expanded when we implement Fork/Reorg handling
	return nil, nil
}

// GetBlockTransactions fetches a full block by its number and normalizes its transactions for our database
func (c *Client) GetBlockTransactions(ctx context.Context, blockNumber uint64) ([]storage.Transaction, error) {
	// Convert uint64 block number to big.Int required by go-ethereum SDK
	bigNumber := new(big.Int).SetUint64(blockNumber)

	// Fetch full block data from the RPC node
	rawBlock, err := c.rpcClient.BlockByNumber(ctx, bigNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block %d from node: %w", blockNumber, err)
	}
	if rawBlock == nil {
		return nil, fmt.Errorf("received empty block pointer for height %d", blockNumber)
	}

	// Fetch the ChainID needed to correctly decode the transaction sender ("From" address)
	chainID, err := c.rpcClient.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chain ID: %w", err)
	}
	signer := types.LatestSignerForChainID(chainID)

	rawTxs := rawBlock.Transactions()
	normalizedTxs := make([]storage.Transaction, 0, len(rawTxs))

	// Loop through all raw transactions and transform them into our storage schema
	for _, tx := range rawTxs {
		if tx == nil {
			continue
		}

		// Extract the sender address ("From"). This requires cryptographic signer decoding.
		fromAddr, err := types.Sender(signer, tx)
		if err != nil {
			// If decoding fails for a single tx, log it and skip to prevent stopping the entire sync
			continue
		}

		// Extract the recipient address ("To"). It can be nil if the transaction creates a smart contract.
		var toAddrStr *string
		if tx.To() != nil {
			str := tx.To().Hex()
			toAddrStr = &str
		}

		// Safely convert types to match our database types (uint64 to int64, big.Int to string)
		normalizedTx := storage.Transaction{
			TxHash:      tx.Hash().Hex(),
			BlockNumber: int64(blockNumber),
			FromAddress: fromAddr.Hex(),
			ToAddress:   toAddrStr,
			Value:       tx.Value().String(), // uint256 converted to exact string representation
			GasPrice:    tx.GasPrice().Int64(),
			GasLimit:    int64(tx.Gas()),
			Nonce:       int64(tx.Nonce()),
		}

		normalizedTxs = append(normalizedTxs, normalizedTx)
	}

	return normalizedTxs, nil
}
