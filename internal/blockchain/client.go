package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/ethclient"
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
