package blockchain

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/time/rate"
)

type Client struct {
	ethClient *ethclient.Client
	limiter   *rate.Limiter
}

// NewClient initializes a new RPC client wrapper with an integrated rate limiter
func NewClient(rpcURL string, rps int) (*Client, error) {
	c, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, err
	}

	// Create a limiter allowing 'rps' requests per second with a maximum burst capacity of 'rps'
	limiter := rate.NewLimiter(rate.Limit(rps), rps)

	return &Client{
		ethClient: c,
		limiter:   limiter,
	}, nil
}

// Close gracefully shuts down the underlying RPC connections
func (c *Client) Close() {
	c.ethClient.Close()
}

// BlockNumber fetches the current tip of the blockchain, respecting the rate limit
func (c *Client) BlockNumber(ctx context.Context) (uint64, error) {
	// Block until the rate limiter allows the request or the context is canceled
	if err := c.limiter.Wait(ctx); err != nil {
		return 0, err
	}
	return c.ethClient.BlockNumber(ctx)
}

// BlockByNumber fetches a full block by its numeric identifier, respecting the rate limit
func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	// Block until the rate limiter allows the request or the context is canceled
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	return c.ethClient.BlockByNumber(ctx, number)
}
