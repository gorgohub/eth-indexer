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

// BlockHeaderData holds normalized block metadata needed for DB insertion
type BlockHeaderData struct {
	Hash       string
	ParentHash string
	Time       int64
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

// GetBlockTransactions fetches a full block by its number, normalizes its metadata and its transactions
func (c *Client) GetBlockTransactions(ctx context.Context, blockNumber uint64) ([]storage.Transaction, BlockHeaderData, error) {
	bigNumber := new(big.Int).SetUint64(blockNumber)

	rawBlock, err := c.rpcClient.BlockByNumber(ctx, bigNumber)
	if err != nil {
		return nil, BlockHeaderData{}, fmt.Errorf("failed to fetch block %d from node: %w", blockNumber, err)
	}
	if rawBlock == nil {
		return nil, BlockHeaderData{}, fmt.Errorf("received empty block pointer for height %d", blockNumber)
	}

	// Извлекаем реальные метаданные блока
	headerData := BlockHeaderData{
		Hash:       rawBlock.Hash().Hex(),
		ParentHash: rawBlock.ParentHash().Hex(),
		Time:       int64(rawBlock.Time()),
	}

	chainID, err := c.rpcClient.ChainID(ctx)
	if err != nil {
		return nil, BlockHeaderData{}, fmt.Errorf("failed to fetch chain ID: %w", err)
	}
	signer := types.LatestSignerForChainID(chainID)

	rawTxs := rawBlock.Transactions()
	normalizedTxs := make([]storage.Transaction, 0, len(rawTxs))

	for _, tx := range rawTxs {
		if tx == nil {
			continue
		}

		fromAddr, err := types.Sender(signer, tx)
		if err != nil {
			continue
		}

		var toAddrStr *string
		if tx.To() != nil {
			str := tx.To().Hex()
			toAddrStr = &str
		}

		normalizedTx := storage.Transaction{
			TxHash:      tx.Hash().Hex(),
			BlockNumber: int64(blockNumber),
			FromAddress: fromAddr.Hex(),
			ToAddress:   toAddrStr,
			Value:       tx.Value().String(),
			GasPrice:    tx.GasPrice().Int64(),
			GasLimit:    int64(tx.Gas()),
			Nonce:       int64(tx.Nonce()),
		}

		normalizedTxs = append(normalizedTxs, normalizedTx)
	}

	// Возвращаем и транзакции, и реальный заголовок
	return normalizedTxs, headerData, nil
}
