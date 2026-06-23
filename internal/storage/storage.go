package storage

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps the modern pgx connection pool
type DB struct {
	pool *pgxpool.Pool
}

// NewConnect initializes a high-performance concurrent connection pool to PostgreSQL
func NewConnect(dsn string) (*DB, error) {
	// Parse configurations from the DSN string
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to parse DSN string: %w", err)
	}

	// Optimize connection parameters for an infrastructure ETL pipeline
	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnIdleTime = 30 * time.Minute

	// Establish the connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Immediate validation ping
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{pool: pool}, nil
}

// Close cleanly shuts down all active network connections in the pool
func (db *DB) Close() error {
	db.pool.Close()
	return nil
}

// GetLatestSavedBlock returns the highest block number present in the database.
// Uses pgx native signatures where context is mandatory.
func (db *DB) GetLatestSavedBlock() (int64, error) {
	query := `SELECT COALESCE(MAX(block_number), 0) FROM blocks;`

	var lastBlock int64
	// pgx.QueryRow strictly demands context as the first argument
	err := db.pool.QueryRow(context.Background(), query).Scan(&lastBlock)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch max block number: %w", err)
	}

	return lastBlock, nil
}

// SaveBlock inserts a single block header record into the database
func (db *DB) SaveBlock(number int64, hash string, parentHash string, timestamp int64) error {
	query := `
		INSERT INTO blocks (block_number, block_hash, parent_hash, block_timestamp)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (block_number) DO NOTHING;
	`
	_, err := db.pool.Exec(context.Background(), query, number, hash, parentHash, timestamp)
	if err != nil {
		return fmt.Errorf("failed to save block headers: %w", err)
	}
	return nil
}

// SaveTransactions inserts multiple transactions and their internal token transfers inside a strict database transaction.
func (db *DB) SaveTransactions(txs []Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	txCtx, err := db.pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin database transaction: %w", err)
	}

	defer func() {
		_ = txCtx.Rollback(context.Background())
	}()

	txQuery := `
		INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, gas_price, gas_limit, nonce)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tx_hash) DO NOTHING;
	`

	tokenQuery := `
		INSERT INTO token_transfers (tx_hash, block_number, contract_address, from_address, to_address, value)
		VALUES ($1, $2, $3, $4, $5, $6);
	`

	for _, item := range txs {
		_, err = txCtx.Exec(context.Background(), txQuery,
			item.TxHash, item.BlockNumber, item.FromAddress, item.ToAddress, item.Value, item.GasPrice, item.GasLimit, item.Nonce,
		)
		if err != nil {
			return fmt.Errorf("failed to execute transaction query: %w", err)
		}

		for _, tokenMove := range item.TokenMoves {
			_, err = txCtx.Exec(context.Background(), tokenQuery,
				tokenMove.TxHash, tokenMove.BlockNumber, tokenMove.ContractAddress,
				tokenMove.FromAddress, tokenMove.ToAddress, tokenMove.Value,
			)
			if err != nil {
				return fmt.Errorf("failed to execute token transfer query: %w", err)
			}

			// Вот эта строчка покажет вам результат прямо в консоли при работе!
			log.Printf("[ERC-20] Found token transfer: %s tokens from %s to %s", tokenMove.Value, tokenMove.FromAddress, tokenMove.ToAddress)
		}
	}

	if err = txCtx.Commit(context.Background()); err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}

	return nil
}

// GetBlockByNumber fetches metadata for a specific block from the database.
func (db *DB) GetBlockByNumber(ctx context.Context, number int64) (*Block, error) {
	query := `
		SELECT block_number, block_hash, parent_hash, block_timestamp 
		FROM blocks 
		WHERE block_number = $1;
	`

	var b Block
	err := db.pool.QueryRow(ctx, query, number).Scan(
		&b.BlockNumber, &b.BlockHash, &b.ParentHash, &b.BlockTimestamp,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block %d: %w", number, err)
	}

	return &b, nil
}

// GetTransactionsByAddress fetches last 100 transactions related to an address (from or to).
func (db *DB) GetTransactionsByAddress(ctx context.Context, address string) ([]Transaction, error) {
	query := `
		SELECT tx_hash, block_number, from_address, to_address, value, gas_price, gas_limit, nonce
		FROM transactions
		WHERE from_address = $1 OR to_address = $1
		ORDER BY block_number DESC
		LIMIT 100;
	`

	rows, err := db.pool.Query(ctx, query, address)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions for address %s: %w", address, err)
	}
	defer rows.Close()

	var txs []Transaction
	for rows.Next() {
		var tx Transaction
		err = rows.Scan(
			&tx.TxHash, &tx.BlockNumber, &tx.FromAddress, &tx.ToAddress,
			&tx.Value, &tx.GasPrice, &tx.GasLimit, &tx.Nonce,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction row: %w", err)
		}
		txs = append(txs, tx)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return txs, nil
}

// GetTokenTransfersByAddress fetches the last 100 ERC-20 token transfers for a specific address.
func (db *DB) GetTokenTransfersByAddress(ctx context.Context, address string) ([]TokenTransfer, error) {
	query := `
		SELECT tx_hash, block_number, contract_address, from_address, to_address, value::TEXT
		FROM token_transfers
		WHERE from_address = $1 OR to_address = $1
		ORDER BY block_number DESC
		LIMIT 100;
	`

	rows, err := db.pool.Query(ctx, query, address)
	if err != nil {
		return nil, fmt.Errorf("failed to query token transfers for address %s: %w", address, err)
	}
	defer rows.Close()

	var transfers []TokenTransfer
	for rows.Next() {
		var t TokenTransfer
		err = rows.Scan(&t.TxHash, &t.BlockNumber, &t.ContractAddress, &t.FromAddress, &t.ToAddress, &t.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to scan token transfer row: %w", err)
		}
		transfers = append(transfers, t)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("token transfers rows iteration error: %w", err)
	}

	return transfers, nil
}
