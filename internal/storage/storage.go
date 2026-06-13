package storage

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

// DB represents the database connection pool
type DB struct {
	*sql.DB
}

// NewConnect establishes a new connection pool to the PostgreSQL database
func NewConnect(dsn string) (*DB, error) {
	// Open connection using pgx driver
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool settings optimized for high-throughput ETL
	db.SetMaxOpenConns(25)                 // Limit maximum active connections
	db.SetMaxIdleConns(25)                 // Keep idle connections alive for reuse
	db.SetConnMaxLifetime(5 * time.Minute) // Recycle connections to prevent memory leaks

	// Verify the connection is alive
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{db}, nil
}

// SaveBlock inserts a single block metadata into the database
func (db *DB) SaveBlock(block Block) error {
	query := `
		INSERT INTO blocks (block_number, block_hash, parent_hash, block_timestamp)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (block_number) DO NOTHING;
	`

	_, err := db.Exec(query, block.BlockNumber, block.BlockHash, block.ParentHash, block.BlockTimestamp)
	if err != nil {
		return fmt.Errorf("failed to insert block %d: %w", block.BlockNumber, err)
	}

	return nil
}

// SaveTransactions inserts multiple transactions efficiently using a single database transaction
func (db *DB) SaveTransactions(txs []Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	// Begin a standard database transaction
	txCtx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin database transaction: %w", err)
	}

	// Explicitly ignore Rollback error in defer, because Rollback will naturally
	// fail with an error if txCtx.Commit() succeeds below. This is expected behavior.
	defer func() {
		_ = txCtx.Rollback()
	}()

	query := `
		INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, gas_price, gas_limit, nonce)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tx_hash) DO NOTHING;
	`

	// Prepare the statement inside the transaction for maximum reuse speed
	stmt, err := txCtx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	// Explicitly ignore Close error since we cannot do anything useful if closing a statement fails here
	defer func() {
		_ = stmt.Close()
	}()

	// Execute inserts in a clean loop, but inside the same isolated transaction
	for _, tx := range txs {
		_, err = stmt.Exec(tx.TxHash, tx.BlockNumber, tx.FromAddress, tx.ToAddress, tx.Value, tx.GasPrice, tx.GasLimit, tx.Nonce)
		if err != nil {
			return fmt.Errorf("failed to execute prepared statement for tx %s: %w", tx.TxHash, err)
		}
	}

	// Commit all changes to disk at once
	if err := txCtx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
