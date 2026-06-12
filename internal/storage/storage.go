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
		db.Close()
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
