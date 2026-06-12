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
