package storage

import (
	"time"
)

// Block represents the 'blocks' table row structure
type Block struct {
	BlockNumber    int64
	BlockHash      string
	ParentHash     string
	BlockTimestamp time.Time
}

// Transaction represents the 'transactions' table row structure
type Transaction struct {
	TxHash      string
	BlockNumber int64
	FromAddress string
	ToAddress   *string // Using pointer because ToAddress can be NULL for contract deployment
	Value       string  // Stored as string to safely pass big numeric value to PostgreSQL NUMERIC
	GasPrice    int64
	GasLimit    int64
	Nonce       int64
}
