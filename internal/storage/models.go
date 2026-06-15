package storage

// Block represents the normalized Ethereum block header for PostgreSQL
type Block struct {
	BlockNumber    int64  `db:"block_number"`
	BlockHash      string `db:"block_hash"`
	ParentHash     string `db:"parent_hash"`
	BlockTimestamp int64  `db:"block_timestamp"` // Добавили поле времени
}

// Transaction represents the normalized Ethereum transaction schema for PostgreSQL
type Transaction struct {
	TxHash      string  `db:"tx_hash"`
	BlockNumber int64   `db:"block_number"`
	FromAddress string  `db:"from_address"`
	ToAddress   *string `db:"to_address"` // Can be nil if it's a contract creation tx
	Value       string  `db:"value"`      // Stored as string to safely handle uint256 Wei
	GasPrice    int64   `db:"gas_price"`
	GasLimit    int64   `db:"gas_limit"`
	Nonce       int64   `db:"nonce"`
}
