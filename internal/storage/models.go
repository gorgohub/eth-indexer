package storage

// Block represents the domain model for an Ethereum block header.
type Block struct {
	BlockNumber    int64  `db:"block_number" json:"block_number"`
	BlockHash      string `db:"block_hash" json:"block_hash"`
	ParentHash     string `db:"parent_hash" json:"parent_hash"`
	BlockTimestamp int64  `db:"block_timestamp" json:"block_timestamp"`
}

// Transaction represents the domain model for a single Ethereum transaction.
type Transaction struct {
	TxHash      string          `db:"tx_hash" json:"tx_hash"`
	BlockNumber int64           `db:"block_number" json:"block_number"`
	FromAddress string          `db:"from_address" json:"from_address"`
	ToAddress   *string         `db:"to_address" json:"to_address"`
	Value       string          `db:"value" json:"value"`
	GasPrice    int64           `db:"gas_price" json:"gas_price"`
	GasLimit    int64           `db:"gas_limit" json:"gas_limit"`
	Nonce       int64           `db:"nonce" json:"nonce"`
	TokenMoves  []TokenTransfer `json:"token_transfers,omitempty"` // Field for parsed internal ERC-20 transfers
}

// TokenTransfer represents an extracted internal ERC-20 token transfer payload.
type TokenTransfer struct {
	TxHash          string `db:"tx_hash" json:"tx_hash"`
	BlockNumber     int64  `db:"block_number" json:"block_number"`
	ContractAddress string `db:"contract_address" json:"contract_address"`
	FromAddress     string `db:"from_address" json:"from_address"`
	ToAddress       string `db:"to_address" json:"to_address"`
	Value           string `db:"value" json:"value"`
}
