CREATE TABLE IF NOT EXISTS token_transfers (
                                               id SERIAL PRIMARY KEY,
                                               tx_hash VARCHAR(66) NOT NULL REFERENCES transactions(tx_hash) ON DELETE CASCADE,
    block_number BIGINT NOT NULL,
    contract_address VARCHAR(42) NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    value NUMERIC NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_token_transfers_contract ON token_transfers(contract_address);
CREATE INDEX IF NOT EXISTS idx_token_transfers_to ON token_transfers(to_address);