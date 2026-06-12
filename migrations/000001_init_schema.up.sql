-- SQL Migration: Initialize Blocks and Transactions tables

-- Table for storing Ethereum blocks metadata
CREATE TABLE IF NOT EXISTS blocks (
    block_number BIGINT PRIMARY KEY,
    block_hash VARCHAR(66) NOT NULL UNIQUE,
    parent_hash VARCHAR(66) NOT NULL,
    block_timestamp TIMESTAMP NOT NULL
    );

-- Table for storing Ethereum transactions
CREATE TABLE IF NOT EXISTS transactions (
    tx_hash VARCHAR(66) PRIMARY KEY,
    block_number BIGINT NOT NULL REFERENCES blocks(block_number) ON DELETE CASCADE,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42), -- Can be NULL if it is a contract creation deployment
    value NUMERIC(78,0) NOT NULL,
    gas_price BIGINT NOT NULL,
    gas_limit BIGINT NOT NULL,
    nonce BIGINT NOT NULL
    );

-- Database Indexes optimized for common Analytical and ETL queries
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON transactions(block_number);
CREATE INDEX IF NOT EXISTS idx_transactions_from_address ON transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address ON transactions(to_address);