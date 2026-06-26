package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorgohub/eth-indexer/internal/api"
	"github.com/gorgohub/eth-indexer/internal/blockchain"
	"github.com/gorgohub/eth-indexer/internal/config"
	"github.com/gorgohub/eth-indexer/internal/storage"
	"github.com/gorgohub/eth-indexer/internal/sync"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Initialize structured JSON logging globally
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Starting Web3 Indexer service...")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Config initialization failed", slog.Any("error", err))
		os.Exit(1)
	}

	db, err := storage.NewConnect(cfg.DatabaseURL)
	if err != nil {
		slog.Error("Database initialization failed", slog.Any("error", err))
		os.Exit(1)
	}

	// Updated: passing cfg.RPS for the rate limiter
	ethClient, err := blockchain.NewClient(cfg.EthRPCURL, cfg.RPS)
	if err != nil {
		slog.Error("Blockchain client initialization failed", slog.Any("error", err))
		os.Exit(1)
	}

	// Setup graceful shutdown handling for SIGINT (Ctrl+C) and SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start Prometheus metrics server on a dedicated port
	go func() {
		slog.Info("Prometheus metrics server listening on :2112")
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Critical metrics HTTP server failure", slog.Any("error", err))
		}
	}()

	// Initialize the HTTP API server
	apiServer := api.NewServer(db)

	// Launch the HTTP server asynchronously in a goroutine to prevent blocking the main execution flow
	go func() {
		if err := apiServer.Start(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Critical API HTTP server failure", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	// Updated: passing cfg.WorkerCount for the streaming worker pool
	syncer := sync.NewSyncer(db, ethClient, cfg.WorkerCount)

	slog.Info("ETL Worker engine successfully armed. Entering infinite processing loop...")

	if err := syncer.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("Indexer service stopped cleanly via graceful shutdown.")
		} else {
			slog.Error("Critical sync loop failure", slog.Any("error", err))
		}
	}

	// Gracefully release active network resources and connection pools after loop termination
	slog.Info("Closing underlying network connections and DB pools...")
	ethClient.Close()

	if err := db.Close(); err != nil {
		slog.Error("Failed to cleanly close database connection pool", slog.Any("error", err))
	}

	slog.Info("Shutdown complete. Bye!")
}
