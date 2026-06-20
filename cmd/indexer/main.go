package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorgohub/eth-indexer/internal/blockchain"
	"github.com/gorgohub/eth-indexer/internal/config"
	"github.com/gorgohub/eth-indexer/internal/storage"
	"github.com/gorgohub/eth-indexer/internal/sync"
)

func main() {
	log.Println("Starting Web3 Indexer service...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Config initialization failed: %v", err)
	}

	db, err := storage.NewConnect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}

	ethClient, err := blockchain.NewClient(cfg.EthRPCURL)
	if err != nil {
		log.Fatalf("Blockchain client initialization failed: %v", err)
	}

	// НАЧАЛО БЛОКА GRACEFUL SHUTDOWN
	// Создаем контекст, который автоматически перейдет в статус Done при нажатии Ctrl+C (SIGINT) или остановке (SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	syncer := sync.NewSyncer(db, ethClient)

	log.Println("ETL Worker engine successfully armed. Entering infinite processing loop...")

	// Передаем наш контролируемый контекст в синхронизатор
	if err := syncer.Start(ctx); err != nil {
		// Обычная отмена контекста по сигналу — это не авария, а штатный выход
		if errors.Is(err, context.Canceled) {
			log.Println("Indexer service stopped cleanly via graceful shutdown.")
		} else {
			log.Printf("Critical sync loop failure: %v", err)
		}
	}

	// Мягко закрываем ресурсы после остановки цикла
	log.Println("Closing underlying network connections and DB pools...")
	ethClient.Close()

	if err := db.Close(); err != nil {
		log.Printf("Failed to cleanly close database connection pool: %v", err)
	}

	log.Println("Shutdown complete. Bye!")
}
