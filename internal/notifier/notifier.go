package notifier

import (
	"log"
	"math/big"

	"github.com/gorgohub/eth-indexer/internal/storage"
)

// Notifier handles business logic filters for transaction events
type Notifier struct {
	usdtThreshold *big.Int
}

// NewNotifier initializes a notification service with specific volume limits
func NewNotifier() *Notifier {
	// Set threshold to 100,000 USDT (100,000 + 6 decimals = 100000000000)
	threshold := new(big.Int)
	threshold.SetString("100000000000", 10)

	return &Notifier{
		usdtThreshold: threshold,
	}
}

// CheckAndNotify inspects token transfers for high-value whale movements
func (n *Notifier) CheckAndNotify(transfers []storage.TokenTransfer) {
	for _, transfer := range transfers {
		val := new(big.Int)
		val, ok := val.SetString(transfer.Value, 10)
		if !ok {
			continue
		}

		// If transfer value is greater than or equal to our threshold, fire a notification alert
		if val.Cmp(n.usdtThreshold) >= 0 {
			n.sendAlert(transfer)
		}
	}
}

// sendAlert simulates dispatching a structured alert to external systems (e.g., Telegram, Webhooks)
func (n *Notifier) sendAlert(t storage.TokenTransfer) {
	log.Printf("[WHALE ALERT] 🚨 Large token move detected on block %d!", t.BlockNumber)
	log.Printf("[WHALE ALERT] Tx: %s", t.TxHash)
	log.Printf("[WHALE ALERT] Contract: %s", t.ContractAddress)
	log.Printf("[WHALE ALERT] From: %s -> To: %s", t.FromAddress, t.ToAddress)
	log.Printf("[WHALE ALERT] Raw Value: %s units", t.Value)
}
