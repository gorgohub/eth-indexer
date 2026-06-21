package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorgohub/eth-indexer/internal/storage"
	"github.com/jackc/pgx/v5"
)

// Server encapsulates the HTTP routing logic and dependencies
type Server struct {
	router *chi.Mux
	db     *storage.DB
}

// NewServer initializes and configures a clean API server
func NewServer(db *storage.DB) *Server {
	r := chi.NewRouter()

	// Standard middlewares for request logging and panic recovery
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	s := &Server{
		router: r,
		db:     db,
	}

	s.routes()

	return s
}

// Start launches the HTTP listener on the given address
func (s *Server) Start(addr string) error {
	log.Printf("HTTP API Server listening on %s", addr)
	return http.ListenAndServe(addr, s.router)
}

// routes registers API endpoints
func (s *Server) routes() {
	s.router.Route("/api/v1", func(r chi.Router) {
		r.Get("/block/{number}", s.handleGetBlock)
		r.Get("/address/{address}/transactions", s.handleGetTransactions)
	})
}

// handleGetBlock fetches block metadata: GET /api/v1/block/{number}
func (s *Server) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	numberStr := chi.URLParam(r, "number")
	number, err := strconv.ParseInt(numberStr, 10, 64)
	if err != nil {
		s.respondWithError(w, http.StatusBadRequest, "Invalid block number format")
		return
	}

	block, err := s.db.GetBlockByNumber(r.Context(), number)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.respondWithError(w, http.StatusNotFound, "Block not found in local database")
			return
		}
		log.Printf("API error: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	s.respondWithJSON(w, http.StatusOK, block)
}

// handleGetTransactions fetches txs for an account: GET /api/v1/address/{address}/transactions
func (s *Server) handleGetTransactions(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if address == "" {
		s.respondWithError(w, http.StatusBadRequest, "Missing address parameter")
		return
	}

	txs, err := s.db.GetTransactionsByAddress(r.Context(), address)
	if err != nil {
		log.Printf("API error: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	// Ensure we return an empty array instead of null if no transactions found
	if txs == nil {
		txs = []storage.Transaction{}
	}

	s.respondWithJSON(w, http.StatusOK, txs)
}

// respondWithJSON helper writes a standardized JSON response
func (s *Server) respondWithJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("Failed to write JSON response: %v", err)
	}
}

// respondWithError helper returns a standardized JSON error message
func (s *Server) respondWithError(w http.ResponseWriter, status int, message string) {
	s.respondWithJSON(w, status, map[string]string{"error": message})
}
