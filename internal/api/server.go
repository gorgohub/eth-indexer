package api

import (
	"encoding/json"
	"errors"
	"log/slog"
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
	slog.Info("HTTP API Server listening", slog.String("addr", addr))
	return http.ListenAndServe(addr, s.router)
}

// routes registers API endpoints
func (s *Server) routes() {
	s.router.Route("/api/v1", func(r chi.Router) {
		r.Get("/block/{number}", s.handleGetBlock)
		r.Get("/address/{address}/transactions", s.handleGetTransactions)
		r.Get("/address/{address}/tokens", s.handleGetTokenTransfers)
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
		slog.Error("API error fetching block", slog.Int64("block_number", number), slog.Any("error", err))
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
	s.respondWithSlice(w, txs, err)
}

// handleGetTokenTransfers fetches ERC-20 transfers for an account: GET /api/v1/address/{address}/tokens
func (s *Server) handleGetTokenTransfers(w http.ResponseWriter, r *http.Request) {
	address := chi.URLParam(r, "address")
	if address == "" {
		s.respondWithError(w, http.StatusBadRequest, "Missing address parameter")
		return
	}

	transfers, err := s.db.GetTokenTransfersByAddress(r.Context(), address)
	s.respondWithSlice(w, transfers, err)
}

// respondWithSlice helper processes query errors and forces empty json array instead of null
func (s *Server) respondWithSlice(w http.ResponseWriter, data interface{}, queryErr error) {
	if queryErr != nil {
		slog.Error("API slice query error", slog.Any("error", queryErr))
		s.respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	// If the specific slice pointer is nil, output an empty json array structure
	if data == nil {
		s.respondWithJSON(w, http.StatusOK, []string{})
		return
	}

	s.respondWithJSON(w, http.StatusOK, data)
}

// respondWithJSON helper writes a standardized JSON response
func (s *Server) respondWithJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("Failed to write JSON response", slog.Any("error", err))
	}
}

// respondWithError helper returns a standardized JSON error message
func (s *Server) respondWithError(w http.ResponseWriter, status int, message string) {
	s.respondWithJSON(w, status, map[string]string{"error": message})
}
