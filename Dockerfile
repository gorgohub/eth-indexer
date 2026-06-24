# Stage 1: Build the Go binary using a specific version
FROM golang:1.26.4-alpine AS builder

WORKDIR /app

# Cache dependencies by copying mod files first
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build a statically linked binary targeting Linux architecture
RUN CGO_ENABLED=0 GOOS=linux go build -o eth-indexer ./cmd/indexer/main.go

# Stage 2: Create a minimal lightweight runtime image
FROM alpine:3.19

WORKDIR /app

# Install root CA certificates required for secure HTTPS requests to RPC nodes
RUN apk --no-cache add ca-certificates

# Copy the compiled binary and database migrations from the builder stage
COPY --from=builder /app/eth-indexer .
COPY --from=builder /app/migrations ./migrations

# Expose the port used by the internal HTTP API server
EXPOSE 8080

# Run the indexer service
CMD ["./eth-indexer"]