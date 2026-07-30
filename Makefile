.PHONY: proto-gen proto-lint build test test-race work-sync tidy

# Generate protobuf code
proto-gen:
	cd proto && PATH="/home/amir/go/bin:$$PATH" buf generate

# Lint protobuf files
proto-lint:
	cd proto && buf lint

# Build all services
build:
	cd services/account && go build ./cmd/server/...
	cd services/ledger && go build ./cmd/server/...
	cd services/payment && go build ./cmd/server/...

# Run go mod tidy
tidy:
	cd services/account && go mod tidy
	cd services/api-gateway && go mod tidy
	cd services/auth && go mod tidy
	cd services/ledger && go mod tidy
	cd services/payment && go mod tidy
	cd shared && go mod tidy

# Run all tests (unit + integration)
test-all:
	cd shared && go test ./... -v -race -count=1
	cd services/account && go test ./... -v -race -count=1
	cd services/ledger && go test ./... -v -race -count=1
	cd services/payment && go test ./... -v -race -count=1

# Run shared package tests
test:
	cd shared && go test ./... -v

# Run tests with race detector
test-race:
	cd shared && go test ./... -v -race -count=1

# Sync workspace
work-sync:
	go work sync
