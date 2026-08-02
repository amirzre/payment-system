.PHONY: help
.PHONY: proto-gen proto-lint build lint fmt test test-race test-all tidy work-sync clean
.PHONY: docker-build docker-up docker-down docker-down-volumes
.PHONY: migrate-up migrate-down generate-keys
.PHONY: ci ci-account ci-api-gateway ci-auth ci-ledger ci-payment ci-shared

# Default target

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	     /^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Protobuf

proto-gen: ## Generate gRPC/protobuf code from .proto files
	cd proto && buf generate

proto-lint: ## Lint .proto files
	cd proto && buf lint

# Build

BUILD_DIR := bin

build: ## Compile all services to bin/
	@mkdir -p $(BUILD_DIR)
	cd services/account     && go build -o ../../$(BUILD_DIR)/account-service  ./cmd/server/
	cd services/api-gateway && go build -o ../../$(BUILD_DIR)/api-gateway      ./cmd/server/
	cd services/auth        && go build -o ../../$(BUILD_DIR)/auth-service     ./cmd/server/
	cd services/ledger      && go build -o ../../$(BUILD_DIR)/ledger-service   ./cmd/server/
	cd services/payment     && go build -o ../../$(BUILD_DIR)/payment-service  ./cmd/server/

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR)

# Code quality

lint: ## Run go vet on all modules
	go vet ./shared/...
	cd services/account     && go vet ./...
	cd services/api-gateway && go vet ./...
	cd services/auth        && go vet ./...
	cd services/ledger      && go vet ./...
	cd services/payment     && go vet ./...

fmt: ## Format all Go code
	cd shared              && gofmt -s -w .
	cd gen/go              && gofmt -s -w .
	cd services/account    && gofmt -s -w .
	cd services/api-gateway&& gofmt -s -w .
	cd services/auth       && gofmt -s -w .
	cd services/ledger     && gofmt -s -w .
	cd services/payment    && gofmt -s -w .

# Tests

test: ## Run all tests
	cd shared              && go test ./... -v -count=1
	cd services/account    && go test ./... -v -count=1
	cd services/api-gateway&& go test ./... -v -count=1
	cd services/auth       && go test ./... -v -count=1
	cd services/ledger     && go test ./... -v -count=1
	cd services/payment    && go test ./... -v -count=1

test-race: ## Run all tests with race detector
	cd shared              && go test ./... -v -race -count=1
	cd services/account    && go test ./... -v -race -count=1
	cd services/api-gateway&& go test ./... -v -race -count=1
	cd services/auth       && go test ./... -v -race -count=1
	cd services/ledger     && go test ./... -v -race -count=1
	cd services/payment    && go test ./... -v -race -count=1

test-all: test-race ## Alias for test-race

# Dependencies

tidy: ## Tidy all Go module dependencies
	cd shared              && go mod tidy
	cd services/account    && go mod tidy
	cd services/api-gateway&& go mod tidy
	cd services/auth       && go mod tidy
	cd services/ledger     && go mod tidy
	cd services/payment    && go mod tidy

work-sync: ## Sync Go workspace
	go work sync

# Docker

docker-build: ## Build all Docker images
	docker compose build

docker-up: ## Start all services with Docker Compose
	docker compose up -d

docker-down: ## Stop all services
	docker compose down

docker-down-volumes: ## Stop all services and destroy volumes (data loss)
	docker compose down -v

# Database migrations (local dev)

MIGRATE := go run -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.18.2

migrate-up: ## Apply all database migrations (set DATABASE_URL_* vars)
	$(MIGRATE) -path services/account/migrations -database "$(DATABASE_URL_ACCOUNT)" up
	$(MIGRATE) -path services/auth/migrations    -database "$(DATABASE_URL_AUTH)"    up
	$(MIGRATE) -path services/ledger/migrations  -database "$(DATABASE_URL_LEDGER)"  up
	$(MIGRATE) -path services/payment/migrations -database "$(DATABASE_URL_PAYMENT)" up

migrate-down: ## Roll back all database migrations (set DATABASE_URL_* vars)
	$(MIGRATE) -path services/payment/migrations -database "$(DATABASE_URL_PAYMENT)" down -all
	$(MIGRATE) -path services/ledger/migrations  -database "$(DATABASE_URL_LEDGER)"  down -all
	$(MIGRATE) -path services/auth/migrations    -database "$(DATABASE_URL_AUTH)"    down -all
	$(MIGRATE) -path services/account/migrations -database "$(DATABASE_URL_ACCOUNT)" down -all

# Auth keys

generate-keys: ## Generate RSA key pair for JWT signing
	./scripts/generate-keys.sh

# CI targets (run locally, same steps as GitHub Actions)

ci: ci-shared ci-account ci-api-gateway ci-auth ci-ledger ci-payment ## Run full CI pipeline locally

ci-account: ## CI: lint → test → docker build (account)
	(cd shared && go mod download && go vet ./...)
	(cd gen/go && go mod download && go vet ./...)
	(cd services/account && go mod download && go vet ./...)
	(cd shared && go test ./... -race -count=1)
	(cd services/account && go test ./... -race -count=1)
	docker build -f services/account/Dockerfile -t payment-system/account:ci .

ci-api-gateway: ## CI: lint → test → docker build (api-gateway)
	(cd shared && go mod download && go vet ./...)
	(cd gen/go && go mod download && go vet ./...)
	(cd services/api-gateway && go mod download && go vet ./...)
	(cd shared && go test ./... -race -count=1)
	(cd services/api-gateway && go test ./... -race -count=1)
	docker build -f services/api-gateway/Dockerfile -t payment-system/api-gateway:ci .

ci-auth: ## CI: lint → test → docker build (auth)
	(cd shared && go mod download && go vet ./...)
	(cd gen/go && go mod download && go vet ./...)
	(cd services/auth && go mod download && go vet ./...)
	(cd shared && go test ./... -race -count=1)
	(cd services/auth && go test ./... -race -count=1)
	docker build -f services/auth/Dockerfile -t payment-system/auth:ci .

ci-ledger: ## CI: lint → test → docker build (ledger)
	(cd shared && go mod download && go vet ./...)
	(cd gen/go && go mod download && go vet ./...)
	(cd services/ledger && go mod download && go vet ./...)
	(cd shared && go test ./... -race -count=1)
	(cd services/ledger && go test ./... -race -count=1)
	docker build -f services/ledger/Dockerfile -t payment-system/ledger:ci .

ci-payment: ## CI: lint → test → docker build (payment)
	(cd shared && go mod download && go vet ./...)
	(cd gen/go && go mod download && go vet ./...)
	(cd services/payment && go mod download && go vet ./...)
	(cd shared && go test ./... -race -count=1)
	(cd services/payment && go test ./... -race -count=1)
	docker build -f services/payment/Dockerfile -t payment-system/payment:ci .

ci-shared: ## CI: lint → test (shared)
	(cd shared && go mod download && go vet ./...)
	(cd shared && go test ./... -race -count=1)
