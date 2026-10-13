.PHONY: generate-domain help build build-api build-worker build-codegen build-random-producer test test-unit test-examples test-integration lint vet fmt check-fmt generate clean run-api run-worker run-all docker-build docker-up docker-down docker-logs migrate-up deps tools verify-generated ci release

# Default target
help:
	@echo "FlowRulz - Event-driven Rules Engine"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Build targets:"
	@echo "  build           Build all binaries"
	@echo "  build-api       Build API server binary"
	@echo "  build-worker    Build worker binary"
	@echo "  build-codegen   Build codegen binary"
	@echo "  build-random-producer Build opt-in random producer example"
	@echo ""
	@echo "Test targets:"
	@echo "  test            Run all tests"
	@echo "  test-unit       Run internal unit tests"
	@echo "  test-examples   Run random-producer scenario tests"
	@echo "  test-integration Run integration tests"
	@echo ""
	@echo "Code quality:"
	@echo "  lint            Run golangci-lint"
	@echo "  vet             Run go vet"
	@echo "  fmt             Format code with gofmt"
	@echo ""
	@echo "Code generation:"
	@echo "  generate        Generate artifacts for a registered contract (set CONTRACT and CONTRACT_VERSION)"
	@echo ""
	@echo "Run targets:"
	@echo "  run-api         Run API server"
	@echo "  run-worker      Run worker"
	@echo "  run-all         Run API, worker, and dependencies via docker-compose"
	@echo ""
	@echo "Docker targets:"
	@echo "  docker-build    Build Docker images"
	@echo "  docker-up       Start all services with docker-compose"
	@echo "  docker-down     Stop all services"
	@echo "  docker-logs     View docker-compose logs"
	@echo ""
	@echo "Database:"
	@echo "  migrate-up      Run database migrations"
	@echo ""
	@echo "Utilities:"
	@echo "  clean           Clean build artifacts"
	@echo "  deps            Download and verify dependencies"
	@echo "  tools           Install development tools"

# Build variables
BIN_DIR := bin
API_BINARY := $(BIN_DIR)/api
WORKER_BINARY := $(BIN_DIR)/worker
CODEGEN_BINARY := $(BIN_DIR)/codegen
RANDOM_PRODUCER_BINARY := $(BIN_DIR)/random-producer
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"

# Go variables
GO := go
GOFLAGS := -trimpath
GOTESTFLAGS := -race -count=1

# Build all binaries
build: build-api build-worker build-codegen

build-api:
	@echo "Building API server..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(API_BINARY) ./cmd/api

build-worker:
	@echo "Building worker..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(WORKER_BINARY) ./cmd/worker

build-codegen:
	@echo "Building codegen..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(CODEGEN_BINARY) ./cmd/codegen

# Optional developer utility; intentionally excluded from the default build.
build-random-producer:
	@echo "Building opt-in random producer example..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -o $(RANDOM_PRODUCER_BINARY) ./examples/random-producer

# Run targets
run-api: build-api
	@echo "Starting API server..."
	$(API_BINARY)

run-worker: build-worker
	@echo "Starting worker..."
	$(WORKER_BINARY)

# Run with docker-compose
run-all: docker-up
	@echo "Services started. HTTP API at http://localhost:8080"
	@echo "Run 'make docker-logs' to view logs"
	@echo "Run 'make docker-down' to stop"

# Test targets
test: test-unit test-examples test-integration

test-unit:
	@echo "Running unit tests..."
	$(GO) test $(GOTESTFLAGS) ./internal/...

test-examples:
	@echo "Running example scenario tests..."
	$(GO) test $(GOTESTFLAGS) ./examples/random-producer/...

test-integration:
	@echo "Running integration tests..."
	$(GO) test $(GOTESTFLAGS) -tags=integration ./tests/...

# Code quality
lint:
	@echo "Running golangci-lint..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed. Run 'make tools'" && exit 1)
	golangci-lint run ./...

vet:
	@echo "Running go vet..."
	$(GO) vet ./...

fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

# Code generation
CONTRACT ?=
CONTRACT_VERSION ?=
TARGET ?= go
OUTPUT_DIR ?= /tmp/flow-rulz-generated

generate:
	@test -n "$(CONTRACT)" || (echo "Set CONTRACT to a registered contract name" && exit 2)
	@test -n "$(CONTRACT_VERSION)" || (echo "Set CONTRACT_VERSION to a registered contract version" && exit 2)
	@echo "Generating $(CONTRACT)@$(CONTRACT_VERSION) as $(TARGET)..."
	$(GO) run ./cmd/codegen -contract "$(CONTRACT)" -version "$(CONTRACT_VERSION)" -target "$(TARGET)" -output "$(OUTPUT_DIR)"

# Generate typed Go code for every stored contract of a domain into domains/<domain>/builds/go (DOMAIN=banking|ecommerce|food-delivery)
generate-domain:
	@test -n "$(DOMAIN)" || (echo "Set DOMAIN" && exit 2)
	./scripts/generate-domain.sh $(DOMAIN)

# Docker targets
docker-build:
	@echo "Building Docker images..."
	docker-compose build

docker-up:
	@echo "Starting services..."
	docker-compose up -d

docker-down:
	@echo "Stopping services..."
	docker-compose down

docker-logs:
	docker-compose logs -f

# Database migrations
migrate-up:
	@echo "Running migrations..."
	$(GO) run ./cmd/migrate up

# Utilities
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BIN_DIR)
	rm -rf /tmp/flow-rulz-generated

deps:
	@echo "Downloading dependencies..."
	$(GO) mod download
	$(GO) mod verify

tools:
	@echo "Installing development tools..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(GO) install github.com/pressly/goose/v3/cmd/goose@latest

# Generate example contracts
# Verify generated code compiles
verify-generated:
	@echo "Verifying generated code compiles..."
	@for dir in domains/*/builds/go; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Checking $$dir..."; \
			(cd $$dir && go build ./...) || exit 1; \
		fi; \
	done

# CI pipeline
check-fmt:
	@files="$$(gofmt -l $$(find cmd internal tests examples/random-producer -name '*.go' -type f))"; \
	if [ -n "$$files" ]; then printf "%s\n" "$$files"; gofmt -d $$files; exit 1; fi

ci: deps check-fmt vet lint test verify-generated
	@echo "CI pipeline passed!"

# Release
release: ci build docker-build
	@echo "Release artifacts ready in $(BIN_DIR)/"