# Set Go proxy (optional override)
export GOPROXY ?= https://proxy.golang.org,direct

.PHONY: all deps fmt lint clean test

all:
	@echo "Usage: make [deps | fmt | lint | clean | test]"

deps:
	@echo "[deps] Tidying and vendoring Go module dependencies..."
	go mod tidy
	go mod vendor

fmt:
	@echo "[fmt] Formatting Go code..."
	go fmt ./...

lint: deps
	@echo "[lint] Running golangci-lint$(if $(FIX), with --fix)..."
	golangci-lint run $(if $(FIX),--fix) ./...

clean:
	@echo "[clean] Cleaning build artifacts..."
	go clean -v -r ./...

test: deps
	@echo "[test] Running Go tests with race and coverage checks..."
	go clean -testcache
	go test -race -cover -covermode=atomic ./...
