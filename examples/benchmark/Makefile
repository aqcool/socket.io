.DEFAULT_GOAL := help
SHELL := /bin/bash

MAKEFLAGS += --no-print-directory
export GOPROXY := https://proxy.golang.org,direct
TEST_TIMEOUT   := 60s

C_RESET  := \033[0m
C_CYAN   := \033[36m
C_GREEN  := \033[32m
C_RED    := \033[31m
C_YELLOW := \033[33m

.PHONY: all help env deps get update build fmt vet lint clean test

all: help

help:
	@printf "\n"
	@printf "$(C_GREEN)Project Makefile Interface$(C_RESET)\n"
	@printf "\n"
	@printf "$(C_YELLOW)Usage:$(C_RESET) make [command]\n"
	@printf "\n"
	@printf "$(C_CYAN)Commands:$(C_RESET)\n"
	@printf "  deps        Run 'go mod tidy' & 'go work sync/vendor'\n"
	@printf "  get         Run 'go get ./...'\n"
	@printf "  update      Run 'go get -u -v' and refresh deps\n"
	@printf "  build       Build module\n"
	@printf "  fmt         Format code (go fmt)\n"
	@printf "  vet         Run go vet\n"
	@printf "  lint        Run golangci-lint (use FIX=1 to --fix)\n"
	@printf "  clean       Clean build cache\n"
	@printf "  test        Run tests with race detection\n"
	@printf "\n"

env:
	@go env

deps:
	@printf "$(C_CYAN)[Deps 1/3] Processing 'go mod tidy'...$(C_RESET)\n"
	@go mod tidy
	@if [ -f "go.work" ]; then \
		printf "$(C_CYAN)[Deps 2/3] Processing 'go work sync'...$(C_RESET)\n"; \
		go work sync; \
		printf "$(C_CYAN)[Deps 3/3] Processing 'go work vendor'...$(C_RESET)\n"; \
		go work vendor; \
	else \
		printf "$(C_CYAN)[Deps 2/2] Processing 'go mod vendor'...$(C_RESET)\n"; \
		go mod vendor; \
	fi

get:
	@printf "$(C_CYAN)[Get] Processing: .$(C_RESET)\n"
	@go get ./...

update:
	@printf "$(C_CYAN)[Update] Processing: .$(C_RESET)\n"
	@go get -u -v ./...
	@$(MAKE) deps

build:
	@printf "$(C_CYAN)[Build] Processing: .$(C_RESET)\n"
	@go build ./...

fmt:
	@printf "$(C_CYAN)[Fmt] Processing: .$(C_RESET)\n"
	@go fmt ./...

vet: deps
	@printf "$(C_CYAN)[Vet] Processing: .$(C_RESET)\n"
	@go vet ./...

lint: deps
	@command -v golangci-lint >/dev/null 2>&1 || { printf "$(C_RED)[Error] golangci-lint is not installed.$(C_RESET)\n"; exit 1; }
	@printf "$(C_CYAN)[Lint] Processing: .$(C_RESET)\n"
	@golangci-lint run $(if $(FIX),--fix) ./...

clean:
	@printf "$(C_CYAN)[Clean] Processing: .$(C_RESET)\n"
	@go clean -mod=mod -v -r ./...

test: deps
	@printf "$(C_CYAN)[Test] Cleaning test cache...$(C_RESET)\n"
	@go clean -testcache
	@printf "$(C_CYAN)[Test] Processing: .$(C_RESET)\n"
	@go test -timeout=$(TEST_TIMEOUT) -race -cover -covermode=atomic ./...
