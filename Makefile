# ============================================================================== 
#  GLOBAL CONFIGURATION
# ==============================================================================
.DEFAULT_GOAL := help
SHELL := /bin/bash

# Root
PROJECT_ROOT := $(CURDIR)

# Silence make output for a clean CLI experience
MAKEFLAGS += --no-print-directory

# Environment
export GOPROXY := https://proxy.golang.org,direct
TEST_TIMEOUT   ?= 120s

# Project Metadata
VERSION_FILE   := pkg/version/version.go
CORE_DEP       := github.com/aqcool/socket.io

# Modules Definition (The Domain)
#
# V3_MODULES remain on the existing lockstep v3 release train. Experimental
# major-version modules participate in development/build/test commands but must
# never inherit the v3 VERSION/tag automatically.
V3_MODULES := parsers/engine \
              parsers/socket \
              servers/engine \
              servers/socket \
              instrumentation \
              observability \
              reliability \
              sticky \
              typed \
              adapters/adapter \
              adapters/amqp \
              adapters/broker \
              adapters/kafka \
              adapters/nats \
              adapters/mongo \
              adapters/postgres \
              adapters/redis \
              adapters/unix \
              adapters/valkey \
              clients/engine \
              clients/socket

EXPERIMENTAL_MODULES := v4
MODULES := $(V3_MODULES) $(EXPERIMENTAL_MODULES)

# Scope Logic: If MODULE=... is passed, use it; otherwise Root (.) + All Modules
SCOPE := $(if $(MODULE),$(MODULE),. $(MODULES))

# ANSI Color Codes
C_RESET  := \033[0m
C_CYAN   := \033[36m
C_GREEN  := \033[32m
C_RED    := \033[31m
C_YELLOW := \033[33m

# ==============================================================================
#  MACROS (The Abstract Machines)
# ==============================================================================

# Macro: EXECUTE
# Safe iteration with Fail-Fast logic.
# $1: Label (Context)
# $2: Command (Action)
define EXECUTE
	@for dir in $(SCOPE); do \
		if [ -d "$$dir" ]; then \
			printf "$(C_CYAN)[%s] Processing: $$dir$(C_RESET)\n" "$1"; \
			(cd "$$dir" && $2) || { \
				printf "$(C_RED)[Error] Failed in $$dir (Exit Code: $$?)$(C_RESET)\n"; \
				exit 1; \
			}; \
		else \
			printf "$(C_YELLOW)[Warn] Skipped missing module: $$dir$(C_RESET)\n"; \
		fi; \
	done
endef

# Macro: VALIDATE_MODULE
# Check if MODULE is in the MODULES list or is root (.)
# Usage: $(call VALIDATE_MODULE)
define VALIDATE_MODULE
	@if [ "$(MODULE)" != "." ] && [ -z "$(filter $(MODULE),$(MODULES))" ]; then \
		printf "$(C_RED)[Error] Unknown module: $(MODULE). Must be one of: . $(MODULES)$(C_RESET)\n"; \
		exit 1; \
	fi
endef

# ==============================================================================
#  TARGETS (The Interfaces)
# ==============================================================================

.PHONY: all help env deps get update build fmt vet lint clean test version release

all: help

help:
	@printf "\n"
	@printf "$(C_GREEN)Project Makefile Interface$(C_RESET)\n"
	@printf "\n"
	@printf "$(C_YELLOW)Usage:$(C_RESET) make [command] [options]\n"
	@printf "\n"
	@printf "$(C_CYAN)Options:$(C_RESET)\n"
	@printf "  MODULE=path/to/dir   Run command on specific module only\n"
	@printf "  VERSION=vX.Y.Z       Required for 'version' command\n"
	@printf "  FORCE=1              Force overwrite tags in 'release' command\n"
	@printf "\n"
	@printf "$(C_CYAN)Commands:$(C_RESET)\n"
	@printf "  deps        Run 'go mod tidy' & 'go mod vendor'\n"
	@printf "  get         Run 'go get ./...'\n"
	@printf "  update      Run 'go get -u' and refresh deps\n"
	@printf "  build       Build all modules\n"
	@printf "  fmt         Format code (go fmt)\n"
	@printf "  vet         Run go vet\n"
	@printf "  lint        Run golangci-lint (use FIX=1 to auto-fix)\n"
	@printf "  clean       Clean build cache\n"
	@printf "  test        Run tests with race detection\n"
	@printf "  version     Update v3 version file and sync v3 submodules\n"
	@printf "  release     Create v3 release tags based on VERSION file\n"
	@printf "                Without MODULE: tags root + v3 modules\n"
	@printf "                With MODULE=path: tags only specified v3 module\n"
	@printf "\n"

env:
	@go env

deps:
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	$(call EXECUTE,Deps,go mod tidy && go mod vendor)

get:
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	$(call EXECUTE,Get,go get ./...)

update:
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	$(call EXECUTE,Update,go get -u -v ./...)
	@$(MAKE) deps

build:
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	$(call EXECUTE,Build,go build ./...)

fmt:
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	$(call EXECUTE,Fmt,go fmt ./...)

vet: deps
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	$(call EXECUTE,Vet,go vet ./...)

lint: deps
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	@command -v golangci-lint >/dev/null 2>&1 || { printf "$(C_RED)[Error] golangci-lint is not installed. See https://golangci-lint.run/welcome/install/$(C_RESET)\n"; exit 1; }
	$(call EXECUTE,Lint,golangci-lint run --timeout=5m --config=$(PROJECT_ROOT)/.golangci.yml $(if $(FIX),--fix) ./...)

clean:
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	$(call EXECUTE,Clean,go clean -v -r ./...)

test: deps
ifdef MODULE
	$(call VALIDATE_MODULE)
endif
	@printf "$(C_CYAN)[Test] Cleaning test cache...$(C_RESET)\n"
	@go clean -testcache
	$(call EXECUTE,Test,go test -run '^$$' ./... && test_packages="$$(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...)" && if [ -n "$$test_packages" ]; then go test -timeout=$(TEST_TIMEOUT) -race -cover -covermode=atomic $$test_packages; fi)

# ==============================================================================
#  SPECIAL OPERATIONS (High-Risk)
# ==============================================================================

version:
ifndef VERSION
	$(error $(C_RED)[Error] VERSION is required (e.g., make version VERSION=v3.1.0)$(C_RESET))
endif
	@# Validation (Strict Regex)
	@echo "$(VERSION)" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z\-\.]+)?$$' || \
		{ printf "$(C_RED)[Error] Invalid version format: $(VERSION)$(C_RESET)\n"; exit 1; }

	@# Update v3 Version File (Portable atomic write)
	@printf "$(C_CYAN)[Version] Updating $(VERSION_FILE) to $(VERSION)$(C_RESET)\n"
	@[ -f "$(VERSION_FILE)" ] || { printf "$(C_RED)[Error] File not found: $(VERSION_FILE)$(C_RESET)\n"; exit 1; }
	@sed 's/VERSION = ".*"/VERSION = "$(VERSION)"/' "$(VERSION_FILE)" > "$(VERSION_FILE).tmp" && \
		mv "$(VERSION_FILE).tmp" "$(VERSION_FILE)"

	@# Update Dependencies in v3 Submodules only
	@for mod in $(V3_MODULES); do \
		if [ -d "$$mod" ]; then \
			printf "$(C_CYAN)[Version] Syncing $$mod$(C_RESET)\n"; \
			(cd "$$mod" && \
				go mod tidy && \
				TARGETS=$$(go list -mod=mod -f '{{if and (not .Main)}}{{.Path}}@$(VERSION){{end}}' -m all | grep "^$(CORE_DEP)"); \
				if [ -n "$$TARGETS" ]; then \
					echo "$$TARGETS" | xargs go get -v; \
				fi; \
				go mod tidy) || exit 1; \
		fi; \
	done
	@printf "$(C_GREEN)[Version] Completed successfully. v3 submodules synced.$(C_RESET)\n"
	@$(MAKE) deps

release:
	@[ -f "$(VERSION_FILE)" ] || { printf "$(C_RED)[Error] Version file missing$(C_RESET)\n"; exit 1; }
	$(eval CUR_VER := $(shell awk -F'"' '/const VERSION/ {print $$2}' "$(VERSION_FILE)"))
	@[ -n "$(CUR_VER)" ] || { printf "$(C_RED)[Error] Could not read version from $(VERSION_FILE)$(C_RESET)\n"; exit 1; }

	$(eval TAG_OPTS := $(if $(filter 1,$(FORCE)),-f,))
ifdef MODULE
	@if [ "$(MODULE)" != "." ] && [ -z "$(filter $(MODULE),$(V3_MODULES))" ]; then \
		printf "$(C_RED)[Error] release only supports v3 modules: . $(V3_MODULES)$(C_RESET)\n"; \
		exit 1; \
	fi
	@[ -d "$(MODULE)" ] || { printf "$(C_RED)[Error] Module path not found: $(MODULE)$(C_RESET)\n"; exit 1; }
	@printf "$(C_CYAN)[Release] Tagging module: $(MODULE)/$(CUR_VER) (Force: $(FORCE))$(C_RESET)\n"
	@git tag $(TAG_OPTS) "$(MODULE)/$(CUR_VER)" || exit 1
	@git show "$(MODULE)/$(CUR_VER)" >/dev/null 2>&1 || exit 1
	@printf "$(C_GREEN)[Release] Tag verified: $(MODULE)/$(CUR_VER)$(C_RESET)\n"
else
	@printf "$(C_CYAN)[Release] Tagging v3 version: $(CUR_VER) (Force: $(FORCE))$(C_RESET)\n"

	@# Tag Root v3
	@git tag $(TAG_OPTS) "$(CUR_VER)" || exit 1

	@# Tag v3 Modules
	@for mod in $(V3_MODULES); do \
		if [ -d "$$mod" ]; then \
			printf "  Tagging $$mod/$(CUR_VER)\n"; \
			git tag $(TAG_OPTS) "$$mod/$(CUR_VER)" || exit 1; \
		fi; \
	done

	@# Verification
	@printf "$(C_GREEN)[Release] Verifying tags...$(C_RESET)\n"
	@git show "$(CUR_VER)" >/dev/null 2>&1 || exit 1
	@for mod in $(V3_MODULES); do \
		[ -d "$$mod" ] && git show "$$mod/$(CUR_VER)" >/dev/null 2>&1 || exit 1; \
	done
	@printf "$(C_GREEN)[Release] All v3 tags verified.$(C_RESET)\n"
endif
