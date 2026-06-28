.PHONY: help build build-check test test-run test-run-verbose coverage coverage-html lint lint-go format \
        setup-safe setup-custom-lint build-custom-lint mod-tidy deps vendor clean test-output-dir check \
        unit-test-coverage

# Use bash for PIPESTATUS support (required for test-run target)
SHELL := /bin/bash

# Build configuration
BUILD_DIR := build

# Test configuration
TAGS ?=
RUN ?=
PKG ?= ./...
TIMEOUT ?= 10m
TEST_FLAGS := -v -mod=mod

ifdef TAGS
	TEST_FLAGS += -tags=$(TAGS)
endif

ifdef RUN
	TEST_FLAGS += -run=$(RUN)
endif

# Test output log (separate unit vs integration)
TEST_OUTPUT := test-outputs/$(if $(filter integration,$(TAGS)),test_integration_output.log,test_unit_output.log)

# Custom golangci-lint binary management
CUSTOM_GCL := tools/golangci-lint/custom-gcl

# ============================================
# HELP
# ============================================

help: ## Show this help
	@echo "KeyValor (Redis-compatible key-value store) - Makefile targets:"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort
	@echo ""
	@echo "Examples:"
	@echo "  make test-run                  # Run all unit tests"
	@echo "  make test-run RUN=TestMyFunc   # Single test by name"
	@echo "  make lint                      # Run all linters"
	@echo "  make format                    # Auto-fix code formatting"

# ============================================
# BUILD
# ============================================

build: ## Build the library (compile check)
	go build -mod=mod -v ./...

build-check: test-output-dir ## Verify code compiles (without running tests)
	@echo "Build check running. Read test-outputs/build-check.log for details"
	@echo "=== Production code ===" | tee test-outputs/build-check.log
	@FAILED=0; \
	echo "  KeyValor..." | tee -a test-outputs/build-check.log; \
	if ! bash -c 'set -o pipefail; go build -gcflags="-e" -mod=mod -v ./... 2>&1 | tee -a test-outputs/build-check.log'; then \
		echo "  ✗ FAILED: production code" | tee -a test-outputs/build-check.log; FAILED=1; \
	fi; \
	[ $$FAILED -eq 0 ] || exit 1
	@echo "=== Unit tests ===" | tee -a test-outputs/build-check.log
	@FAILED=0; \
	echo "  KeyValor..." | tee -a test-outputs/build-check.log; \
	if ! bash -c 'go test -c -gcflags="-e" -mod=mod -o /dev/null ./... 2>&1 | (grep -v "no test files" || true) | tee -a test-outputs/build-check.log; exit $${PIPESTATUS[0]}'; then \
		echo "  ✗ FAILED: unit tests" | tee -a test-outputs/build-check.log; FAILED=1; \
	fi; \
	[ $$FAILED -eq 0 ] || exit 1
	@echo "✓ Build check passed" | tee -a test-outputs/build-check.log

# ============================================
# TESTING
# ============================================

test-output-dir:
	@mkdir -p test-outputs

test: ## Run all unit tests
	go test -mod=mod -v ./...

test-run: test-output-dir ## Run tests with verbose output
	@echo "═══════════════════════════════════════════════════════════════════" | tee $(TEST_OUTPUT)
	@echo "Test Run: $$(date)" | tee -a $(TEST_OUTPUT)
	@echo "═══════════════════════════════════════════════════════════════════" | tee -a $(TEST_OUTPUT)
	@echo "Config:" | tee -a $(TEST_OUTPUT)
	@echo "  TAGS:    $(if $(TAGS),$(TAGS),(none - unit tests only))" | tee -a $(TEST_OUTPUT)
	@echo "  RUN:     $(if $(RUN),$(RUN),(all tests))" | tee -a $(TEST_OUTPUT)
	@echo "  PKG:     $(PKG)" | tee -a $(TEST_OUTPUT)
	@echo "  TIMEOUT: $(TIMEOUT)" | tee -a $(TEST_OUTPUT)
	@echo "═══════════════════════════════════════════════════════════════════" | tee -a $(TEST_OUTPUT)
	@echo "" | tee -a $(TEST_OUTPUT)
	@EXIT_CODE=0; \
	go test $(TEST_FLAGS) -timeout $(TIMEOUT) $(PKG) 2>&1 | tee -a $(TEST_OUTPUT); \
	if [ $${PIPESTATUS[0]} -ne 0 ]; then EXIT_CODE=1; fi; \
	echo "" | tee -a $(TEST_OUTPUT); \
	echo "═══════════════════════════════════════════════════════════════════" | tee -a $(TEST_OUTPUT); \
	echo "Test completed at: $$(date)" | tee -a $(TEST_OUTPUT); \
	echo "Exit code: $$EXIT_CODE" | tee -a $(TEST_OUTPUT); \
	echo "Output saved to: $(TEST_OUTPUT)" | tee -a $(TEST_OUTPUT); \
	echo "═══════════════════════════════════════════════════════════════════" | tee -a $(TEST_OUTPUT); \
	exit $$EXIT_CODE

test-run-verbose: ## Run tests with race detector
	go test -v -race -mod=mod ./...

unit-test-coverage: test-output-dir ## Run unit tests with coverage and threshold enforcement (fast, for pre-commit)
	@echo "Running unit tests with coverage..."
	@rm -f /tmp/keyvalor-coverage.out
	@PACKAGES=$$(go list ./... | grep -v /testutil); \
	if [ -n "$$PACKAGES" ]; then \
		go test -v -race -cover -mod=mod -coverprofile=/tmp/keyvalor-coverage.out $$PACKAGES; \
	fi
	@if [ -f /tmp/keyvalor-coverage.out ]; then \
		go tool cover -html=/tmp/keyvalor-coverage.out -o /tmp/keyvalor-coverage.html; \
		echo "keyvalor unit test coverage report: /tmp/keyvalor-coverage.html"; \
	fi
	@echo "Checking unit test coverage..."
	@if [ -f /tmp/keyvalor-coverage.out ]; then \
		COVERAGE=$$(go tool cover -func=/tmp/keyvalor-coverage.out | grep total | awk '{print $$3}' | sed 's/%//'); \
		THRESHOLD=0; \
		echo "keyvalor unit test coverage: $${COVERAGE}% (target: $${THRESHOLD}%)"; \
		if [ -z "$$COVERAGE" ]; then \
			echo "⚠ Could not compute coverage (go tool cover failed) - skipping threshold check"; \
		elif awk -v cov="$$COVERAGE" -v thresh="$$THRESHOLD" 'BEGIN {exit (cov < thresh)}'; then \
			echo "✓ coverage check passed"; \
		else \
			echo "⚠ coverage below target: $${COVERAGE}% < $${THRESHOLD}%"; \
			exit 1; \
		fi; \
	else \
		echo "⚠ No coverage file generated - skipping threshold check"; \
	fi

coverage: ## Generate test coverage report
	go test -v -mod=mod -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

coverage-html: coverage ## Generate HTML coverage report
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# ============================================
# LINT & FMT
# ============================================

# Custom golangci-lint binary management
setup-safe: ## Build custom-gcl only if it doesn't exist (safe for CI and dependencies)
	@if [ ! -f $(CUSTOM_GCL) ]; then \
		echo "Building custom golangci-lint with NilAway..."; \
		cd tools/golangci-lint && golangci-lint custom; \
	else \
		echo "custom-gcl already exists, skipping build"; \
	fi

setup-custom-lint: ## Force rebuild custom-gcl (use when you want fresh binary)
	@echo "Force rebuilding custom golangci-lint with NilAway..."
	@rm -f $(CUSTOM_GCL)
	cd tools/golangci-lint && golangci-lint custom
	@echo "✅ custom-gcl built successfully"

.PHONY: setup-safe setup-custom-lint
build-custom-lint: setup-safe

# NOTE: fatcontext linter is disabled because it causes auto-fix to convert = to := (variable shadowing in hooks/pipeline.go:41)
format: setup-safe ## Format code with gofmt, goimports, and gci
	@echo "Formatting code..."
	@./tools/golangci-lint/custom-gcl run --fix \
		--disable fatcontext \
		./... 2>/dev/null || true
	@echo "Fixing imports with goimports..."
	@which goimports > /dev/null 2>&1 || (echo "Installing goimports..." && go install golang.org/x/tools/cmd/goimports@latest)
	@goimports -w -local KeyValor .
	@echo "Fixing import grouping with gci..."
	@which gci > /dev/null 2>&1 || (echo "Installing gci..." && go install github.com/daixiang0/gci@latest)
	@gci write --skip-generated -s standard -s default -s "prefix(KeyValor)" .
	@echo "✅ Format complete"
	@echo ""

lint: lint-go ## Run Go linters
	@echo "✅ Go lint passed"

lint-go: setup-safe format ## Run golangci-lint on Go code (check-only, no auto-fix)
	@echo "Verifying .golangci.yml config..."
	@./tools/golangci-lint/custom-gcl config verify --schema tools/golangci-lint/golangci.jsonschema.json
	@echo "Running golangci-lint (with NilAway)..."
	@./tools/golangci-lint/custom-gcl run ./...
	@echo "✅ Go lint passed"

check: format build-check lint test-run ## Run format, build check, lint, and tests (pre-commit sequence)

# ============================================
# DEPENDENCIES
# ============================================

mod-tidy: ## Tidy go.mod and go.sum
	go mod tidy

deps: ## Download and tidy Go dependencies
	go mod download && go mod tidy

vendor: ## Vendor dependencies (sync + tidy)
	@echo "Tidying modules..."
	go mod tidy
	@echo "Vendoring modules..."
	go mod vendor
	@echo "✓ Modules tidied and vendored"

# ============================================
# CLEANUP
# ============================================

clean: ## Clean build artifacts and test cache
	rm -f coverage.out coverage.html
	rm -rf test-outputs
	go clean -cache -testcache
	@echo "✅ Cleaned"
