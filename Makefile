.PHONY: explain test bench fmt lint lint-json default

.DEFAULT_GOAL := explain

explain:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Options for test targets:"
	@echo "  [N=...]  - Number of times to run burst tests (default 1)"
	@echo "  [RACE=1] - Enable the race flag for tests."
	@echo "  [V=1]    - Add V=1 for verbose output"
	@echo ""
	@echo "Targets:"
	@echo "  test             - Run tests."
	@echo "  bench            - Run benchmarks."
	@echo "  fmt              - Run go fmt."
	@echo "  lint             - Run golangci-lint."
	@echo "  lint-json        - Run golangci-lint (JSON output)."
	@echo "  explain          - Display this help message."

# Flag V=1 for verbose mode
TEST_FLAGS :=
ifdef RACE
	TEST_FLAGS += -race
endif
ifdef V
	TEST_FLAGS += -v
endif

# Number of times to run burst tests, default 1
N ?= 1

test:
	@echo "==> Running tests..."
	@go test -count=$(N) $(TEST_FLAGS) ./...

bench:
	@echo "==> Running benchmarks..."
	@go test -count=$(N) -bench=. -benchmem -benchtime=1s -run=^$ -v

fmt:
	@echo "==> Running formatter (go fmt)..."
	@gofmt -w .

lint:
	@echo "==> Running golangci-lint..."
	@golangci-lint run

lint-json:
	@echo "==> Running golangci-lint (JSON output)..."
	@golangci-lint run --output.json.path stdout
