# Distributed KV store on Raft — see PLAN.md for the phase roadmap.

GO      ?= go
PKGS    := ./...
CORE    := ./raft
BIN     := bin

.DEFAULT_GOAL := check

## check: the gate every phase must pass before it is considered done
.PHONY: check
check: fmt-check vet purity test

## purity: enforce the core's I/O-free contract (see raft/purity_test.go)
.PHONY: purity
purity:
	@echo "==> purity contract"
	@$(GO) test $(CORE) -run 'TestCoreHasNo' -count=1

## deps: show exactly what the core links — should stay tiny and boring
.PHONY: deps
deps:
	@echo "==> transitive dependencies of the raft core"
	@$(GO) list -deps $(CORE) | grep -v '^internal/'

.PHONY: test
test:
	@echo "==> tests"
	@$(GO) test $(PKGS) -count=1

## test-race: weak by design here — the core is single-threaded, so the race
## detector has little to say. The DST harness is the real correctness check.
.PHONY: test-race
test-race:
	@$(GO) test -race $(PKGS) -count=1

.PHONY: cover
cover:
	@$(GO) test $(PKGS) -coverprofile=coverage.out -covermode=atomic
	@$(GO) tool cover -func=coverage.out | tail -1

.PHONY: fmt
fmt:
	@gofmt -w $(shell find . -name '*.go' -not -path './vendor/*')

.PHONY: fmt-check
fmt-check:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "run 'make fmt'" && exit 1)

.PHONY: vet
vet:
	@$(GO) vet $(PKGS)

.PHONY: lint
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not installed. Install with:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"; \
		exit 1; }
	@golangci-lint run

.PHONY: build
build:
	@mkdir -p $(BIN)
	@$(GO) build -o $(BIN)/ ./cmd/... 2>/dev/null || echo "(no commands implemented yet)"

## dst: run the deterministic simulator against one seed (Phase 3)
.PHONY: dst
dst:
	@$(GO) run ./cmd/dst -seed=$(or $(SEED),1) -ticks=$(or $(TICKS),100000)

## soak: run many seeds in parallel; any failure prints a single reproducing seed
.PHONY: soak
soak:
	@$(GO) run ./cmd/dst -soak -seeds=$(or $(SEEDS),10000) -parallel=$(or $(PAR),8)

.PHONY: tidy
tidy:
	@$(GO) mod tidy

.PHONY: clean
clean:
	@rm -rf $(BIN) coverage.out data/ seeds/

.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
