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
	@$(GO) build -o $(BIN)/ ./cmd/...

## dst: run the deterministic simulator against one seed (Phase 3)
.PHONY: dst
dst:
	@$(GO) run ./cmd/dst -seed=$(or $(SEED),1) -ticks=$(or $(TICKS),100000)

## soak: run many seeds in parallel; any failure prints a single reproducing seed
.PHONY: soak
soak:
	@$(GO) run ./cmd/dst -soak -seeds=$(or $(SEEDS),10000) -parallel=$(or $(PAR),8)

## demo: start a 3-node cluster, kill the leader, restart it from its WAL
.PHONY: demo
demo: build
	@./demo.sh

## bench: measure naive vs group commit vs no-fsync, then a concurrency sweep
.PHONY: bench
bench: build
	@rm -f bench/results.csv
	@CLIENTS=64 DURATION=15s bench/run.sh "1-naive"        bench/results.csv -max-batch=1
	@CLIENTS=64 DURATION=15s bench/run.sh "2-group-commit" bench/results.csv -max-batch=256
	@CLIENTS=64 DURATION=15s bench/run.sh "3-no-fsync"     bench/results.csv -max-batch=256 -unsafe-no-fsync
	@for c in 1 8 32 128; do CLIENTS=$$c DURATION=10s bench/run.sh "sweep-c$$c" bench/results.csv -max-batch=256 | head -1; done

## bench-reads: compare the three read paths at 1 and 64 clients
.PHONY: bench-reads
bench-reads: build
	@rm -f bench/reads.csv
	@for m in log readindex lease; do \
		./bin/kvbench -spawn=3 -clients=64 -duration=12s -warmup=3s -op=get \
		  -server-flags="-read-mode=$$m" -label="read-$$m" -csv=bench/reads.csv | head -1; done
	@for m in log readindex lease; do \
		./bin/kvbench -spawn=3 -clients=1 -duration=8s -warmup=3s -op=get \
		  -server-flags="-read-mode=$$m" -label="read-$$m-c1" -csv=bench/reads.csv | head -1; done

## bench-failover: sweep election timeouts, measuring failover against churn
.PHONY: bench-failover
bench-failover: build
	@rm -f bench/failover.csv
	@./bin/dst -failover -seeds=500 -nodes=5 -election-timeouts=50,75,100,150,250,400,600 -csv=bench/failover.csv
	@./bin/dst -failover -seeds=500 -nodes=5 -pre-vote -election-timeouts=50,75,100,150,250,400,600 -csv=bench/failover.csv

## plots: render bench/results.csv into analysis/*.png (needs uv)
.PHONY: plots
plots:
	@cd analysis && uv run --quiet python plot_bench.py

## chaos: run a real cluster under real faults and check linearizability
.PHONY: chaos
chaos: build
	@./bin/chaosctl -nodes=5 -duration=$(or $(DURATION),60s) -seed=$(or $(SEED),1) -viz=chaos-violation.html

## chaos-verify: prove the chaos suite can fail, using deliberately stale reads
.PHONY: chaos-verify
chaos-verify: build
	@./bin/chaosctl -nodes=5 -duration=30s -seed=1 -unsafe-local-reads || echo "(a violation here is the expected result)"

## proto: regenerate gRPC code (needs protoc, protoc-gen-go, protoc-gen-go-grpc)
.PHONY: proto
proto:
	@protoc --proto_path=proto \
		--go_out=proto/kvstorepb --go_opt=paths=source_relative \
		--go-grpc_out=proto/kvstorepb --go-grpc_opt=paths=source_relative \
		proto/kvstore.proto

.PHONY: tidy
tidy:
	@$(GO) mod tidy

.PHONY: clean
clean:
	@rm -rf $(BIN) coverage.out data/ seeds/

.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
