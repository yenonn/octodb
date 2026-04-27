.PHONY: build run test test-integration test-block1 test-block2 bench bench-cpu bench-mem bench-compare clean

BINARY := octodb
GO := go

BENCH_PKGS := ./internal/store ./internal/wal ./internal/memtable ./internal/segment ./internal/bloom ./internal/manifest
BENCH_TIME := 2s

testclient := bin/testclient

build:
	@mkdir -p bin
	$(GO) build -o bin/$(BINARY) ./cmd/octodb
	$(GO) build -o $(testclient) ./cmd/testclient

run: build
	./bin/$(BINARY)

test:
	$(GO) test ./...

# Run integration tests that compile the server binary and exercise it end-to-end.
test-integration:
	$(GO) test ./tests/integration/ -v -timeout 60s

# Run only Block 1 integration test (WAL crash recovery).
test-block1:
	$(GO) test ./tests/integration/ -v -run TestBlock1_WALCrashRecovery -timeout 30s

# Run only Block 2 integration test (memtable flush + query).
test-block2:
	$(GO) test ./tests/integration/ -v -run TestBlock2_FlushAndQuery -timeout 60s

# ---------------------------------------------------------------------------
# Benchmark targets
# ---------------------------------------------------------------------------

# Run all benchmarks across packages with throughput + allocation stats.
bench:
	@echo "Running benchmarks (time=$(BENCH_TIME))..."
	$(GO) test -bench=. -benchtime=$(BENCH_TIME) -benchmem $(BENCH_PKGS)

# Run benchmarks with CPU profiling.
bench-cpu:
	@echo "Running benchmarks with CPU profile..."
	$(GO) test -bench=. -benchtime=$(BENCH_TIME) -cpuprofile=/tmp/octodb_cpu.prof -benchmem $(BENCH_PKGS)
	@echo "CPU profile saved to /tmp/octodb_cpu.prof"

# Run benchmarks with memory profiling.
bench-mem:
	@echo "Running benchmarks with memory profile..."
	$(GO) test -bench=. -benchtime=$(BENCH_TIME) -memprofile=/tmp/octodb_mem.prof -benchmem $(BENCH_PKGS)
	@echo "Memory profile saved to /tmp/octodb_mem.prof"

# Run benchmarks twice and compare (basic regression check).
bench-compare:
	@echo "Running baseline benchmark..."
	$(GO) test -bench=. -benchtime=$(BENCH_TIME) -count=5 -benchmem $(BENCH_PKGS) > /tmp/bench_baseline.txt
	@echo "Baseline saved to /tmp/bench_baseline.txt"
	@echo "Apply changes, then run 'make bench-compare-against-baseline'"

bench-compare-against-baseline:
	@echo "Running comparison benchmark..."
	$(GO) test -bench=. -benchtime=$(BENCH_TIME) -count=5 -benchmem $(BENCH_PKGS) > /tmp/bench_current.txt
	@echo "Comparison saved to /tmp/bench_current.txt"
	@echo "Use 'benchstat /tmp/bench_baseline.txt /tmp/bench_current.txt' to compare"

clean:
	rm -rf bin/ *.wal octodb-data/ *.log *.pb bench_*.txt bench_*.prof *.bench
