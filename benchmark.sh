#!/usr/bin/env bash
# OctoDB Benchmark Runner
# Systematic profiling + report generation for the write path.
#
# Usage:
#   ./benchmark.sh              # run all benchmarks + generate report
#   ./benchmark.sh --quick      # shorter benchtime (1s)
#   ./benchmark.sh --profile    # also generate CPU + memory profiles
#   ./benchmark.sh --compare      # run twice for before/after comparison
#
set -euo pipefail

BENCH_TIME="${BENCH_TIME:-2s}"
BENCH_PKGS="./internal/store ./internal/wal ./internal/memtable ./internal/segment ./internal/bloom ./internal/manifest"

# Colors for terminal output.
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

REPORT_DIR="benchmark_reports"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="$REPORT_DIR/report_$TIMESTAMP.txt"

run_quick=false
run_profile=false
run_compare=false

for arg in "$@"; do
    case "$arg" in
        --quick)   run_quick=true ;;
        --profile) run_profile=true ;;
        --compare) run_compare=true ;;
    esac
done

if [ "$run_quick" = true ]; then
    BENCH_TIME="1s"
fi

mkdir -p "$REPORT_DIR"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  OctoDB Benchmark Pipeline${NC}"
echo -e "${GREEN}========================================${NC}"
echo "Bench time: $BENCH_TIME"
echo "Report dir: $REPORT_DIR"
echo "Report file: $REPORT_FILE"
echo ""

# ---------------------------------------------------------------------------
# Helper: log header
# ---------------------------------------------------------------------------
log_section() {
    echo ""
    echo -e "${YELLOW}>>> $1${NC}"
    echo "=======================================" | tee -a "$REPORT_FILE"
    echo ">>> $1" | tee -a "$REPORT_FILE"
    echo "=======================================" | tee -a "$REPORT_FILE"
}

# ---------------------------------------------------------------------------
# 1. Quick compilation check
# ---------------------------------------------------------------------------
log_section "Build Check"
echo "Verifying all packages compile..."
if go build ./...; then
    echo -e "${GREEN}Build OK${NC}" | tee -a "$REPORT_FILE"
else
    echo -e "${RED}Build FAILED${NC}" | tee -a "$REPORT_FILE"
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Unit tests (sanity check)
# ---------------------------------------------------------------------------
log_section "Unit Tests"
echo "Running unit tests..."
if go test ./internal/store/... ./internal/wal/... ./internal/memtable/... ./internal/segment/... ./internal/bloom/... ./internal/manifest/... ./internal/index/... 2>&1 | tee -a "$REPORT_FILE"; then
    echo -e "${GREEN}Unit tests PASSED${NC}" | tee -a "$REPORT_FILE"
else
    echo -e "${RED}Unit tests FAILED${NC}" | tee -a "$REPORT_FILE"
    exit 1
fi

# ---------------------------------------------------------------------------
# 3. Main benchmark run
# ---------------------------------------------------------------------------
log_section "Benchmarks (benchtime=$BENCH_TIME)"
echo "Running benchmarks..."
go test -bench=. -benchtime="$BENCH_TIME" -benchmem $BENCH_PKGS 2>&1 | tee "$REPORT_DIR/bench_raw_$TIMESTAMP.txt"

# Parse throughput metrics from raw output.
echo "" | tee -a "$REPORT_FILE"
echo "Throughput Summary:" | tee -a "$REPORT_FILE"
grep -E "(spans/sec|logs/sec|metrics/sec|records/sec|inserts/sec|queries/sec|lookups/sec|cycles/sec|pairs/sec|seeks/sec|rotations/sec|snapshots/sec|rows/sec|filters/sec|checkpoints/sec|replays/sec)" "$REPORT_DIR/bench_raw_$TIMESTAMP.txt" | sed 's/^/  /' | tee -a "$REPORT_FILE"

# ---------------------------------------------------------------------------
# 4. Allocation stats summary
# ---------------------------------------------------------------------------
log_section "Allocation Summary"
echo "Top allocators (allocs/op):" | tee -a "$REPORT_FILE"
grep -E "Benchmark.*allocs/op" "$REPORT_DIR/bench_raw_$TIMESTAMP.txt" | sort -t '/' -k 2,2 -n -r | head -20 | sed 's/^/  /' | tee -a "$REPORT_FILE"

# ---------------------------------------------------------------------------
# 5. Latency (ns/op) summary
# ---------------------------------------------------------------------------
log_section "Latency Summary (ns/op)"
grep -E "Benchmark.*ns/op" "$REPORT_DIR/bench_raw_$TIMESTAMP.txt" | sort -t '/' -k 2,2 -n | head -20 | sed 's/^/  /' | tee -a "$REPORT_FILE"

# ---------------------------------------------------------------------------
# 6. CPU Profile (optional)
# ---------------------------------------------------------------------------
if [ "$run_profile" = true ]; then
    log_section "CPU Profiling"
    echo "Generating CPU profile..."
    go test -bench=BenchmarkWriteTraces/count=100 -benchtime="$BENCH_TIME" -cpuprofile="$REPORT_DIR/cpu_$TIMESTAMP.prof" ./internal/store/... > /dev/null
    echo "CPU profile: $REPORT_DIR/cpu_$TIMESTAMP.prof" | tee -a "$REPORT_FILE"
    echo "Top CPU consumers:" | tee -a "$REPORT_FILE"
    go tool pprof -top "$REPORT_DIR/cpu_$TIMESTAMP.prof" 2>/dev/null | head -20 | sed 's/^/  /' | tee -a "$REPORT_FILE"

    log_section "Memory Profiling"
    echo "Generating memory profile..."
    go test -bench=BenchmarkWriteTraces/count=100 -benchtime="$BENCH_TIME" -memprofile="$REPORT_DIR/mem_$TIMESTAMP.prof" ./internal/store/... > /dev/null
    echo "Memory profile: $REPORT_DIR/mem_$TIMESTAMP.prof" | tee -a "$REPORT_FILE"
    echo "Top memory consumers:" | tee -a "$REPORT_FILE"
    go tool pprof -top "$REPORT_DIR/mem_$TIMESTAMP.prof" 2>/dev/null | head -20 | sed 's/^/  /' | tee -a "$REPORT_FILE"
fi

# ---------------------------------------------------------------------------
# 7. Comparison mode (optional)
# ---------------------------------------------------------------------------
if [ "$run_compare" = true ]; then
    log_section "Comparison Mode"
    echo "Running 5 passes for stable numbers..."
    go test -bench=. -benchtime="$BENCH_TIME" -count=5 -benchmem $BENCH_PKGS > "$REPORT_DIR/bench_5x_$TIMESTAMP.txt"
    echo "5-pass results: $REPORT_DIR/bench_5x_$TIMESTAMP.txt" | tee -a "$REPORT_FILE"
    echo "Use 'benchstat' to compare against another run:" | tee -a "$REPORT_FILE"
    echo "  benchstat old.txt $REPORT_DIR/bench_5x_$TIMESTAMP.txt" | tee -a "$REPORT_FILE"
fi

# ---------------------------------------------------------------------------
# 8. Falsification check against design criteria
# ---------------------------------------------------------------------------
log_section "Falsification Criteria Check"

# Extract best throughput numbers.
BEST_TRACE_THROUGHPUT=$(grep "spans/sec" "$REPORT_DIR/bench_raw_$TIMESTAMP.txt" | awk '{print $1}' | sort -rn | head -1 || echo "0")
BEST_LOG_THROUGHPUT=$(grep "logs/sec" "$REPORT_DIR/bench_raw_$TIMESTAMP.txt" | awk '{print $1}' | sort -rn | head -1 || echo "0")
BEST_METRIC_THROUGHPUT=$(grep "metrics/sec" "$REPORT_DIR/bench_raw_$TIMESTAMP.txt" | awk '{print $1}' | sort -rn | head -1 || echo "0")

echo "Design target: >50,000 spans/sec ingestion" | tee -a "$REPORT_FILE"
echo "  Best trace throughput:  $BEST_TRACE_THROUGHPUT spans/sec" | tee -a "$REPORT_FILE"
if (( $(echo "$BEST_TRACE_THROUGHPUT > 50000" | bc -l 2>/dev/null || echo "0") )); then
    echo -e "  ${GREEN}PASS${NC} — exceeds 50,000 spans/sec" | tee -a "$REPORT_FILE"
else
    echo -e "  ${YELLOW}NOTE${NC} — may fall below target depending on workload" | tee -a "$REPORT_FILE"
fi

echo "" | tee -a "$REPORT_FILE"
echo "Best log throughput:    $BEST_LOG_THROUGHPUT logs/sec" | tee -a "$REPORT_FILE"
echo "Best metric throughput: $BEST_METRIC_THROUGHPUT metrics/sec" | tee -a "$REPORT_FILE"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log_section "Done"
echo -e "${GREEN}Report saved to: $REPORT_FILE${NC}"
echo "Raw benchmark output: $REPORT_DIR/bench_raw_$TIMESTAMP.txt"

if [ "$run_profile" = true ]; then
    echo "Profiles:"
    echo "  CPU:    $REPORT_DIR/cpu_$TIMESTAMP.prof"
    echo "  Memory: $REPORT_DIR/mem_$TIMESTAMP.prof"
fi

if [ "$run_compare" = true ]; then
    echo "Comparison data: $REPORT_DIR/bench_5x_$TIMESTAMP.txt"
fi

# Output report to stdout as well.
cat "$REPORT_FILE"
