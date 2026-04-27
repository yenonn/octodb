package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// --------------------------------------------------------------------------
// Write-path micro-benchmarks (isolated components)
// --------------------------------------------------------------------------

// BenchmarkWritePathSortKeyOnly measures just the proto marshal + sort key extraction.
func BenchmarkWritePathSortKeyOnly(b *testing.B) {
	span := makeTestTrace("svc", "span", uint64(time.Now().UnixNano()))
	store := &block2Store{}

	raw, _ := proto.Marshal(span)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.extractTraceSortKey(raw)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "keys/sec")
}

// BenchmarkWritePathWALOnly measures WAL append throughput (no memtable).
func BenchmarkWritePathWALOnly(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2StoreWithConfig(dataDir, StoreConfig{MemtableFlushThreshold: 1 << 30})
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	span := makeTestTrace("svc", "span", now)
	batch := []*tracepb.ResourceSpans{span}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = st.WriteTraces(ctx, "default", batch)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/sec")
}

// --------------------------------------------------------------------------
// Durability model comparison benchmarks
// --------------------------------------------------------------------------

// BenchmarkWriteTracesSyncWAL uses Block 1 style (sync every batch).
func BenchmarkWriteTracesSyncWAL(b *testing.B) {
	path := "bench_sync_store.wal"
	defer os.Remove(path)

	ws, _ := NewWALStore(path)
	defer ws.Close()

	span := makeTestTrace("svc", "span", uint64(time.Now().UnixNano()))
	batch := []*tracepb.ResourceSpans{span}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ws.WriteTraces(ctx, "default", batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/sec")
}

// --------------------------------------------------------------------------
// Read-path benchmarks (scanning + point lookups)
// --------------------------------------------------------------------------

// BenchmarkReadTracesFullScan measures full table scan performance.
func BenchmarkReadTracesFullScan(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		span := makeTestTrace("scan-svc", fmt.Sprintf("scan-%d", i), now+uint64(i))
		_ = st.WriteTraces(ctx, "default", []*tracepb.ResourceSpans{span})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = st.ReadTraces(ctx, TraceReadRequest{TenantID: "default"})
	}
	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkReadTracesByService measures filtered read performance.
func BenchmarkReadTracesByService(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		svc := fmt.Sprintf("svc-%d", i%10)
		span := makeTestTrace(svc, fmt.Sprintf("scan-%d", i), now+uint64(i))
		_ = st.WriteTraces(ctx, "default", []*tracepb.ResourceSpans{span})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = st.ReadTraces(ctx, TraceReadRequest{TenantID: "default", Service: "svc-5"})
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkReadTraceByID measures point-lookup via trace_id index.
func BenchmarkReadTraceByID(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	traceID := fmt.Sprintf("%x", []byte("1234567890abcdef1234567890abcdef"))
	for i := 0; i < 100; i++ {
		span := makeTestTrace("svc", fmt.Sprintf("scan-%d", i), now+uint64(i))
		span.ScopeSpans[0].Spans[0].TraceId = []byte("1234567890abcdef1234567890abcdef")
		_ = st.WriteTraces(ctx, "default", []*tracepb.ResourceSpans{span})
	}
	_ = st.Close()

	st2, _ := NewBlock2Store(dataDir)
	defer st2.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = st2.ReadTraceByID(ctx, "default", traceID)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lookups/sec")
}

// --------------------------------------------------------------------------
// End-to-end workload benchmarks (mixed read/write)
// --------------------------------------------------------------------------

// BenchmarkWriteReadMixed simulates realistic workload with concurrent writes and reads.
func BenchmarkWriteReadMixed(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	span := makeTestTrace("mixed-svc", "mixed-span", now)
	batch := []*tracepb.ResourceSpans{span}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = st.WriteTraces(ctx, "default", batch)
		_, _ = st.ReadTraces(ctx, TraceReadRequest{TenantID: "default"})
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "cycles/sec")
}

// BenchmarkConcurrentReadWrite simulates concurrent writers and readers.
func BenchmarkConcurrentReadWrite(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	span := makeTestTrace("mixed-svc", "mixed-span", now)

	// Pre-populate data.
	for i := 0; i < 100; i++ {
		_ = st.WriteTraces(ctx, "default", []*tracepb.ResourceSpans{span})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = st.WriteTraces(ctx, "default", []*tracepb.ResourceSpans{span})
		}()
		go func() {
			defer wg.Done()
			_, _ = st.ReadTraces(ctx, TraceReadRequest{TenantID: "default"})
		}()
		wg.Wait()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pairs/sec")
}

// --------------------------------------------------------------------------
// Cross-package full-pipeline benchmarks
// --------------------------------------------------------------------------

// BenchmarkFullPipelineTraces measures end-to-end pipeline: marshal → WAL → memtable → read.
func BenchmarkFullPipelineTraces(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	spans := make([]*tracepb.ResourceSpans, 100)
	for i := range spans {
		spans[i] = makeTestTrace("pipeline-svc", fmt.Sprintf("pipeline-%d", i), now+uint64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.WriteTraces(ctx, "default", spans); err != nil {
			b.Fatal(err)
		}
		if _, err := st.ReadTraces(ctx, TraceReadRequest{TenantID: "default", Service: "pipeline-svc"}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(spans))/b.Elapsed().Seconds(), "spans/sec")
}

// BenchmarkFullPipelineLogs measures end-to-end log pipeline.
func BenchmarkFullPipelineLogs(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	logs := make([]*logspb.ResourceLogs, 100)
	for i := range logs {
		logs[i] = makeTestLog("pipeline-svc", fmt.Sprintf("log-%d", i), logspb.SeverityNumber_SEVERITY_NUMBER_INFO, nil, nil, now+uint64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.WriteLogs(ctx, "default", logs); err != nil {
			b.Fatal(err)
		}
		if _, err := st.ReadLogs(ctx, LogReadRequest{TenantID: "default", Service: "pipeline-svc"}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(logs))/b.Elapsed().Seconds(), "logs/sec")
}

// BenchmarkFullPipelineMetrics measures end-to-end metric pipeline.
func BenchmarkFullPipelineMetrics(b *testing.B) {
	dir := b.TempDir()
	dataDir := fmt.Sprintf("%s/data", dir)
	st, _ := NewBlock2Store(dataDir)
	defer st.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	metrics := make([]*metricspb.ResourceMetrics, 100)
	for i := range metrics {
		metrics[i] = makeTestMetric("pipeline-svc", fmt.Sprintf("metric-%d", i), float64(i), now+uint64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.WriteMetrics(ctx, "default", metrics); err != nil {
			b.Fatal(err)
		}
		if _, err := st.ReadMetrics(ctx, MetricReadRequest{TenantID: "default", Service: "pipeline-svc"}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*len(metrics))/b.Elapsed().Seconds(), "metrics/sec")
}


