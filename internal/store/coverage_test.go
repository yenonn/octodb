package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// TestInsert_UpdatesLastWrite tests that insert updates lastWrite.
func TestInsert_UpdatesLastWrite(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	b, err := store.getBundleForTenant("insert-test")
	if err != nil {
		t.Fatal(err)
	}

	before := b.lastWrite
	time.Sleep(10 * time.Millisecond)

	now := uint64(time.Now().UnixNano())
	span := makeTestTrace("svc", "span", now)
	if err := s.WriteTraces(context.Background(), "insert-test", []*tracepb.ResourceSpans{span}); err != nil {
		t.Fatal(err)
	}

	if !b.lastWrite.After(before) {
		t.Fatalf("expected lastWrite to be updated, before=%v after=%v", before, b.lastWrite)
	}
}

// TestLazyInitLegacyBundles tests the legacy bundle initialization path.
func TestLazyInitLegacyBundles(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	if err := store.lazyInitLegacyBundles(); err != nil {
		t.Fatalf("lazyInitLegacyBundles: %v", err)
	}

	if store.traceBundle == nil {
		t.Fatal("expected traceBundle to be initialized")
	}

	if err := store.lazyInitLegacyBundles(); err != nil {
		t.Fatalf("lazyInitLegacyBundles 2nd: %v", err)
	}
}

// TestGetBundleForTenant_NonRetryableUnhealthy tests getBundleForTenant with future retryAfter.
func TestGetBundleForTenant_NonRetryableUnhealthy(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	b := &tenantBundle{
		tenantID:   "noretry-test",
		dataDir:    filepath.Join(dataDir, "tenant", "noretry-test"),
		state:      StateUnhealthy,
		stateErr:   fmt.Errorf("not yet"),
		retryAfter: time.Now().Add(time.Hour),
	}
	store.tenantMap.Store("noretry-test", b)

	_, err = store.getBundleForTenant("noretry-test")
	if err == nil {
		t.Fatal("expected error for non-retryable unhealthy bundle")
	}
}


// TestReadTraceByID_NotFound tests ReadTraceByID returns empty for missing trace.
func TestReadTraceByID_NotFound(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	result, err := s.ReadTraceByID(ctx, "default", "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 traces, got %d", len(result))
	}
}

// TestReadLogs_TimeRangeFilter tests ReadLogs with time range.
func TestReadLogs_TimeRangeFilter(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := time.Now()
	oldLog := makeTestLog("svc", "old-log", logspb.SeverityNumber_SEVERITY_NUMBER_INFO, nil, nil, uint64(now.Add(-time.Hour).UnixNano()))
	newLog := makeTestLog("svc", "new-log", logspb.SeverityNumber_SEVERITY_NUMBER_INFO, nil, nil, uint64(now.UnixNano()))

	if err := s.WriteLogs(ctx, "default", []*logspb.ResourceLogs{oldLog, newLog}); err != nil {
		t.Fatal(err)
	}

	result, err := s.ReadLogs(ctx, LogReadRequest{
		TenantID:  "default",
		StartTime: now.Add(-time.Minute).UnixNano(),
		EndTime:   now.Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 log in time range, got %d", len(result))
	}
}

// TestReadMetrics_TimeRangeFilter tests ReadMetrics with time range.
func TestReadMetrics_TimeRangeFilter(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := time.Now()
	oldMetric := makeTestMetric("svc", "old-metric", 1.0, uint64(now.Add(-time.Hour).UnixNano()))
	newMetric := makeTestMetric("svc", "new-metric", 2.0, uint64(now.UnixNano()))

	if err := s.WriteMetrics(ctx, "default", []*metricspb.ResourceMetrics{oldMetric, newMetric}); err != nil {
		t.Fatal(err)
	}

	result, err := s.ReadMetrics(ctx, MetricReadRequest{
		TenantID:  "default",
		StartTime: now.Add(-time.Minute).UnixNano(),
		EndTime:   now.Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 metric in time range, got %d", len(result))
	}
}

// TestCreateBundle tests the createBundle function.
func TestCreateBundle(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	b, err := store.createBundle("test-signal", cfg.WALSyncInterval)
	if err != nil {
		t.Fatalf("createBundle: %v", err)
	}
	if b == nil {
		t.Fatal("expected bundle")
	}
	if b.subdir != "test-signal" {
		t.Fatalf("expected subdir 'test-signal', got %s", b.subdir)
	}
}

// TestNewBlock2StoreWithConfig_Error tests error path.
func TestNewBlock2StoreWithConfig_Error(t *testing.T) {
	_, err := NewBlock2StoreWithConfig("/nonexistent/path", DefaultStoreConfig())
	if err == nil {
		t.Fatal("expected error for invalid dataDir")
	}
}

// TestReplayBundle_WALReplay tests WAL replay by simulating crash and recovery.
func TestReplayBundle_WALReplay(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	span := makeTestTrace("replay-svc", "replay-span", now)

	if err := s.WriteTraces(ctx, "replay-test", []*tracepb.ResourceSpans{span}); err != nil {
		t.Fatal(err)
	}

	s.Close()

	s2, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	result, err := s2.ReadTraces(ctx, TraceReadRequest{TenantID: "replay-test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 trace after WAL replay, got %d", len(result))
	}
}

// TestMinDur tests the minDur helper.
func TestMinDur(t *testing.T) {
	tests := []struct {
		a, b, want time.Duration
	}{
		{1 * time.Second, 2 * time.Second, 1 * time.Second},
		{3 * time.Second, 2 * time.Second, 2 * time.Second},
		{0, 5 * time.Second, 0},
	}
	for _, tt := range tests {
		got := minDur(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("minDur(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

// TestActivateTenantBundle_ColdBundle tests activation of cold bundle.
func TestActivateTenantBundle_ColdBundle(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	b, _ := store.getBundleForTenant("cold-activate")
	store.evictBundle(b)

	b.mu.RLock()
	if b.state != StateCold {
		t.Fatalf("expected Cold, got %s", b.state)
	}
	b.mu.RUnlock()

	if err := store.activateTenantBundle(b); err != nil {
		t.Fatalf("activateTenantBundle: %v", err)
	}

	b.mu.RLock()
	if b.state != StateActive {
		t.Fatalf("expected Active, got %s", b.state)
	}
	b.mu.RUnlock()
}

// TestFlushTenantBundle_Empty tests flushTenantBundle with empty memtable.
func TestFlushTenantBundle_Empty(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)
	b, _ := store.getBundleForTenant("flush-empty")

	if err := store.flushTenantBundle(b); err != nil {
		t.Fatalf("flushTenantBundle on empty bundle: %v", err)
	}
}

// TestListWALFiles tests listWALFiles.
func TestListWALFiles(t *testing.T) {
	dir := t.TempDir()

	for _, name := range []string{"000001.wal", "000002.wal", "tombstone_000001.wal"} {
		err := os.WriteFile(filepath.Join(dir, name), []byte("test"), 0644)
		if err != nil {
			t.Fatal(err)
		}
	}

	files := listWALFiles(dir)
	if len(files) != 2 {
		t.Fatalf("expected 2 WAL files, got %d", len(files))
	}
	if files[0].seq >= files[1].seq {
		t.Fatal("expected WAL files sorted by sequence")
	}
}

// TestWALSeqFromName tests walSeqFromName.
func TestWALSeqFromName(t *testing.T) {
	tests := []struct {
		name string
		want int64
	}{
		{"000001.wal", 1},
		{"000123.wal", 123},
		{"invalid.wal", 0},
		{"abc.wal", 0},
	}

	for _, tt := range tests {
		got := walSeqFromName(tt.name)
		if got != tt.want {
			t.Errorf("walSeqFromName(%q) = %d, want %d", tt.name, got, tt.want)
		}
	}
}

// TestString_BundleState tests String() for BundleState.
func TestString_BundleState(t *testing.T) {
	tests := []struct {
		state BundleState
		want  string
	}{
		{StateActive, "active"},
		{StateParked, "parked"},
		{StateCold, "cold"},
		{StateUnhealthy, "unhealthy"},
	}

	for _, tt := range tests {
		got := tt.state.String()
		if got != tt.want {
			t.Errorf("BundleState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

// TestDeleteTraces_SingleKey tests deleting a single trace record.
func TestDeleteTraces_SingleKey(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	span := makeTestTrace("del-svc", "del-span", now)
	if err := s.WriteTraces(ctx, "default", []*tracepb.ResourceSpans{span}); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteTraces(ctx, "default", TraceReadRequest{TenantID: "default", Service: "del-svc"}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	result, err := s.ReadTraces(ctx, TraceReadRequest{TenantID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 traces after delete, got %d", len(result))
	}
}

// TestDeleteLogs_SingleKey tests deleting a single log record.
func TestDeleteLogs_SingleKey(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	log := makeTestLog("del-svc", "del-log", logspb.SeverityNumber_SEVERITY_NUMBER_INFO, nil, nil, now)
	if err := s.WriteLogs(ctx, "default", []*logspb.ResourceLogs{log}); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteLogs(ctx, "default", LogReadRequest{TenantID: "default", Service: "del-svc"}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	result, err := s.ReadLogs(ctx, LogReadRequest{TenantID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 logs after delete, got %d", len(result))
	}
}

// TestDeleteMetrics_SingleKey tests deleting a single metric record.
func TestDeleteMetrics_SingleKey(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	metric := makeTestMetric("del-svc", "del-metric", 1.0, now)
	if err := s.WriteMetrics(ctx, "default", []*metricspb.ResourceMetrics{metric}); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteMetrics(ctx, "default", MetricReadRequest{TenantID: "default", Service: "del-svc"}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	result, err := s.ReadMetrics(ctx, MetricReadRequest{TenantID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 metrics after delete, got %d", len(result))
	}
}

// TestReadTraces_TimeFilter tests time-based filtering.
func TestReadTraces_TimeFilter(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	oldSpan := makeTestTrace("svc", "old-span", now-uint64(time.Hour.Nanoseconds()))
	newSpan := makeTestTrace("svc", "new-span", now)

	if err := s.WriteTraces(ctx, "default", []*tracepb.ResourceSpans{oldSpan, newSpan}); err != nil {
		t.Fatal(err)
	}

	result, err := s.ReadTraces(ctx, TraceReadRequest{
		TenantID:  "default",
		StartTime: time.Now().Add(-time.Minute).UnixNano(),
		EndTime:   time.Now().Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 trace in time range, got %d", len(result))
	}
}
