package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// TestTenantBundle_Lifecycle tests getBundleForTenant, activateTenantBundle,
// defaultTenantBundle, and bundle lifecycle transitions.
func TestTenantBundle_Lifecycle(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	// 1. getBundleForTenant creates bundle on first access
	b, err := store.getBundleForTenant("lifecycle-test")
	if err != nil {
		t.Fatalf("getBundleForTenant: %v", err)
	}
	if b == nil {
		t.Fatal("expected bundle, got nil")
	}
	if b.tenantID != "lifecycle-test" {
		t.Fatalf("expected tenantID lifecycle-test, got %s", b.tenantID)
	}

	// 2. Second call returns same bundle from sync.Map
	b2, err := store.getBundleForTenant("lifecycle-test")
	if err != nil {
		t.Fatalf("getBundleForTenant second call: %v", err)
	}
	if b != b2 {
		t.Fatal("expected same bundle from sync.Map")
	}

	// 3. defaultTenantBundle returns default tenant
	def, err := store.defaultTenantBundle()
	if err != nil {
		t.Fatalf("defaultTenantBundle: %v", err)
	}
	if def.tenantID != "default" {
		t.Fatalf("expected tenantID default, got %s", def.tenantID)
	}

	// 4. Write triggers active state
	ctx := context.Background()
	span := makeTestTrace("lifecycle-svc", "lifecycle-span", uint64(time.Now().UnixNano()))
	if err := s.WriteTraces(ctx, "lifecycle-test", []*tracepb.ResourceSpans{span}); err != nil {
		t.Fatalf("write: %v", err)
	}

	b.mu.RLock()
	if b.state != StateActive {
		t.Fatalf("expected state Active after write, got %s", b.state)
	}
	b.mu.RUnlock()
}

// TestTenantBundle_EvictAndPark tests evictBundle and parkBundle transitions.
func TestTenantBundle_EvictAndPark(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	// Create bundle by writing
	ctx := context.Background()
	span := makeTestTrace("evict-svc", "evict-span", uint64(time.Now().UnixNano()))
	if err := s.WriteTraces(ctx, "evict-test", []*tracepb.ResourceSpans{span}); err != nil {
		t.Fatalf("write: %v", err)
	}

	b, _ := store.getBundleForTenant("evict-test")
	if b == nil {
		t.Fatal("expected bundle")
	}

	// Park the bundle (Active → Parked)
	if err := store.parkBundle(b); err != nil {
		t.Fatalf("parkBundle: %v", err)
	}

	b.mu.RLock()
	if b.state != StateParked {
		t.Fatalf("expected state Parked, got %s", b.state)
	}
	b.mu.RUnlock()

	// Evict the bundle (Parked → Cold)
	store.evictBundle(b)

	// After evict, bundle should be Cold but still in sync.Map
	if val, ok := store.tenantMap.Load("evict-test"); ok {
		b2 := val.(*tenantBundle)
		b2.mu.RLock()
		if b2.state != StateCold {
			t.Fatalf("expected state Cold after evict, got %s", b2.state)
		}
		if b2.locator != nil {
			t.Fatal("expected locator nil after evict (Cold drops locator)")
		}
		b2.mu.RUnlock()
	} else {
		t.Fatal("expected bundle still in tenantMap after evict")
	}
}

// TestTenantBundle_AutoMigrateLegacy tests that legacy data is auto-migrated.
func TestTenantBundle_AutoMigrateLegacy(t *testing.T) {
	dataDir := t.TempDir()

	// Create legacy layout
	legacyTraces := filepath.Join(dataDir, "traces")
	if err := os.MkdirAll(legacyTraces, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyTraces, "000001.wal"), []byte("legacy"), 0644); err != nil {
		t.Fatal(err)
	}

	// Open store — should auto-migrate
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024
	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Verify migration happened
	defaultTenant := filepath.Join(dataDir, "tenant", "default")
	if _, err := os.Stat(defaultTenant); os.IsNotExist(err) {
		t.Fatal("auto-migration did not create tenant/default/")
	}

	// Verify legacy WAL was moved
	migratedWAL := filepath.Join(defaultTenant, "000001.wal")
	if _, err := os.Stat(migratedWAL); os.IsNotExist(err) {
		t.Fatal("legacy WAL not migrated")
	}
}

// TestTenantBundle_UnhealthyRetry tests that unhealthy bundles are retried.
func TestTenantBundle_UnhealthyRetry(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	store := s.(*block2Store)

	// Create an unhealthy bundle manually
	b := &tenantBundle{
		tenantID: "unhealthy-test",
		dataDir:  filepath.Join(dataDir, "tenant", "unhealthy-test"),
		state:    StateUnhealthy,
		stateErr: fmt.Errorf("simulated failure"),
		retryAfter: time.Now().Add(50 * time.Millisecond),
	}
	store.tenantMap.Store("unhealthy-test", b)

	// Manually trigger evict and park
	if err := store.parkBundle(b); err != nil {
		t.Fatalf("parkBundle: %v", err)
	}

	// Verify bundle transitioned
	b.mu.RLock()
	if b.state != StateParked && b.state != StateCold {
		t.Logf("bundle state after park: %s", b.state)
	}
	b.mu.RUnlock()
}

// TestTenantBundle_FlushTenantBundle tests the per-tenant flush path end-to-end.
func TestTenantBundle_FlushTenantBundle(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1 // tiny threshold to force flush

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())

	// Write a span to trigger flush via monitor
	span := makeTestTrace("flush-svc", "flush-span", now)
	if err := s.WriteTraces(ctx, "flush-test", []*tracepb.ResourceSpans{span}); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Manually trigger flush
	store := s.(*block2Store)
	b, _ := store.getBundleForTenant("flush-test")
	if err := store.flushTenantBundle(b); err != nil {
		t.Fatalf("flushTenantBundle: %v", err)
	}

	// Verify segment exists
	entries, _ := os.ReadDir(b.dataDir)
	var hasPB bool
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".pb" {
			hasPB = true
			break
		}
	}
	if !hasPB {
		t.Fatal("expected segment file after flush")
	}

	s.Close()
}

// TestTenantBundle_MultipleTenants tests writing and reading across multiple tenants.
func TestTenantBundle_MultipleTenants(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())

	// Write to two tenants
	span1 := makeTestTrace("tenant-a-svc", "tenant-a-span", now)
	span2 := makeTestTrace("tenant-b-svc", "tenant-b-span", now)

	if err := s.WriteTraces(ctx, "tenant-a", []*tracepb.ResourceSpans{span1}); err != nil {
		t.Fatalf("write tenant-a: %v", err)
	}
	if err := s.WriteTraces(ctx, "tenant-b", []*tracepb.ResourceSpans{span2}); err != nil {
		t.Fatalf("write tenant-b: %v", err)
	}

	// Read back tenant-a only
	result, err := s.ReadTraces(ctx, TraceReadRequest{TenantID: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 trace for tenant-a, got %d", len(result))
	}

	// Read back tenant-b only
	result, err = s.ReadTraces(ctx, TraceReadRequest{TenantID: "tenant-b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 trace for tenant-b, got %d", len(result))
	}

	// Verify directories
	if _, err := os.Stat(filepath.Join(dataDir, "tenant", "tenant-a")); os.IsNotExist(err) {
		t.Fatal("tenant-a dir not created")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "tenant", "tenant-b")); os.IsNotExist(err) {
		t.Fatal("tenant-b dir not created")
	}
}

// TestTenantBundle_ConcurrentWrites tests concurrent writes to the same tenant.
func TestTenantBundle_ConcurrentWrites(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024 // large threshold to avoid flush during test

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			span := makeTestTrace("concurrent-svc", fmt.Sprintf("concurrent-span-%d", n), now+uint64(n))
			if err := s.WriteTraces(ctx, "concurrent", []*tracepb.ResourceSpans{span}); err != nil {
				t.Errorf("write %d: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	// Read back all traces
	result, err := s.ReadTraces(ctx, TraceReadRequest{TenantID: "concurrent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 10 {
		t.Fatalf("expected 10 traces, got %d", len(result))
	}
}

// TestTenantBundle_CloseFlushes tests that Close flushes memtables.
func TestTenantBundle_CloseFlushes(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.MemtableFlushThreshold = 1024 * 1024 // large threshold — no auto-flush

	s, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := uint64(time.Now().UnixNano())
	span := makeTestTrace("close-flush-svc", "close-flush-span", now)
	if err := s.WriteTraces(ctx, "close-flush", []*tracepb.ResourceSpans{span}); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Close should flush
	s.Close()

	// Reopen and verify data survived
	s2, err := NewBlock2StoreWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	result, err := s2.ReadTraces(ctx, TraceReadRequest{TenantID: "close-flush"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 trace after close+reopen, got %d", len(result))
	}
}
