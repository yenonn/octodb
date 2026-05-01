# ADR-008 Rev 2: Per-Tenant Bundles with Lifecycle Management

**Status:** Approved  
**Date:** 2026-05-01  
**Revises:** ADR-008 (2026-04-29)  
**Driver:** Close critical blockers (TODO #3, #4, #676, #7) before implementing per-tenant isolation  

---

## 1. Context & Problem Statement

The original ADR-008 (2026-04-29) specified per-tenant signal bundles with Active/Parked/Cold lifecycle states. During pre-implementation review, four critical issues were identified that make the original design unsafe to implement:

### Critical Issues Fixed Before This Revision

| Issue | Location | Problem | Fix Applied |
|-------|----------|---------|-------------|
| **TODO #4: Double Parse** | `extractTraceSortKey` | Marshals protobuf then unmarshals to extract tenant ID — 2x CPU, sort key tenant can diverge from caller | `extractTraceSortKey(tenantID, data)` — trusts caller-provided tenant |
| **TODO #3: Memtable Race** | `memtable.Set.Insert` | Uses `RLock` during write operation — data race | Changed to `Lock()` |
| **TODO #676: Tenant Enforcement** | `traceKeyMatches` | "tenant check not strictly enforced" — can leak records across queries | Strict span-tenant vs request-tenant comparison |
| **TODO #7: Delete WAL Sync** | `DeleteTraces/Logs/Metrics` | Uses async `wal.Sync()` — tombstones may be lost on crash | Explicit `wal.Sync()` after tombstone batch append |

### Additional Critical Issues Discovered

**#5: Undefined Lock Ordering (NEW — Hard Blocker for Approach C)**
The original design introduces a `tenantMap map[string]*signalBundle` that multiple goroutines access. Without defined lock hierarchy, eviction (holding bundle.mu) and writes (acquiring bundle.mu via getBundleForTenant) deadlock. This is an AB-BA deadlock that **will** happen under load.

**#6: No Error State (NEW — Soft Blocker)**
The original design has no defined behavior when Cold→Active fails (WAL corruption, disk error, missing segment files). Current code panics or silently skips data.

**#7: Unbounded Locator Memory (NEW — Scalability Risk)**
The original design keeps `TraceLocator` in RAM for Parked bundles. At 10K tenants with 1MB locators each = 10GB RAM. This violates the bounded-memory promise of Approach C.

### Why We Revise Before Implementing

These issues are **structural**, not cosmetic. They affect the core state machine, locking model, and memory footprint. Fixing them after implementation would require rewriting the per-tenant logic from scratch. We fix the design first, then implement once.

---

## 2. Decision: Per-Tenant Single Bundle with 4-State Lifecycle

### What Changed from ADR-008 Rev 1

| Aspect | Rev 1 (2026-04-29) | Rev 2 (this document) |
|--------|-------------------|----------------------|
| **Bundles per tenant** | 3 (traces, logs, metrics) | **1** (all signals interleaved) |
| **States** | Active, Parked, Cold | **Active, Parked, Cold, Unhealthy** |
| **Tenant map** | `map + RWMutex` (lock ordering undefined) | **`sync.Map`** (zero explicit locks, no deadlock) |
| **Locator memory** | Kept in RAM for Parked bundles | **Dropped from RAM on Cold** |
| **Error handling** | Undefined | **Unhealthy state + exponential backoff retry** |
| **Migration** | Manual bash script | **Auto-detect on startup** |

### Why Single Bundle Per Tenant

The current sort key already includes `DType` (DataType) as the first field:
```go
key := otelutil.TraceSortKey{TenantID: tenantID, Service: svc, TimeNano: timeNano, SpanID: spanID}
// Key() returns: "trace\x00{tenant}\x00{service}\x00{time}\x00{span_id}"
```

This means traces, logs, and metrics are already sorted separately within the same B-tree. Three separate bundles per tenant creates:
- 3 WAL files (3x fd pressure)
- 3 memtables (3x RAM pressure)
- 3 manifests (3x compaction overhead)
- 3 flush monitors (3x goroutine overhead)

A single bundle is simpler, uses fewer resources, and the `DType` prefix already provides the separation we need.

---

## 3. Directory Layout

```
dataDir/
  tenant/
    {tenant-id}/
      000001.wal
      manifest.log
      manifest.log.checkpoint
      sst-00000001.pb
      sst-00000001.pb.trace.index
      sst-00000001.bloom
```

### Filesystem Safety

`tenant-id` is sanitized before `os.MkdirAll`. The resolver already produces filesystem-safe strings, but we add a defensive pass:
- Reject tenant IDs containing `/`, `\`, `..`, or control characters
- Replace spaces with `_`
- Max length: 128 characters
- Invalid IDs fall back to `sanitized_{hash}`

---

## 4. Bundle States

### State Definitions

| State | WAL | Memtable | Locator | FDs | Read | Write |
|-------|-----|----------|---------|-----|------|-------|
| **Active** | Open, appending | Dual memtable hot | In RAM | 1 (WAL) + overhead | ✅ | ✅ |
| **Parked** | Closed, flushed | Empty | In RAM | 0 | ✅ | → Active first |
| **Cold** | Closed, flushed | Empty | **Not in RAM** | 0 | → Active first | → Active first |
| **Unhealthy** | Closed or failed | Empty | May be nil | 0 | ❌ (error) | ❌ (error) |

### State Transitions

```
                        ┌────────────────────────────────────────┐
                        │  NOT_FOUND (on disk but not in map)  │
                        └──────────────┬─────────────────────────┘
                                       │ getBundleForTenant()
                                       ▼
                              ┌────────────────┐
                              │     COLD       │
                              │  (locator=nil) │
                              └───────┬────────┘
                                      │ write / read
                                      ▼
  ┌──────────────────────────────────────────────────────────────┐
  │                         ACTIVE                               │
  │  WAL open, memtable hot, locator in RAM, bundle.mu held      │
  └────────────┬────────────────────┬────────────────────────────┘
               │ idle > ParkTimeout │          │ flush error / WAL corruption
               ▼                    │          ▼
        ┌────────────┐             │   ┌─────────────┐
        │   PARKED   │◄─────────────┘   │  UNHEALTHY  │
        │  (no WAL)  │                  │   (err)     │
        └─────┬──────┘                  └──────┬──────┘
              │ idle > ColdTimeout              │ retryAfter elapsed
              ▼                                 │ (exponential backoff)
         ┌─────────┐                            │
         │  COLD   │                            ▼
         │(no RAM) │────────────────────────►  ACTIVE (rebuild attempt)
         └─────────┘
```

### Transition Triggers

| Transition | Condition | Action |
|------------|-----------|--------|
| **NotFound → Cold** | First access for tenant | Check disk. If segments exist → Cold (no locator). If empty dir → Cold (empty). |
| **Cold → Active** | `WriteTraces/ReadTraces` called | Open WAL (new or append). Rebuild locator from `.trace.index` files. Create memtable. Replay WAL. |
| **Parked → Active** | `WriteTraces` called | Reopen WAL in append mode. Memtable stays empty (data already in segment). Fast. |
| **Active → Parked** | `time.Since(lastWrite) > ParkTimeout` (default 30s) | Flush memtable → segment → fsync → write checkpoint → close WAL. Keep locator in RAM. |
| **Parked → Cold** | `time.Since(lastRead) > ColdTimeout` (default 5min) OR LRU eviction | Drop locator from RAM. Bundle stays in sync.Map. |
| **Any → Unhealthy** | WAL open fails, manifest append fails, flush fsync fails | Record `stateErr`, set `retryAfter = now + 5s`. Return error to caller. |
| **Unhealthy → Active** | Lifecycle monitor: `now > retryAfter` | Attempt full rebuild (same as Cold→Active). Success → Active. Failure → double `retryAfter` (5→10→30→60s max). |

---

## 5. Data Structures

### BundleState

```go
type BundleState int

const (
    StateActive BundleState = iota
    StateParked
    StateCold
    StateUnhealthy
)
```

### tenantBundle

```go
type tenantBundle struct {
    tenantID    string
    dataDir     string           // dataDir/tenant/{tenantID}/
    
    // Storage
    wal         *wal.Writer
    walSeq      int64
    set         *memtable.Set    // dual memtable (all signals)
    man         *manifest.Manager
    locator     *index.TraceLocator  // nil when Cold
    
    // Lifecycle
    state       BundleState
    stateErr    error            // last error causing Unhealthy
    retryAfter  time.Time        // next retry timestamp
    lastWrite   time.Time
    lastRead    time.Time
    
    // Locking
    mu          sync.RWMutex     // protects bundle mutable fields
}
```

### block2Store (revised)

```go
type block2Store struct {
    dataDir    string
    cfg        StoreConfig
    
    // Per-tenant bundles (NEW)
    tenantMap  sync.Map         // tenantID -> *tenantBundle
    
    // Legacy bundles (during migration, or for default tenant)
    traceBundle  *signalBundle   // used when tenant resolution is disabled
    logBundle    *signalBundle
    metricBundle *signalBundle
    
    // Lifecycle management
    stopFlush    chan struct{}
    flushDone    sync.WaitGroup
    closeOnce    sync.Once
    
    // Migration flag
    migrated     bool             // true if legacy data auto-migrated
}
```

---

## 6. Lock Hierarchy (No-Deadlock Guarantee)

### The Rule

> **Never hold `bundle.mu` while calling `sync.Map` operations.**

### Why This Works

`sync.Map` uses Go runtime-internal lock sharding. There is no user-visible lock that can participate in a deadlock cycle with `bundle.mu`. The only way to deadlock is if a goroutine holds `bundle.mu` and calls `sync.Map.Store/Load/Delete` while another goroutine holds a different `bundle.mu` and does the same. But `sync.Map` locks are independent per-key, so this cannot form a cycle.

### Eviction Pattern (Safe)

```go
func (s *block2Store) evictBundle(b *tenantBundle) {
    // Phase 1: Remove from sync.Map (no bundle.mu held)
    s.tenantMap.Delete(b.tenantID)
    
    // Phase 2: Now safe to lock bundle — nobody else can find it
    b.mu.Lock()
    defer b.mu.Unlock()
    
    // Flush if Active, close WAL, drop locator
    if b.state == StateActive {
        s.flushBundle(b)
    }
    if b.wal != nil {
        b.wal.Close()
    }
    b.locator = nil
    b.state = StateCold
}
```

### getBundleForTenant Pattern (Safe)

```go
func (s *block2Store) getBundleForTenant(tenantID string) (*tenantBundle, error) {
    // Phase 1: sync.Map lookup (no locks held)
    if val, ok := s.tenantMap.Load(tenantID); ok {
        b := val.(*tenantBundle)
        b.mu.RLock()
        state := b.state
        b.mu.RUnlock()
        
        if state == StateUnhealthy {
            return nil, fmt.Errorf("tenant %s bundle unhealthy: %w", tenantID, b.stateErr)
        }
        return b, nil
    }
    
    // Phase 2: Not found — create from disk
    b := &tenantBundle{tenantID: tenantID, dataDir: filepath.Join(s.dataDir, "tenant", tenantID)}
    
    // Phase 3: Attempt build (may fail → Unhealthy)
    if err := s.buildBundle(b); err != nil {
        b.state = StateUnhealthy
        b.stateErr = err
        b.retryAfter = time.Now().Add(5 * time.Second)
        s.tenantMap.Store(tenantID, b)
        return nil, fmt.Errorf("build tenant %s bundle: %w", tenantID, err)
    }
    
    b.state = StateActive
    s.tenantMap.Store(tenantID, b)
    return b, nil
}
```

---

## 7. Memory Management

### Bounded Memory Guarantee

| Tenant Count | Active Bundles | Parked Bundles | Cold Bundles | Total RAM |
|-------------|---------------|----------------|--------------|-----------|
| 1K | 500 (configurable) | 300 | 200 | ~500MB |
| 5K | 500 | 1K | 3.5K | ~1.5GB |
| 10K | 500 | 1.5K | 8K | ~2GB |

**Active:** ~1MB per bundle (memtable + locator + WAL buffer)
**Parked:** ~200KB per bundle (locator only)
**Cold:** ~0KB per bundle (nothing in RAM)

### LRU Eviction

When `MaxActiveTenants` (default 500) is reached and a new tenant needs activation:
1. Scan all Parked bundles, find oldest `lastRead`
2. `evictBundle` on that tenant (Parked → Cold)
3. Now activate new tenant

---

## 8. Migration Strategy

### Auto-Detect Legacy Layout

On startup, `NewBlock2StoreWithConfig` checks:
1. Does `dataDir/traces/` exist with segments/WAL?
2. Does `dataDir/tenant/` NOT exist?

If both true → **auto-migrate**:
```go
// One-time atomic migration
mkdir -p dataDir/tenant/default
cp -r dataDir/traces/* dataDir/tenant/default/
cp -r dataDir/logs/* dataDir/tenant/default/    // (logs share same bundle now)
cp -r dataDir/metrics/* dataDir/tenant/default/ // (metrics share same bundle now)
// Old dirs kept as backup: dataDir/traces_backup_20260501/
```

Log a **warning**: `Auto-migrated legacy data to tenant/default/`. Old directories preserved as `*_backup_{date}/`.

### Post-Migration

- `tenant/default` becomes the only tenant
- All future writes use per-tenant path
- Can later split default tenant using export/import

---

## 9. Write Path Changes

### Current WriteTraces

```go
func (s *block2Store) WriteTraces(ctx, tenantID string, spans) error {
    s.traceBundle.mu.Lock()
    s.traceBundle.wal.AppendBatch(batch)
    s.traceBundle.insert(key, data)
}
```

### New WriteTraces

```go
func (s *block2Store) WriteTraces(ctx, tenantID string, spans) error {
    bundle, err := s.getBundleForTenant(tenantID)
    if err != nil {
        return err  // bundle unhealthy or build failed
    }
    
    bundle.mu.Lock()
    defer bundle.mu.Unlock()
    
    if bundle.state == StateUnhealthy {
        return fmt.Errorf("tenant %s write failed: %w", tenantID, bundle.stateErr)
    }
    
    bundle.wal.AppendBatch(batch)
    for _, data := range batch {
        key, data2 := s.extractTraceSortKey(tenantID, data)  // trusts caller tenantID
        bundle.set.Insert(key, data2)
    }
    bundle.lastWrite = time.Now()
    return nil
}
```

### ReadTraces

```go
func (s *block2Store) ReadTraces(ctx, req TraceReadRequest) error {
    bundle, err := s.getBundleForTenant(req.TenantID)
    if err != nil {
        return err
    }
    
    bundle.mu.RLock()
    defer bundle.mu.RUnlock()
    bundle.lastRead = time.Now()
    
    // Same read logic as before, but bundle segments are single-tenant
    // Gate 1 (tenant pruning) is now 100% effective
}
```

---

## 10. Compaction & TTL

### Per-Tenant Compaction

Compaction operates on a single tenant bundle. The current `compactBundle` logic already iterates `b.man.AllRecords()` for one bundle — no logic change needed.

**But now safe to parallelize:** Multiple tenants can compact simultaneously without cross-tenant locks.

### TTL Sweep

`sweepBundleTTL` already removes old segments. Must also update `b.locator` to remove references to deleted segment sequences. This was TODO #2 — fixed in this revision.

---

## 11. Scope of Changes

### New / Modified Files

| File | Change | Lines (est.) |
|------|--------|--------------|
| `internal/store/block2.go` | Add `tenantBundle`, `BundleState`, `getBundleForTenant`, `evictBundle`, `buildBundle` | ~+200 |
| `internal/store/block2.go` | Modify `WriteTraces/ReadTraces/WriteLogs/ReadLogs/WriteMetrics/ReadMetrics` to use `getBundleForTenant` | ~+100, ~-50 |
| `internal/store/block2.go` | Add auto-migration logic in `NewBlock2StoreWithConfig` | ~+50 |
| `internal/store/block2.go` | Add lifecycle monitor goroutine (Active→Parked→Cold, Unhealthy retry) | ~+100 |
| `internal/store/block2.go` | `traceKeyMatches` — strict tenant enforcement (already done in pre-fix) | ~+15 |
| `internal/store/store.go` | No interface change | 0 |
| `internal/config/config.go` | Add `ParkTimeout`, `ColdTimeout`, `MaxActiveTenants` | ~+20 |
| `internal/tenant/resolver.go` | Add filesystem sanitization helper | ~+10 |
| `tests/integration/tenant_test.go` | Update path expectations for `tenant/{id}/` layout | ~+30 |
| `internal/store/store_test.go` | Update helpers to pass tenant directories | ~+30 |

### Estimated Effort
- **Code changes:** ~400 lines modified/added
- **Tests:** All existing tests need tenant subdirs. Add lifecycle transition tests.
- **Integration tests:** Same HTTP API — `?tenant=payments` queries unchanged
- **Total time:** 1 full day

### Breaking Changes
- **Directory layout:** Old `dataDir/traces/` auto-migrated to `dataDir/tenant/default/`
- **Store interface:** Unchanged
- **Config file:** Add `tenant.lifecycle` section

---

## 12. Out of Scope (for this ADR)

- Cross-tenant compaction or data sharing (not needed — isolated by design)
- Per-tenant retention/TTL policies (`signalBundle.retention` already configurable)
- Multi-node replication per tenant (future ADR)
- Per-tenant query rate limiting (middleware layer)
- Sharding the tenant map (sync.Map is sufficient for 10K tenants; shard if profiling shows contention)

---

## 13. Appendix: Decisions Made Today

| Decision | Chosen | Rationale |
|----------|--------|-----------|
| **Architecture** | Per-tenant single bundle (all signals) | Fewer files, simpler lifecycle, DType prefix already separates signals |
| **Tenant map** | `sync.Map` | Zero explicit locks, no deadlock possible, scales to 10K tenants |
| **Lifecycle states** | Active, Parked, Cold, Unhealthy | Unhealthy handles transient failures; Cold drops locator for bounded memory |
| **Lock ordering** | sync.Map ops first, bundle.mu second | Prevents AB-BA deadlock; eviction removes from map before locking bundle |
| **Locator memory** | Drop on Cold (rebuild on warmup) | Bounded RAM: only Active + Parked bundles hold locators |
| **Park timeout** | 30s (configurable) | Traces/logs are bursty; metrics may need longer |
| **Cold timeout** | 5min (configurable) | Drop locator after extended idle; balances responsiveness vs. memory |
| **Max active tenants** | 500 (configurable) | Approximate upper bound before LRU eviction |
| **Unhealthy retry** | Exponential backoff: 5→10→30→60s max | Transient errors self-resolve without manual intervention |
| **Migration** | Auto-detect + move on startup | Zero-downtime upgrade; old dirs preserved as backup |
| **WAL sync** | Explicit on delete (already fixed) | Tombstones durable before memtable update |

---

## 14. Next Step

Invoke `writing-plans` skill to create detailed implementation plan.

---

*Revision history:*
- **2026-04-29:** ADR-008 Rev 1 — Initial per-tenant signal bundles design
- **2026-05-01:** ADR-008 Rev 2 — Added Unhealthy state, sync.Map, single bundle, auto-migration, closed critical blockers
