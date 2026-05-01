# CONTINUATION STATE — Per-Tenant Bundles (2026-05-01)

**For any agent resuming this work:** This document is a brain-dump of the entire working session so you can resume without losing context or chain-of-thought.

## 0. Executive Summary

We implemented **ADR-008 Rev 2: Per-Tenant Signal Bundles** — refactored `block2Store` from 3 shared signal bundles to **one bundle per tenant**, with 4-state lifecycle (Active/Parked/Cold/Unhealthy), using `sync.Map` for deadlock-free tenant lookup. All store + integration tests pass. Coverage is 68.3%, short of 80% target.

## 1. What Was Done (Commit History — Latest First)

```
747edef test(store): add coverage tests for low-covered functions
        -> Added 18 coverage-focused tests (insert, lazyInit, getBundleForTenant error, ReadTraceByID_NotFound, time-range filters, createBundle, WAL replay, minDur, activateTenantBundle_ColdBundle, flushTenantBundle_Empty, listWALFiles, walSeqFromName, BundleState.String, DeleteTraces/Logs/Metrics)

3f307b4 chore: ignore benchmark_reports and .worktrees directories
0ce05b9 chore: ignore benchmark_reports directory

e3d5833 test(store): add comprehensive tenant bundle tests
        -> Added 8 tests: lifecycle transitions, multi-tenant, concurrent, flush, crash recovery

b8c82e6 fix(store): clean up merge conflict markers in traceKeyMatches
        -> Removed "<<<<<<< HEAD" / "=====" / ">>>>>>>" artifacts from block2.go

8fd8cf8 fix(tests): resolve merge conflicts in integration tests
        -> Updated dataDir paths from shared to per-tenant

54871bb feat(store): per-tenant bundles — core implementation
38d25a2 feat(store): per-tenant bundles — core implementation + tests
cb5d470 feat(store): per-tenant bundles implementation
044cfb0 feat(store): add tenantBundle, getBundleForTenant, and buildBundle
        -> Core implementation across 4 commits
```

## 2. Architecture Decisions (Don't Undo These)

### 2.1 Single Bundle Per Tenant (Not 3)
- All 3 signals (traces, logs, metrics) are stored in **one `tenantBundle`** per tenant
- Separation is logical (DType prefix), not physical (3 separate bundles)
- Reason: fewer WAL files, fewer goroutines, less disk pressure for 10K tenants

### 2.2 sync.Map Instead of map+RWMutex
- `tenantMap` is `sync.Map` — zero explicit locks for lookup
- Deadlock between `parkBundle` and `evictBundle` is **impossible**
- Reason: previous `map[string]*tenantBundle+mu.RWMutex` caused deadlocks

### 2.3 Unhealthy State
- Build failure → Unhealthy → retry after 5s exponential backoff
- `getBundleForTenant` returns error for Unhealthy bundles until backoff expires
- Reason: transient disk failures shouldn't crash the entire store

### 2.4 Cold State Drops Locator
- Cold bundle has `locator=nil`, `set=nil` — bounded RAM
- Activate restores from disk on demand
- Reason: support 10K tenants without OOM

### 2.5 Auto-Migration
- Startup checks for legacy layout (`dataDir/traces/`, `dataDir/logs/`)
- If found, creates `default` tenant bundle and WAL
- `autoMigrateLegacy` is always safe to run (idempotent)

## 3. Current Test Status

```
$ go test ./internal/store/  # PASS (6.8s, 68.3% coverage)
$ go test ./tests/integration/  # PASS (36.4s, 7 tests)
```

### 3.1 Coverage Breakdown (Functions Below 80%)

| Function | File:Line | Coverage | Why Low |
|----------|-----------|----------|---------|
| `lifecycleManager` | block2.go:399 | 14.3% | Hard-coded 10s ticker, never fires in unit tests |
| `sweepBundleTTL` | block2.go:1925 | 8.3% | Triggered only by lifecycleManager |
| `compactBundle` | block2.go:2026 | 6.4% | Triggered only by lifecycleManager |
| `ttlAndCompactionSweep` | block2.go:1745 | 47.1% | Calls sweepBundleTTL/compactBundle |
| `getBundleForTenant` | block2.go:217 | 59.3% | Unhealthy build-failure path hard to trigger |
| `NewBlock2StoreWithConfig` | block2.go:661 | 86.5% | (Acceptable) |
| `extractTraceSortKey` | block2.go:900 | 83.3% | (Acceptable) |

## 4. Tomorrow's Tasks (Priority Order)

### Task A: Add `LifecycleInterval` Config → Test Background Goroutines

**Goal:** Hit 80% coverage.

**Where to change:**
1. **File:** `internal/config/config.go`
   - Add `LifecycleInterval time.Duration` to `Config` struct (default: 10s)
   - Add env override in `applyEnv()`

2. **File:** `internal/store/block2.go:399`
   - Change `time.NewTicker(10 * time.Second)` → `time.NewTicker(s.config.LifecycleInterval)`
   - `block2Store` needs reference to config (if not already present)
   - Search: `lifecycleManager` has signature `func (s *block2Store) lifecycleManager()`
   - `s.compactInterval` already exists at line 79, so config plumbing may already exist

3. **File:** `internal/store/coverage_test.go` (or new test file)
   - Add test: `TestLifecycleManagerRuns` with `LifecycleInterval: 1ms`
   - Create store, write to tenant, wait 50ms, assert lifecycle ran (check `s.tenantMap` for state changes)
   - Add test: `TestSweepBundleTTL` — create bundle with old `lastRead`, wait for TTL sweep
   - Add test: `TestCompactBundle` — write enough data to trigger compaction threshold

**Expected coverage lift:** 14.3% + 8.3% + 6.4% + 47.1% → ~80% combined

### Task B: Fix `getBundleForTenant` Unhealthy Path

**Goal:** Cover the `buildBundle` failure → Unhealthy transition.

**Where to change:**
1. **File:** `internal/store/block2.go:240`
   - Currently: `if err := s.buildBundle(b); err != nil { ... }`
   - Need to inject a controllable failure

**Options (pick one):**
- **Option B1 (Recommended):** Add test-only build hook
  - Add `func buildBundleHook func(*tenantBundle) error` to `block2Store` (default nil)
  - In `buildBundle`, if `s.buildBundleHook != nil`, call it and optionally fail
  - In test, set `s.buildBundleHook = func(b *tenantBundle) error { return errors.New("injected") }`
  - Verify: returns error, bundle stored as Unhealthy, `retryAfter` set, second call returns same error within 5s

- **Option B2:** Mock filesystem (more invasive)

### Task C: Fix `benchmark.sh` Parser Bug

**Goal:** Eliminate false "falsification" warnings.

**Where to change:**
1. **File:** `benchmark.sh` (exact line to find)
   - Search for awk extraction of benchmark throughput
   - Likely around line 100-130 (see `benchmark.sh` for exact awk command)
   - Fix: change field extraction from `$1` (benchmark name) to `$3` (throughput number)

**Verification:**
```bash
./benchmark.sh --quick
# Should show real throughput numbers, no "falsification" warnings
```

### Task D: Deferred — WAL Replay Offset Checkpoint

**File:** `internal/wal/replay.go:90`
```go
// TODO(gate-0): Replay from a specific offset (requires manifest checkpoint).
```
**Status:** Not in scope unless explicitly requested.

## 5. Key Implementation Details (In Your Head)

### 5.1 Lock Hierarchy
```
DO NOT acquire bundle.mu while holding store.mu
DO NOT acquire multiple bundle.mu locks simultaneously
ALWAYS use sync.Map for tenantMap (never raw map access)
```

### 5.2 `getBundleForTenant` Flow
```
1. Check sync.Map (no locks)
2. Create bundle struct (Cold state)
3. Call buildBundle() -> may fail -> Unhealthy
4. Store in sync.Map (if new)
5. Call activateTenantBundle() -> Cold->Active (loads locator)
6. Return bundle
```

### 5.3 `activateTenantBundle` States
```
Cold  -> Active (loads locator from disk)
Parked -> Active (re-open memtable)
Active -> Active (no-op)
Unhealthy -> Active (only if retryAfter expired, then rebuild)
```

### 5.4 `flushTenantBundle` Guard
```go
if b.set == nil {
    return nil // Unhealthy or Cold bundle, nothing to flush
}
```
This MUST stay — without it, flushing a Cold/Unhealthy bundle panics on nil set.

### 5.5 `traceKeyMatches` Tenant Check Removal
- `traceKeyMatches` NO LONGER checks `span.Attributes` for tenant attributes
- Tenant isolation is **physical** (per-bundle), not **logical** (per-key)
- `extractTraceSortKey(tenantID, raw)` embeds caller-provided tenantID in sort key
- This was a deliberate simplification — don't re-add tenant checks to traceKeyMatches

## 6. Test Patterns Already Established

### 6.1 Coverage Test Pattern
```go
func TestCoverage_FunctionName_Scenario(t *testing.T) {
    s, cleanup := newTestStoreWithConfig(t, "lifecycle-1ms", &Block2Config{...})
    defer cleanup()
    // ... exercise the function ...
}
```

### 6.2 Multi-Tenant Test Pattern
```go
func TestMultiTenant_ConcurrentWrites(t *testing.T) {
    s, cleanup := newTestStoreWithConfig(t, "multi", nil)
    defer cleanup()
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) { ... write to tenant-001, tenant-002, ... }(i)
    }
    wg.Wait()
}
```

### 6.3 Integration Test Data Paths
```
Before: dataDir/traces/  ->  After: dataDir/tenant/default/
Before: dataDir/logs/    ->  After: dataDir/tenant/default/logs/
Before: dataDir/metrics/ ->  After: dataDir/tenant/default/metrics/
Before: dataDir/wal/     ->  After: dataDir/tenant/default/wal/
```

## 7. Risk: What NOT to Do Tomorrow

1. **Don't add a second bundle per tenant** — single bundle is correct (ADR-008 Rev 2)
2. **Don't switch back to map+RWMutex** — sync.Map prevents deadlocks
3. **Don't re-add tenant checks to `traceKeyMatches`** — tenant isolation is physical now
4. **Don't change `autoMigrateLegacy` semantics** — must always create `default` tenant
5. **Don't remove the `b.set == nil` guard in `flushTenantBundle`** — prevents panic on Cold bundles
6. **Don't use `time.Sleep` in tests > 100ms** — make ticker interval configurable instead

## 8. How to Resume

When you start tomorrow:

```bash
# 1. Verify state
git status
git log --oneline -5
go test ./internal/store/  # Should be 68.3%

# 2. Read this file (CONTINUATION_STATE.md)

# 3. Pick Task A, B, or C and start
```

## 9. Files at a Glance

| File | Lines | What It Does |
|------|-------|--------------|
| `internal/store/block2.go` | 2,547 | Core store — tenantBundle, lifecycle, getBundleForTenant, buildBundle |
| `internal/store/tenant_bundle_test.go` | ~400 | Integration-style tests for lifecycle |
| `internal/store/coverage_test.go` | ~500 | Coverage-focused unit tests |
| `internal/config/config.go` | 74 | Config struct — needs LifecycleInterval |
| `internal/wal/replay.go` | ~100 | WAL replay — has TODO at line 90 |
| `benchmark.sh` | 183 | Runner — has awk parser bug |
| `tests/integration/block1_test.go` | ~150 | WAL crash recovery |
| `tests/integration/block2_test.go` | ~200 | Flush, query, crash recovery |

---

**Session ended:** 2026-05-01  
**Next expected action:** Add `LifecycleInterval` to `Block2Config` and test background goroutines  
**Confidence:** High — all tests pass, architecture is solid, remaining work is plumbing + testing
