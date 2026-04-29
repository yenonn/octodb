# ADR-008: Per-Tenant Signal Bundles with Lifecycle Management

**Status:** Approved  
**Date:** 2026-04-29  
**Driver:** Need strict tenant isolation in multi-tenant SaaS platform (1K–10K tenants)  
**Updates:** ADR-003 (Tenant Resolution), ADR-002 (Block 2 Storage)  

---

## 1. Context & Problem Statement

The current OctoDB architecture uses **one shared signalBundle per signal** — traces, logs, and metrics each have:
- A single WAL file
- A single dual-memtable (`memtable.Set`)
- A single manifest
- A single segment directory

All tenants' data is interleaved into the same sorted stream. This caused known limitations documented in `docs/superpowers/specs/TODO-write-path-limitations.md` (items #5, #8, #10).

### Sub-issues discovered today
1. **traceKeyMatches** has a TODO at line 676: "tenant check not strictly enforced." Mixed-tenant segments (when multiple tenants' data flushes together) can in theory leak records across tenant queries.
2. **WAL isolation:** A bad WAL record, corruption, or flush crash affects all tenants.
3. **Backup:** No way to back up or restore a single tenant without copying all data.
4. **Memory/FD pressure:** Unbounded growth — as tenant count scales, the single memtable and WAL don't naturally isolate resource consumption.

---

## 2. Decision: Per-Tenant Signal Bundle with Lifecycle Management (Approach C)

Each tenant gets their own physical `signalBundle` per signal (traces, logs, metrics). Bundles transition through three lifecycle states to keep memory and file descriptors bounded.

---

## 3. Directory Layout

```
dataDir/
  tenant/
    {tenant-id}/
      traces/
        000001.wal
        manifest.log
        manifest.log.checkpoint
        sst-00000001.pb
        sst-00000001.pb.trace.index
        sst-00000001.bloom
      logs/
        ... (same as traces)
      metrics/
        ... (same as traces)
```

`{tenant-id}` is the resolved tenant string from `tenant.Resolver`. Must be filesystem-safe (already enforced by our resolver; we should sanitize `tenant_id` for `os.MkdirAll`).

---

## 4. Lifecycle States

| State | WAL | Memtable | Manifest in RAM | File Descriptors |
|-------|-----|----------|-----------------|------------------|
| **Active** | Open, appending | Full dual-memtable (`memtable.Set`) | Yes | 1 (WAL) + memtable |
| **Parked** | Closed, flushed to segment | Empty (flushed to segment) | Yes (in-memory locator) | 0 (no open files) |
| **Cold** | Closed, flushed to segment | Empty | Manifest loaded on first read | Only segments opened per-query |

### Transitions

```
           ┌────────────┐
      write│            │ idle timeout
   ┌──────►│   PARKED   ├───────►┌────────┐
   │      │            │        │  COLD  │
   │      └────────────┘        └────────┘
   │            ▲                     │
   │            │ write                 │ read (lazy warmup)
   │            │                      │
   │     ┌──────┴──────┐               │
   │     │   ACTIVE    │◄──────────────┘
   └─────┤             │
         │ WAL open    │
         │ memtable hot│
         └─────────────┘
```

### Triggers

| Transition | Condition | Action |
|------------|-----------|--------|
| Cold → Active | `WriteTraces("tenantA", ...)` called | Create bundle (open WAL, create memtable, rebuild locator from existing segments) |
| Parked → Active | `WriteTraces("tenantA", ...)` called | Reopen WAL in append mode, memtable stays empty (data already in segment) |
| Active → Parked | `time.Since(b.lastWrite) > ParkTimeout` (default 30s) | Flush memtable → segment → fsync → WAL tombstone → close WAL |
| Active → Cold | Explicit admin call or aggressive eviction | Same as Parked, but manifest dropped from RAM |
| Parked ↔ Cold | LRU cache eviction when max active tenant count hit | Drop manifest/locator from RAM; reload from disk on next read |

---

## 5. Write Path Changes

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
    bundle := s.getBundleForTenant("traces", tenantID)  // resolves lifecycle
    bundle.mu.Lock()
    defer bundle.mu.Unlock()
    bundle.wal.AppendBatch(batch)
    for _, data := range batch {
        key, data2 := s.extractTraceSortKey(data)
        bundle.insert(key, data2)
    }
    bundle.lastWrite = time.Now()
    return nil
}
```

`getBundleForTenant`:
1. Checks `tenantMap map[string]*signalBundle` (sync.Map or RWMutex-protected map)
2. If found + Active or Parked → return
3. If Cold or not found → create from disk (open WAL, rebuild locator, create memtable)
4. Track creation in `tenantMap`

---

## 6. Read Path Changes

### ReadTraces
```go
func (s *block2Store) ReadTraces(ctx, req TraceReadRequest) error {
    bundle := s.getBundleForTenant("traces", req.TenantID)
    // Now bundle.MinKey / MaxKey cover ONLY this tenant
    // Segment pruning is 100% effective — no mixed segments exist
    // traceKeyMatches no longer needs tenant enforcement (but we add it anyway for safety)
}
```

---

## 7. Flush Changes

### Current flushBundle
Flushes everything in the dual-memtable into a single segment, regardless of tenant.

### New flushBundle
Flushes one tenant's memtable into that tenant's segment directory. Since the memtable only contains one tenant's data, `minKey.TenantID == maxKey.TenantID == tenantID`.

After flush:
1. WAL rotated → next WAL opened
2. `manifest.log.checkpoint` written
3. Bundle stays Active (writes continue) or transitions to Parked (idle)

---

## 8. Compaction Changes

Compaction operates on a single tenant's manifest and segments. The current `compactBundle` already iterates on `b.man.AllRecords()` for one bundle. No logic change needed — but it's now safe to run compaction per-tenant in parallel without cross-tenant locks.

---

## 9. Backup, Restore, Migration

### Backup a single tenant
```bash
# Hot tenant — copy open WAL + segments
rsync -a dataDir/tenant/{tid}/traces/000001.wal /backup/traces/
rsync -a dataDir/tenant/{tid}/traces/sst-*.pb /backup/traces/
# On restore: replay WAL → tenant back online
```

### Restore a single tenant
```bash
# Zero impact on other tenants
mkdir -p dataDir/tenant/{tid}/traces
cp tenant-backup/* dataDir/tenant/{tid}/traces/
# OctoDB rebuilds locator on next WriteTraces or ReadTraces call
```

### Export / Migrate a tenant
```bash
tar -czf tenant-{tid}-export.tar.gz dataDir/tenant/{tid}/
# On destination node: extract, bundle warms up on first access
```

---

## 10. Error Isolation

| Failure | Surface Area |
|---------|------------|
| WAL corruption for tenant A | Only tenant A's reads/writes fail. Other tenants unaffected. |
| Memtable OOM for tenant A | Only tenant A's bundle marked unhealthy. Lifecycle manager can force-park. |
| Segment file corruption for tenant A | Only tenant A's affected segment returns error. Locator skip works for other segments. |
| Manifest append failure for tenant A | Writes to tenant A fail. Other tenants continue. |

---

## 11. Scope of Changes Required

### New / Modified Files

| File | Change | Lines (est.) |
|------|--------|--------------|
| `internal/store/block2.go` | Major: replace `traceBundle`/`logBundle`/`metricBundle` fields with `tenantBundleMap map[string]*signalBundle`. Modify `WriteTraces`, `ReadTraces`, etc. | ~+150, ~-50 |
| `internal/store/block2.go` | Add `getBundleForTenant`, `createTenantBundle`, `parkBundle`, lifecycle monitor | ~+200 |
| `internal/store/block2.go` | `flushBundle` — no logic change, but operates on per-tenant bundle | ~0 |
| `internal/store/block2.go` | `compactBundle` — no logic change, per-tenant already | ~0 |
| `internal/store/block2.go` | `createBundle` — add `tenantID` param, create tenant subdirectory | ~+20 |
| `internal/store/block2.go` | `NewBlock2StoreWithConfig` — remove global bundle creation; build map lazily | ~-40 |
| `internal/store/block2.go` | `traceKeyMatches` — add strict tenant enforcement (closes TODO #676) | ~+15 |
| `internal/store/store.go` | No change to interface — same `WriteTraces(ctx, tenantID, spans)` | 0 |
| `cmd/octodb/main.go` | No change — resolver already groups by tenant | 0 |
| `internal/server/grpc.go` | No change — already calls `WriteTraces(ctx, tenantID, spans)` | 0 |
| `internal/config/config.go` | Add `ParkTimeout` and `MaxActiveTenants` config fields | ~+20 |
| `internal/tenant/resolver.go` | Sanitize tenant IDs for filesystem safety | ~+10 |
| `tests/integration/tenant_test.go` | Already tests multi-tenant — adjust path expectations | ~+20 |
| `internal/store/store_test.go` | Update all test helpers to pass tenant directories | ~+30 |

### Estimated Effort
- **Code changes:** ~500 lines modified/added
- **Tests:** All existing tests need tenant subdirs. Add lifecycle transition tests.
- **Integration tests:** No change to HTTP API — same `?tenant=payments` queries
- **Total time:** 1 full day (today we spec, tomorrow we implement)

### Breaking Changes
- **Directory layout:** Old data under `dataDir/traces/` won't be readable. Migration needed:
  ```bash
  # One-time migration script: move old shared segments into default tenant
  mkdir -p dataDir/tenant/default/traces
  mv dataDir/traces/* dataDir/tenant/default/traces/
  ```
- **Store interface:** Unchanged. gRPC server unaffected.
- **Config file:** Add `tenant` section (lifecycle timeouts + limits).

---

## 12. Out of Scope (for this ADR)

- Cross-tenant compaction or data sharing (not needed — tenants are isolated by design)
- Per-tenant retention/TTL policies (covered by `signalBundle.retention`, already configurable)
- Multi-node replication per tenant (future ADR)
- Per-tenant query rate limiting (middleware layer, not store layer)

---

## 13. Appendix: Summary of Decisions Made Today

| Decision | Chosen | Rationale |
|----------|--------|-----------|
| Architecture | Approach C (per-tenant bundle + lifecycle) | Scales to 1K–10K tenants; memory bounded |
| Directory nesting | `dataDir/tenant/{id}/{signal}/` | Tenant migration / backup is one directory |
| Lifecycle states | Active → Parked → Cold | Limits RAM and file descriptors |
| Park timeout | 30 seconds (configurable) | Balances responsiveness vs. resource pressure |
| Max active tenants | 500 (configurable) | Approximate upper bound before parking kicks in |
| Strict tenant check | Add to `traceKeyMatches` | Safety net even though segments are single-tenant |
| WAL rotation | Same logic, per-tenant | Proven, no new code needed |
| Backup strategy | Per-tenant `rsync` or `tar` | Segments are immutable; WAL is the only mutable file |

---

*Next step: Invoke `writing-plans` skill to create detailed implementation plan for tomorrow.*
