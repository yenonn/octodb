# OctoDB Write Path - Known Limitations TODO

**Last Updated:** 2026-04-26

## High Priority

### 1. Trace Index Not Rebuilt on Compaction
- **Location:** `block2.go:1136`
- **Issue:** `compactBundle` creates new merged segment but doesn't rebuild `b.locator` (trace index)
- **Impact:** `ReadTraceByID` may miss traces after compaction
- **Status:** TODO
- **Fix:** Merge trace.index files from old segments

### 2. Locator Not Updated on TTL Sweep
- **Location:** `block2.go:1086-1100`
- **Issue:** `sweepBundleTTL` removes segments but doesn't update `b.locator`
- **Impact:** `ReadTraceByID` fails for expired traces
- **Status:** TODO
- **Fix:** Update locator after removing segments

### 3. Memtable Set.Insert Race
- **Location:** `memtable.go:124-128`
- **Issue:** `Set.Insert` uses `RLock` but performs write operation
- **Impact:** Potential data race (mitigated by bundle lock in callers)
- **Status:** Known - caller holds bundle lock
- **Fix:** Change to `Lock()` or redesign

## Medium Priority

### 4. Double Parse in Write Path
- **Location:** `block2.go:538,553`
- **Issue:** Marshal to bytes, then unmarshal to extract sort key
- **Impact:** 2x CPU for same data
- **Status:** TODO
- **Fix:** Extract sort key before marshal

### 5. Hardcoded "default" Tenant
- **Location:** `block2.go` multiple locations
- **Issue:** `tenantID` parameter accepted but ignored, hardcoded to "default"
- **Impact:** No multi-tenant isolation
- **Status:** TODO
- **Fix:** Use tenantID in sort key

### 6. No Input Size Limits
- **Location:** `block2.go:525-545` (WriteTraces)
- **Issue:** No validation on batch size
- **Impact:** Memory exhaustion from large batches
- **Status:** TODO
- **Fix:** Add MaxBatchSize check

## Low Priority

### 7. Delete Operations Don't Sync WAL
- **Location:** `block2.go:1009,1037,1065`
- **Issue:** Delete uses `wal.Sync()` which is async now, but was blocking
- **Impact:** Deletes may be lost on crash
- **Status:** TODO
- **Fix:** Add explicit sync or accept eventual consistency

### 8. Segment fsync Before Close
- **Location:** `block2.go:1264`
- **Issue:** Segment written but not synced before close
- **Impact:** Data in OS buffer cache may be lost
- **Status:** TODO
- **Fix:** Add fsync before segment Close

### 9. CompactBundle Error Handling
- **Location:** `block2.go:1188`
- **Issue:** Bloom file save error ignored (`_ = bf.Save(...)`)
- **Impact:** Bad bloom file recorded in manifest
- **Status:** TODO
- **Fix:** Check error return

### 10. Trace Index in Compact Not Merged
- **Location:** `block2.go:1136`
- **Issue:** `TODO: rebuild trace index from old segments' trace.index files.`
- **Impact:** Lost trace ID index after compaction
- **Status:** TODO

## Not Issues (Known Design)

- **Single-node only** — Use replication spec for HA
- **Async WAL sync** — By design for throughput
- **Memtable size race** — Protected by bundle lock

## Testing Coverage Gaps

1. **Crash during compaction** — Not tested
2. **Concurrent delete + flush** — Basic test only
3. **TTL sweep + ReadTraceByID** — Not integrated
4. **Large batch (>1000)** — Not tested
5. **Multiple tenants** — Not implemented