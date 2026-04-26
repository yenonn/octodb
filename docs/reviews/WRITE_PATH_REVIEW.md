# OctoDB — Write Path Review & Concrete Design Fixes

**Date:** 2026-04-26  
**Status:** Design refinement — for Phase 1 Go prototype  
**Scope:** Manifest format, memtable rotation, WAL safety, and LSM architecture gaps identified in existing documentation.

---

## 1. Honest Critique of Current Write-Path Design

The documents describe an LSM-tree-like pipeline: `WAL → Memtable → Flush → SSTable Segment`.
This is a **sound default** for telemetry workloads, but the current spec is **unsafe and incomplete** — it copies Cassandra's happy-path diagram without copying the engineering details that make it durable.

### 1.1 Gaps Identified

| Issue | Severity | Why It Matters |
|-------|----------|----------------|
| **Unsorted memtable** (`Go map`) | High | Flushing a hash map to a sorted segment requires an O(N log N) sort at flush time, creating a latency spike. LSM memtables must stay sorted (B-tree / skip-list). |
| **No memtable rotation** | High | If you flush the active write buffer, you block all ingestion during flush or race and lose data. You need a rotating pair: active (writable) + immutable (flushing). |
| **WAL truncation without manifest** | Critical | Truncating the WAL after "successful flush" is a data-loss footgun. If you crash between flush completion and truncation, replay cannot distinguish "already in segment" from "not yet in segment." |
| **No compaction strategy** | Medium | "Pure merge sort" is a single pass, not a policy. Telemetry ages out. You need time-window compaction or similar, or segments accumulate forever. |
| **No secondary index for trace_id** | High | The sort key is `(tenant, service, time)`, but trace reconstruction is by `trace_id`. Without a reverse index, you scan all segments for a tenant/time range. |
| **Metrics forced through row-oriented path** | Medium | Time-series are naturally columnar. Forcing metrics through the same row-oriented LSM as traces means underperforming against columnar competitors (ClickHouse, VictoriaMetrics). Consider a side path for metrics. |

### 1.2 Go ↔ Rust FFI Boundary Concern

The plan puts the FFI split at the segment read/write interface. This is the **most painful location** because segment formats involve structs, lifetimes, and memory-mapped bytes — all Rust-native concepts. Crossing FFI for every segment scan adds serialization overhead at the hottest path.

**Recommendation:** Reconsider the split. Go should own gRPC + query orchestration + ACL. Rust should own everything below the query plan. The segment iterator should live entirely in Rust, returning serialized rows to Go.

---

## 2. Manifest File Format (Concrete Design)

Use a **write-ahead manifest log** — lighter than RocksDB's full MANIFEST but same safety properties.

### Directory Layout

```
/octodb-data/
  wal/
    000001.wal
    000002.wal
  manifest/
    manifest.log             # append-only: every line is one flush event
    manifest.log.checkpoint  # last successfully processed WAL offset
  segments/
    sst-00000001.pb
    sst-00000002.pb
    sst-00000001.bloom
```

### Manifest Record (one JSON line per flush)

```json
{
  "sequence": 7,
  "segment_file": "sst-00000007.pb",
  "bloom_file": "sst-00000007.bloom",
  "wal_checkpoint": "000002.wal:1847294",
  "min_key": {
    "tenant": "team-payments",
    "service": "checkout",
    "time": 1714120000000000000
  },
  "max_key": {
    "tenant": "team-payments",
    "service": "checkout",
    "time": 1714123600000000000
  },
  "span_count": 150000,
  "checksum": "sha256:abc123..."
}
```

**Why this works:**
- **Append-only** = crash-safe. A partial last line is ignored during replay.
- **Sequence number** monotonic = detects gaps if corruption truncates the file.
- **`wal_checkpoint`** = `filename:offset`. After replaying the manifest, you know exactly where to start WAL replay. No ambiguity, no duplicates.
- **min/max key in JSON** = Gate 2 (time-range seek) and Gate 3 (segment pruning) read only the manifest, never touching segment bytes.

### Write Sequence (Atomic Steps)

1. Flush memtable → `sst-00000007.pb`
2. `fsync(sst-00000007.pb)`
3. Write Bloom filter → `sst-00000007.bloom`
4. `fsync(bloom file)`
5. Append manifest record to `manifest.log`
6. `fsync(manifest.log)`
7. Write `manifest.log.checkpoint` with `000002.wal:1847294`
8. `fsync(checkpoint file)`
9. `fsync(data directory)` (on Linux: `fdatasync` on directory fd)
10. **Now safe:** truncate WAL to offset 1847294

**Crash safety between Step 5 and Step 8:** On next startup, the old checkpoint is read. WAL is replayed from the old offset. The extra segment file from Step 1 was never committed to the manifest, so reads ignore it. You may leak the file; a background cleaner can garbage-collect unreferenced `.pb` files on startup.

---

## 3. Memtable Rotation Sequence (Concrete Design)

Do **not** use a `map` for the memtable. Use a sorted structure.

### Recommended Memtable Type in Go

```go
import (
    "sync"
    "github.com/google/btree"
)

type SortKey struct {
    TenantID   string
    Service    string
    TimeNano   uint64
    SpanID     string
}

type Memtable struct {
    data  *btree.BTree  // sorted in-memory tree
    size  int64         // estimated bytes
    mu    sync.RWMutex
}
```

> Library suggestion: `github.com/google/btree` — pure Go, well-tested.

### Dual-Memtable Structure

```go
type MemtableSet struct {
    active   *Memtable      // ingestion writes here
    flushing *Memtable      // locked for background flush; nil when idle
    mu       sync.RWMutex   // guards active/flushing swap
}
```

### Ingestion Path (Writer Goroutine)

```
1. Acquire MemtableSet.mu RLock
2. btree.Insert(key, value) into active
3. Add key+value size to active.size
4. RUnlock

5. If active.size >= threshold (e.g., 64 MB):
     a. Upgrade to WLock
     b. If active.size still >= threshold (another writer may have beaten you):
          flushing = active
          active = new empty Memtable
          spawn background goroutine: doFlush(flushing)
     c. WUnlock
6. Continue
```

### Flusher Goroutine (Background)

```
1. Serialize flushing.data in-order to segment file (already sorted!)
2. Build Bloom filter from keys while serializing
3. fsync(segment) + fsync(bloom)
4. Append record to manifest.log, fsync manifest
5. Write checkpoint file, fsync checkpoint
6. Truncate WAL up to checkpointed offset
7. Set MemtableSet.flushing = nil, free memory
```

### Query Path During Flush

The read path must consult **both** memtables simultaneously:

```go
func (db *DB) Query(ctx, req) {
    // Snapshot both under RLock
    db.memtableSet.mu.RLock()
    activeSnapshot   := db.memtableSet.active
    flushingSnapshot := db.memtableSet.flushing
    db.memtableSet.mu.RUnlock()

    // Spin off goroutine for active memtable scan
    // Spin off goroutine for flushing memtable scan (if non-nil)
    // Spin off goroutine for segment file scans (via manifest)
    // Merge results on channel → stream to caller
}
```

This mirrors RocksDB: reads search active memtable, immutable queue, and segments concurrently.

---

## 4. WAL Replay on Startup (Now Manifest-Aware)

```
1. Read manifest.log.checkpoint → last known wal_checkpoint = "000002.wal:1847294"
2. Read all manifest records ≥ checkpoint sequence → build segment index in memory
3. Open WAL file 000002.wal, seek to offset 1847294
4. Replay remaining length-prefixed records:
     proto.Unmarshal(ResourceSpans)
     Insert into active memtable (same as ingestion path)
5. fsync is not needed here — we are recovering in-memory state
6. Start gRPC server, now ready to accept new writes
```

---

## 5. What to Change from the Docx Spec

| Docx says | Should be |
|-----------|-----------|
| *"Go map keyed on (service_name, start_time_unix_nano, span_id)"* | B-tree or skip-list memtable (sorted in-memory) |
| *"After successful flush, WAL is truncated"* | After manifest + checkpoint fsync, then truncate |
| Single memtable | Dual memtable (active + flushing) |
| WAL truncation as implicit | WAL truncation as explicit, offset-driven with manifest |
| Segment sort key assumed sufficient for all queries | Acknowledge need for secondary index (e.g., trace_id) or hybrid storage |
| All signals through same pipeline | Metrics may need columnar side path (Arrow/Parquet) even in prototype |

---

## 6. Risk: The "Postgres is Good Enough" Trap

Postgres with JSONB GIN indexes and CTEs for traces is already a viable observability backend (Grafana supports it natively). There is a real risk that Phase 1 works well enough that the Rust rewrite loses urgency.

**Mitigation:** Define falsification criteria now.

- **Ingestion throughput:** Postgres must sustain >50,000 spans/sec without falling behind.
- **Query latency:** Trace reconstruction by `trace_id` must complete in <200ms for a 7-day retention window.
- **Attribute cardinality:** Postgres JSONB GIN index must not degrade when attribute cardinality exceeds 100,000 distinct keys per tenant.
- **Columnar need:** Aggregating 1M metric datapoints over a 1-hour window must complete in <100ms; if Postgres cannot do this, that justifies a metrics-specific columnar side path.

If none of these are breached, Postgres may genuinely be "good enough" — which is acceptable, but the decision should be explicit and data-driven.

---

*End of write-path review. Next step: implement manifest + dual-memtable rotation in Go prototype, validate with crash-kill tests before proceeding to Block 2.*

---

## Related Documentation

| Document | Location |
|----------|----------|
| Architecture Decisions | [`../architecture/ADR.md`](../architecture/ADR.md) |
| Ideas & Open Questions | [`../planning/IDEAS.md`](../planning/IDEAS.md) |
| Roadmap | [`../planning/ROADMAP.md`](../planning/ROADMAP.md) |
| Project README | [`../../README.md`](../../README.md) |
