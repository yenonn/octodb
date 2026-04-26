# OctoDB — Write Path & Read Path Review

**Date:** 2026-04-26  
**Status:** Block 2 Solid + Block 3 Read Path + Block 4 Trace Index Foundation  
**Commit:** `2059420`

---

## Write Path: Complete Architecture

### High-Level Flow

```
OTel Collector / testclient
    │
    ▼
gRPC ExportTraceServiceRequest
    │
    ├───▶ WAL.Append(record) ──▶ fsync WAL
    │       └── Length-prefixed protobuf
    │
    ├───▶ Memtable.Insert(key, value)
    │       └── B-tree sorted by (tenant, service, time, span_id)
    │
    └───▶ ACK to client
              └── Durability guaranteed

(Background, triggered by threshold or shutdown)
    │
    ├───▶ Memtable.Rotate()
    │       └── active → immutable flushing memtable
    │
    ├───▶ Segment.Create(sst-00000001.pb)
    │       ├── Write records in sort key order
    │       ├── Block index entries every ~1KB
    │       └── Footer: JSON with blocks[], version, count
    │
    ├───▶ Bloom.Build(keys, 0.01)
    │       └── Save to sst-00000001.bloom
    │
    ├───▶ TraceIndex.Build(trace_id → offsets)
    │       └── Save to sst-00000001.trace.index
    │
    ├───▶ Manifest.AppendRecord()
    │       └── JSON: segment_file, bloom_file, min/max key, WAL checkpoint
    │
    ├───▶ Manifest.WriteCheckpoint()
    │       └── Atomic: wal:offset (append-only manifest + .checkpoint)
    │
    ├───▶ WAL.Truncate()
    │       └── Safe: manifest + checkpoint fsynced first
    │
    └───▶ WAL.Reopen()
              └── Ready for new writes
```

### Per-Record Write Cost

| Step | Cost | What Happens |
|------|------|-------------|
| Proto marshal | ~50-200μs | `proto.Marshal(ResourceSpans)` |
| WAL append + fsync | 1-5ms (SSD) | 2 disk I/O ops (append + fsync) |
| Memtable insert | 1-5μs | B-tree insert + size tracking |
| Total (client-visible) | **1-6ms** | Per batch, not per record |
| BG flush (per segment) | 10-50ms | Sorted write, sync, build indexes |

### Files Created Per Flush

| File | Size | Purpose |
|------|------|---------|
| `sst-00000001.pb` | ~5-15KB | Sorted proto records + JSON footer |
| `sst-00000001.bloom` | 200-600B | Bloom filter for key prefix skip |
| `sst-00000001.trace.index` | 1-3KB | `trace_id → []offset` sidecar |
| `manifest.log` | +~200B/segment | Append-only record metadata |
| `manifest.log.checkpoint` | ~20B | Latest fsynced WAL offset |

---

## Read Path: Complete Architecture

### Two Query Types Implemented

| Query Type | API | What It Does |
------------|-----|------------|
| **Time-range scan** | `GET /v1/traces?tenant=X&service=Y&start=T1&end=T2` | Service + time queries, 5-gate pipeline |
| **Trace ID lookup** | `GET /v1/traces/:trace_id` | Direct trace reconstruction via two-level index |

### Type A: Time-Range Query (5 Gates)

```
Query: tenant=team-A, service=api, start=14:00, end=14:30

Gate 1 — Tenant Isolation
  For each manifest record:
    if rec.min_key.tenant != "team-A" AND rec.max_key.tenant != "team-A"
        → SKIP (entire segment belongs to another tenant)
  Result: 50 segments → 30 segments

Gate 2 — Time Range Overlap
  For remaining segments:
    if end <= rec.min_key.time OR start >= rec.max_key.time
        → SKIP (entire segment outside query window)
  Result: 30 segments → 5 segments

Gate 3 — Service Filter
  For remaining segments:
    Load sst-XXXX.bloom
    if !bloom.MightContain("team-A\x00api\x00...")
        → SKIP (segment does not contain this service)
  Result: 5 segments → 3 segments

Gate 4 — Block Index Seek
  For remaining segments:
    Open sst-XXXX.pb
    Load footer → blocks array
    Binary search: find block where block.key >= "team-A:api:14:00:..."
    SeekTo(block.offset)
  Result: File positioned to ~14:00, no scan before that

Gate 5 — Deserialize + Final Filter
  For records from seek point:
    data = seg.Next()
    rs = proto.Unmarshal(data)
    if rs.time >= end → STOP (past query window)
    if rs.service == "api" → append result
  Result: Only matching records deserialized
```

### Type B: Trace ID Lookup (Two-Level Index)

```
Query: trace_id = "abc123def456"

Layer 1 — Global Locator (in-memory map)
  seqs = locator.Find("abc123def456")
  Result: [segment 3, segment 7]  ← 2 segments contain this trace

Layer 2 — Per-Segment Sidecar
  For seq=3:
    Load sst-00000003.trace.index
    offsets = index.offsets["abc123def456"]
    Result: [byte 1024, byte 5120, byte 8192]

  Open segment 3
  SeekToOffset(1024) → read 1 record → seek to 5120 → read → seek to 8192 → read

Total: 3 records read from 1 segment, 0 scanning, 0 false positives
```

### Cost Comparison

| Query Type | Without Index | With Index | Speedup |
------------|--------------|------------|---------|
| Time-range (1M records, 50 segments) | 1M deserializations | ~500 records | **2000x** |
| Trace ID (1M records, 50 segments) | 1M deserializations | 3 records | **333,000x** |
| Restart (memtable empty) | N/A | Replay WAL → rebuild locator from sidecars | Valid |

---

## What's Implemented vs What's Missing

### ✅ Implemented (In This Commit)

| Component | Where | Evidence |
|-----------|-------|----------|
| WAL write + fsync | `internal/wal/wal.go` | `TestWALRoundTrip`, `TestWALMultipleRecords` |
| WAL replay (crash recovery) | `internal/wal/replay.go` | `TestBlock1_WALCrashRecovery` |
| Dual-memtable rotation | `internal/memtable/memtable.go` | Active + flushing + Snapshot |
| B-tree sort order | `pkg/otelutil/sortkey.go` | `(tenant, service, time, span_id)` |
| Segment writer + footer | `internal/segment/segment.go` | `TestWriteReadRoundTrip` |
| Block index + SeekTo | `internal/segment/segment.go` | Footer.Blocks binary search |
| Manifest append + checkpoint | `internal/manifest/manifest.go` | Atomic write + fsync |
| Bloom filter | `internal/bloom/bloom.go` | `TestBuildAndContains`, `TestSaveLoad` |
| **Trace ID two-level index** | `internal/index/trace_index.go` | `TraceLocator` + `SegmentTraceIndex` |
| HTTP query `/v1/traces` | `cmd/octodb/main.go` | `TestBlock2_FlushAndQuery` returns count=100 |
| Integration tests | `tests/integration/` | Both Block 1 + Block 2 PASS |

### ⚠️ Partial / Needs Verification

| Component | Status | Why |
|-----------|--------|-----|
| Trace ID HTTP endpoint | 🚧 **Not wired** | `Store.ReadTraceByID` exists but `main.go` has no `/v1/traces/:trace_id` route |
| Attribute queries | Not implemented | No inverted index for `http.status_code`, `user.id` |
| Exemplar linking | Not started | No metric → trace bridge |
| Compaction | Not started | Old segments accumulate forever |

### ❌ Not Yet Implemented

| Component | Priority | Complexity |
|-----------|----------|------------|
| Tenant routing | High | Medium — config + attribute resolution |
| mTLS auth | High | Medium — cert OU → tenant mapping |
| Query gRPC endpoint | Medium | Low — wrap ReadTraces in gRPC |
| Metrics ingestion | Medium | Medium — same pipeline, different proto |
| Log ingestion | Low | Medium — same pipeline, different proto |
| Compaction / TTL | Medium | High — merge segments, remove old data |
| Columnar metrics path | Post-Phase 1 | High — Arrow/Parquet side path |

---

## File Inventory (Post-Commit)

```
octodb/
├── cmd/
│   ├── octodb/main.go           # gRPC server + HTTP query API
│   ├── replay_check/main.go      # WAL inspection tool
│   └── testclient/main.go        # OTLP gRPC test sender
├── internal/
│   ├── bloom/
│   │   ├── bloom.go              # Bloom filter implementation
│   │   └── bloom_test.go         # Unit tests
│   ├── config/
│   │   └── config.go             # YAML + env var config
│   ├── index/
│   │   └── trace_index.go        # ⭐ Two-level trace index
│   ├── manifest/
│   │   └── manifest.go           # Append-only manifest log
│   ├── memtable/
│   │   └── memtable.go           # B-tree memtable + dual rotation
│   ├── segment/
│   │   ├── segment.go            # SSTable writer/reader with footer
│   │   └── segment_test.go       # Unit tests
│   ├── server/
│   │   └── grpc.go               # OTLP TraceService handler
│   ├── store/
│   │   ├── store.go              # Store interface (WriteTraces, ReadTraces, ReadTraceByID)
│   │   ├── walstore.go           # Block 1 WAL-only implementation
│   │   └── block2.go             # ⭐ WAL + memtable + flush + trace index
│   └── wal/
│       ├── wal.go                # Length-prefixed WAL writer
│       ├── replay.go             # WAL replay
│       └── wal_test.go           # Unit tests
├── tests/integration/
│   ├── block1_test.go            # Crash recovery test
│   └── block2_test.go            # Flush + query test
├── docs/
│   ├── architecture/ADR.md
│   ├── planning/IDEAS.md + ROADMAP.md
│   ├── reference/*.docx
│   └── reviews/
│       ├── WRITE_PATH_REVIEW.md
│       └── IMPLEMENTATION_PROGRESS.md
├── go.mod, go.sum, Makefile, .gitignore, README.md
```

---

## Commit History

| Commit | Message | What Changed |
|--------|---------|--------------|
| `2059420` | Block 2 solid + Block 3 read path + two-level trace_id index | Complete implementation of write path, read path, and trace index. 36 files, 4,029 insertions. |

---

## Next Decision Points

1. **Wire `/v1/traces/:trace_id`** — expose the two-level index via HTTP (~15 min)
2. **Tenant routing** — dynamic tenant resolution from Resource attributes (~1 hour)
3. **Attribute queries** — inverted index for `http.status_code`, `user.id` (~2-3 hours)
4. **Compaction** — merge old segments, TTL enforcement (~half day)

**Recommendation:** Wire the trace ID endpoint first — it's fast, validates the index works end-to-end, and gives a concrete demo of the two-level index.

---

*Review complete. Ready to proceed.*
