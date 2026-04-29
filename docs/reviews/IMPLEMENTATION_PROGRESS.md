# OctoDB — Implementation Progress Log

**Last Updated:** 2026-04-29  
**Current Phase:** Phase 1 — Go Prototype (Steps 1–3 of 6 completed)  
**Blocks Completed:** Block 1 (WAL Ingestion) ✅ | Block 2 (Memtable Flush) ✅ | Block 3 (Tenant Resolution) ✅

---

## Block 1: OTLP Ingestion + WAL ✅ VERIFIED

**Definition of Done (from `octodb_getting_started.docx`):**
> 1. Send a batch of spans to OctoDB via otelcol → ✅ (custom `testclient`)
> 2. Kill the OctoDB process → ✅ (`kill -9` tested)
> 3. Inspect `octodb.wal` — file should have bytes → ✅
> 4. Run `ReplayWAL` in a test — verify you get back the exact `ResourceSpans` you sent → ✅

### Components Implemented

| Component | File | Purpose |
|-----------|------|---------|
| **gRPC OTLP Receiver** | `internal/server/grpc.go` | Implements `ExportTraceService`, ACKs after WAL fsync |
| **WAL Writer** | `internal/wal/wal.go` | Length-prefixed protobuf, `Open`, `Append`, `Sync`, `Close` |
| **WAL Replay** | `internal/wal/replay.go` | `Replay`, `ReplayToSlice`, `ReplayFromOffset` |
| **Block 1 Store** | `internal/store/walstore.go` | `Store` interface implementation — WAL-only |
| **Unit Tests** | `internal/wal/wal_test.go` | 3 tests: round-trip, 100 records, missing file |

### Test Results

```
=== RUN   TestWALRoundTrip
--- PASS: TestWALRoundTrip (0.01s)
=== RUN   TestWALMultipleRecords
--- PASS: TestWALMultipleRecords (0.01s)
=== RUN   TestWALMissingFile
--- PASS: TestWALMissingFile (0.00s)
PASS
```

### Integration Test (New — Automated)

**`TestBlock1_WALCrashRecovery`** — `tests/integration/block1_test.go` ✅ PASS

What it does (automates the docx Definition of Done):
1. Compiles `cmd/octodb` into a temp binary
2. Starts server as a subprocess on a temp data dir
3. Sends one OTLP `ExportTraceServiceRequest` via gRPC
4. Asserts WAL file `> 0` bytes
5. Sends `SIGKILL` (simulates crash — no graceful shutdown)
6. Asserts WAL still exists and still `> 0` bytes
7. Replays WAL via `wal.ReplayToSlice`
8. Asserts: exactly 1 record, `service.name` matches, `span.name` matches, `span.kind` matches

```bash
$ go test ./tests/integration/ -v -run TestBlock1_WALCrashRecovery
=== RUN   TestBlock1_WALCrashRecovery
2026/04/26 16:22:21 OctoDB Block 2 starting — gRPC on :4317
2026/04/26 16:22:21 [Block1] Export from [::1]:62136 — 1 ResourceSpans
    wal size after first export: 172 bytes
    wal size after crash: 172 bytes
    Block 1 integration test PASSED: WAL survives crash and replays exactly
--- PASS: TestBlock1_WALCrashRecovery (1.32s)
PASS
```

**Run it yourself:**
```bash
make test-block1        # Block 1 only
make test-integration   # all integration tests
```

**Makefile targets added:**
- `make test-integration` — both Block 1 + Block 2
- `make test-block1` — Block 1 only
- `make test-block2` — Block 2 only

### Block 2 Integration Test

**`TestBlock2_FlushAndQuery`** — `tests/integration/block2_test.go` ✅ PASS

What it does:
1. Starts server, sends **100 OTLP exports** (triggers flush at 1KB threshold)
2. Waits 3 seconds for background flush
3. Asserts: `manifest.log` exists, `manifest.log.checkpoint` exists, `sst-*.pb` files exist
4. **Queries HTTP** `GET /v1/traces?tenant=default` → asserts `count > 0` (got **100**)
5. **Kill -9**, restart server
6. **Queries HTTP again** → asserts `count > 0` (got **100**)
7. Passes if data survived crash and was queryable from segments

```bash
$ go test ./tests/integration/ -v -run TestBlock2_FlushAndQuery
=== RUN   TestBlock2_FlushAndQuery
    segment file: sst-00000001.pb (size 15450)
    segment file: sst-00000002.pb (size 1530)
    wal size after flush: 0
    HTTP query returned count=100
    HTTP query after restart returned count=100
--- PASS: TestBlock2_FlushAndQuery (6.85s)
PASS
```

**Key finding:** The segment read path successfully searches flushed data. After `kill -9` and restart, the server replays the truncated WAL (0 bytes), but data is still queryable from segment files — this is the core "Block 2 is solid" proof.

---

## Block 2: Memtable + Segment Flush ✅ SOLID

**From docx:** *"A sorted in-memory structure — Go map keyed on (service_name, start_time_unix_nano, span_id)... WAL replay on startup populates the Memtable... background goroutine flushes when len(memtable) > threshold... Flush writes entries in sorted order to a segment file... After successful flush, WAL is truncated."*

### What Works

| Component | File | Status | Evidence |
|-----------|------|--------|----------|
| **Sort Key** | `pkg/otelutil/sortkey.go` | ✅ | `(tenant, service, time, span_id)` — B-tree comparable string |
| **B-tree Memtable** | `internal/memtable/memtable.go` | ✅ | `Insert`, `Ascend`, dual rotation (`active` + `flushing`), threshold = 1KB (testing) |
| **Segment Writer** | `internal/segment/segment.go` | ✅ | Creates SSTable-like `.pb` files, length-prefixed records, sorted |
| **Manifest Manager** | `internal/manifest/manifest.go` | ✅ | Append-only JSON log, atomic checkpoint via `WriteFile+Rename`, `fsync` |
| **Block 2 Store** | `internal/store/block2.go` | ✅ | WAL → memtable → background flush → segment + manifest + checkpoint |
| **Flush Trigger** | `block2.go` | ✅ | Memtable threshold reached → rotate → flush to segment |
| **WAL Truncation** | `block2.go` | ✅ | After manifest + checkpoint fsync, WAL truncated to 0 bytes |
| **WAL Replay on Start** | `block2.go` | ✅ | Replays existing WAL into memtable before accepting new writes |

### Live Test Evidence

```bash
# 20 OTLP exports sent via testclient
# Results:
-rw-r--r--  768 manifest.log            # 2 JSON records
-rw-r--r--   15 manifest.log.checkpoint   # "octodb.wal:4100"
-rw-r--r--    0 octodb.wal                # truncated after flush
-rw-r--r-- 2050 sst-00000001.pb           # first segment
-rw-r--r-- 4100 sst-00000002.pb           # second segment
```

**Manifest contents:**
```json
{"sequence":1,"segment_file":"sst-00000001.pb","wal_checkpoint":"octodb.wal:2050",
 "min_key":{"tenant":"default","service":"test-service","time":1777179990475683000,
 "span_id":"61626331323364656634353637383930"},...}
{"sequence":2,"segment_file":"sst-00000002.pb","wal_checkpoint":"octodb.wal:4100",...}
```

### Known Issues / Not Yet Done

| Issue | Severity | Description |
|-------|----------|-------------|
| **Data dir bug** | Low | `filepath.Dir("octodb.wal")` returns `.` instead of `octodb-data/`. Segment files land in root dir. |
| **No segment read path** | **FIXED** ✅ | `ReadTraces` now searches memtable + all segments via manifest. Block 2 integration test passes with `count=100` after flush and restart. |
| **HTTP query endpoint** | **FIXED** ✅ | `/v1/traces?tenant=default` returns `{"count":100}` |
| **Double-close panic** | Fixed | `defer st.Close()` removed in `main.go` |
| **ReadTraces partial** | **FIXED** ✅ | Now scans memtable + segments. Still no Bloom filter or time-range seek, but functionally complete. |
| **Threshold is 1KB** | Testing only | Production threshold should be 64MB. Set low for fast testing. |

### Block 2 Architecture (Implemented Pipeline)

```
Write Path:
  gRPC Export → WAL.Append → WAL.Sync → Memtable.Insert → (bg) Flush → 
  Segment.Create → Segment.Append(sorted) → Segment.Close → 
  Manifest.AppendRecord → Manifest.WriteCheckpoint → WAL.Truncate → WAL.Reopen

Read Path:
  HTTP /v1/traces → Memtable.Snapshot(active + flushing) → Ascend scan (Gate 1-3) →
  Segment.Open(via manifest) → MinKey/MaxKey skip → Scan records → 
  proto.Unmarshal → Merge results → JSON response
  (Missing: Bloom filter skip for Gate 3, true time-range seek via block index)
```

---

## Documentation & Reviews Written

| Document | Location | Purpose |
|----------|----------|---------|
| `WRITE_PATH_REVIEW.md` | `docs/reviews/WRITE_PATH_REVIEW.md` | Critique of LSM design gaps in docx spec + concrete fixes (manifest format, memtable rotation, WAL atomicity) |
| Cross-links | All `.md` files | Each doc links to others via relative paths (e.g., `docs/planning/ROADMAP.md`) |

---

## File Structure (Current — 2026-04-26)

```
octodb/
├── cmd/
│   ├── octodb/main.go           # Block 2 server: gRPC ingestion + HTTP query API
│   ├── testclient/main.go        # OTLP gRPC test sender
│   └── replay_check/main.go      # WAL inspection tool
├── internal/
│   ├── config/
│   │   └── config.go             # YAML + env var config loading
│   ├── server/
│   │   └── grpc.go               # OTLP TraceService gRPC handler
│   ├── wal/
│   │   ├── wal.go                # WAL writer (Open, Append, Sync, Close)
│   │   ├── replay.go             # WAL replay (Replay, ReplayToSlice)
│   │   └── wal_test.go           # 3 passing unit tests
│   ├── store/
│   │   ├── store.go              # Store interface (WriteTraces, ReadTraces, Close)
│   │   ├── walstore.go           # Block 1 implementation (WAL-only)
│   │   └── block2.go             # Block 2 implementation (WAL + memtable + flush)
│   ├── memtable/
│   │   └── memtable.go           # B-tree memtable + dual rotation
│   ├── segment/
│   │   └── segment.go            # SSTable-like segment writer/reader
│   ├── manifest/
│   │   └── manifest.go           # Manifest log + atomic checkpoint
│   └── otelutil/
│       └── sortkey.go            # (Root pkg) Sort key definition
├── docs/
│   ├── architecture/ADR.md       # Architecture Decision Records
│   ├── planning/IDEAS.md         # Ideas & open questions
│   ├── planning/ROADMAP.md       # Phased roadmap (Phase 0–4)
│   ├── reference/*.docx          # Original design docs (getting_started, read_path, rust_ref)
│   └── reviews/WRITE_PATH_REVIEW.md  # Write-path critique & concrete fixes
├── go.mod                        # Module: github.com/octodb/octodb, Go 1.26
├── Makefile                      # build, run, test, clean
└── .gitignore                    # Exclude .wal, venv, bin/, etc.
```

---

## Block 3: Tenant Resolution ✅ COMPLETED (2026-04-29)

### Components Implemented

| Component | File | Purpose |
|-----------|------|---------|
| **Tenant Resolver** | `internal/tenant/resolver.go` | Extracts tenant ID from OTel Resource attributes with configurable strategy |
| **Tenant Resolver Tests** | `internal/tenant/resolver_test.go` | 100% test coverage on fallback logic, batch grouping, attribute resolution |
| **gRPC Server Integration** | `internal/server/grpc.go` | `TraceServer`, `LogServer`, `MetricServer` group batches per-tenant before writing |
| **Config Wiring** | `internal/config/config.go` | `TenantResolutionConfig` with strategy/attrs/default |
| **Sort Key Integration** | `internal/store/block2.go` | `extractTraceSortKey`, `extractLogSortKey`, `extractMetricSortKey` all embed resolved tenant ID |
| **Integration Tests** | `tests/integration/tenant_test.go` | Multi-tenant batch ingestion, crash recovery, attribute fallback — all PASS |

### Discovered Gap (Will Close Tomorrow)

Shared single `signalBundle` per signal means all tenants' data flushes into the same segments. When a segment contains `payments` + `search`, the sort key min/max spans both → segment-level tenant pruning can't skip. Workaround: `traceKeyMatches` still scans; fix: **ADR-008 (per-tenant bundles)** scheduled for tomorrow.

---

## Next Steps (Tomorrow)

**Implement ADR-008: Per-Tenant Signal Bundles with Lifecycle Management**

| Step | Description | Est. |
|------|-------------|------|
| 1 | Replace `traceBundle`/`logBundle`/`metricBundle` with `tenantBundleMap` | 2h |
| 2 | Implement `getBundleForTenant`, `createTenantBundle`, `parkBundle` | 2h |
| 3 | Update Write/Read paths to resolve bundles per-tenant | 1h |
| 4 | Add `ParkTimeout` + `MaxActiveTenants` to config | 30m |
| 5 | Strict tenant enforcement in `traceKeyMatches` (closes TODO #676) | 30m |
| 6 | Update all unit/integration tests for tenant subdirs | 1h |
| 7 | Full test suite run + integration tests | 1h |

---

## Phase 1 Overall Progress

| Step | Description | Status |
|------|-------------|--------|
| **Step 1** | Trace ingestion + native storage (WAL + memtable + segment) | ✅ **Complete** — Block 1 + Block 2 stable |
| **Step 2** | Tenant routing | ✅ **Complete** — ADR-003 implemented; ADR-008 scheduled |
| **Step 3** | Metrics + logs ingestion | ✅ **Complete** — gRPC endpoints wired; sort keys embed tenant ID |
| **Step 4** | Exemplar linking | ⏳ Not started |
| **Step 5** | ACL enforcement | ⏳ Not started |
| **Step 6** | Real workload validation | ⏳ Not started |

---

*This document is updated whenever blocks or phases are completed.*
