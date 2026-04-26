# WAL Rotation Design — Numbered Multi-File WAL

**Date:** 2026-04-26
**Status:** Approved
**Scope:** Replace single-file WAL truncation with numbered WAL file rotation

---

## Problem

The current write path uses a single WAL file per signal bundle (e.g., `traces.wal`). After flush, the file is truncated to zero and reopened. This works but diverges from the original design spec, which calls for numbered WAL files (`000001.wal`, `000002.wal`). Numbered files provide:

- Filesystem-visible history of write activity
- Simpler crash reasoning (no ambiguity about truncation state)
- Clean separation between "committed to segments" and "pending replay"

## Design

### WAL File Naming

Each signal bundle's directory contains numbered WAL files:

```
traces/
  000001.wal.tombstone    ← flushed, pending cleanup
  000002.wal.tombstone    ← flushed, pending cleanup
  000003.wal              ← active (writes go here)
```

Format: `%06d.wal` (zero-padded, 6 digits). Tombstoned files are renamed with `.tombstone` suffix.

### signalBundle Changes

```go
type signalBundle struct {
    walDir     string         // directory containing WAL files (e.g., "octodb-data/traces")
    walSeq     int64          // current WAL file sequence number
    wal        *wal.Writer    // writer for the active WAL file
    // ... rest unchanged
}
```

The active WAL path is derived: `filepath.Join(walDir, fmt.Sprintf("%06d.wal", walSeq))`.

### Startup Sequence (createBundle)

1. Scan bundle directory for `*.wal` files (excluding `.tombstone`)
2. If none exist, set `walSeq = 1`, create `000001.wal`
3. If files exist, find the highest sequence number, open that file for append
4. Set `walSeq` to that number

### Flush Rotation (flushBundle)

After segment + manifest + checkpoint are fsynced:

1. `b.wal.Close()`
2. `os.Rename(currentWALPath, currentWALPath + ".tombstone")`
3. `b.walSeq++`
4. `b.wal = wal.Open(newWALPath)` — new file, e.g., `000004.wal`
5. Manifest checkpoint records `"000004.wal:0"`

### Replay on Startup (replayBundle)

1. Read manifest checkpoint → `WALPosition{File: "000003.wal", Offset: 4096}`
2. Find all non-tombstoned `*.wal` files with sequence >= checkpoint file's sequence
3. Sort by sequence number ascending
4. For the checkpoint file: replay from the checkpoint offset
5. For all subsequent files: replay from offset 0
6. Insert each record into memtable

### Tombstone Cleanup

In the existing `ttlAndCompactionSweep` goroutine, after TTL and compaction:

1. For each signal bundle, `glob(walDir + "/*.wal.tombstone")`
2. Delete all matches

No age check needed — a `.tombstone` file means the checkpoint has already advanced past it.

### Crash Safety

**Crash before rename:** WAL file is intact. On restart, replay finds it as the active WAL and replays from the checkpoint offset. No data loss.

**Crash after rename but before new WAL open:** No active WAL file exists. On restart, `createBundle` finds no `*.wal` files (only `.tombstone`), creates `000001.wal` (or next sequence). The checkpoint already points past all tombstoned data, so replay is a no-op. No data loss.

**Crash during replay:** Replay is idempotent — re-inserting the same records into the memtable produces the same result. Safe to re-run.

## Files Changed

| File | Change |
|------|--------|
| `internal/store/block2.go` | `signalBundle` struct: replace `walPath` with `walDir` + `walSeq`. Update `createBundle`, `replayBundle`, `flushBundle`. Add WAL tombstone cleanup to `ttlAndCompactionSweep`. |
| `internal/store/store_test.go` | New tests: WAL rotation after flush, multi-file replay, tombstone cleanup. |
| `internal/wal/wal.go` | No changes — WAL writer is file-agnostic. |
| `internal/wal/replay.go` | No changes — replay functions already accept a path and offset. |
| `internal/manifest/manifest.go` | No changes — already stores `filename:offset` checkpoint format. |

## Testing

1. **TestWALRotationAfterFlush** — Write data, trigger flush, verify old WAL renamed to `.tombstone`, new WAL created with incremented sequence.
2. **TestWALMultiFileReplay** — Write across multiple WAL files (flush between), crash (close without flush), restart, verify all un-flushed data replayed from correct WAL + offset.
3. **TestWALTombstoneCleanup** — Create `.tombstone` files, trigger compaction sweep, verify they are deleted.
4. **TestWALStartupNoFiles** — Start with empty directory, verify `000001.wal` created.
5. **TestWALStartupResumeHighest** — Start with `000003.wal` present, verify it is opened for append (not `000001.wal`).
