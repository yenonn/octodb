package index

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentTraceIndexSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.trace.index")

	idx := &SegmentTraceIndex{
		Offsets: map[string][]int64{
			"abc123": {0, 1024, 2048},
			"def456": {512},
		},
	}
	if err := idx.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := LoadSegmentTraceIndex(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Offsets) != 2 {
		t.Fatalf("expected 2 trace_ids, got %d", len(loaded.Offsets))
	}
	if len(loaded.Offsets["abc123"]) != 3 {
		t.Fatalf("expected 3 offsets for abc123, got %d", len(loaded.Offsets["abc123"]))
	}
	if loaded.Offsets["abc123"][0] != 0 || loaded.Offsets["abc123"][2] != 2048 {
		t.Fatalf("offset mismatch: %v", loaded.Offsets["abc123"])
	}
}

func TestSegmentTraceIndexLoadMissingFile(t *testing.T) {
	_, err := LoadSegmentTraceIndex("/nonexistent/path.idx")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestSegmentTraceIndexLoadCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(path, []byte("not json at all"), 0644)
	_, err := LoadSegmentTraceIndex(path)
	if err == nil {
		t.Fatal("expected error for corrupt json")
	}
}

func TestTraceLocatorAddAndFind(t *testing.T) {
	loc := NewTraceLocator()
	loc.Add("trace-a", 1)
	loc.Add("trace-a", 2)
	loc.Add("trace-b", 3)

	if loc.Size() != 2 {
		t.Fatalf("expected size 2, got %d", loc.Size())
	}

	seqs := loc.Find("trace-a")
	if len(seqs) != 2 {
		t.Fatalf("expected 2 seqs for trace-a, got %d", len(seqs))
	}
	seqs = loc.Find("missing")
	if seqs != nil {
		t.Fatal("expected nil for missing trace_id")
	}
}

func TestTraceLocatorMerge(t *testing.T) {
	loc := NewTraceLocator()
	idx := &SegmentTraceIndex{Offsets: map[string][]int64{
		"t1": {0, 100},
		"t2": {200},
	}}
	loc.Merge(1, idx)
	loc.Merge(2, &SegmentTraceIndex{Offsets: map[string][]int64{
		"t1": {300},
		"t3": {400},
	}})

	if loc.Size() != 3 {
		t.Fatalf("expected size 3, got %d", loc.Size())
	}

	seqs := loc.Find("t1")
	if len(seqs) != 2 {
		t.Fatalf("expected t1 in 2 segments, got %d", len(seqs))
	}
	seqs = loc.Find("t2")
	if len(seqs) != 1 {
		t.Fatalf("expected t2 in 1 segment, got %d", len(seqs))
	}
	seqs = loc.Find("t3")
	if len(seqs) != 1 {
		t.Fatalf("expected t3 in 1 segment, got %d", len(seqs))
	}
}

func TestTraceLocatorConcurrent(t *testing.T) {
	loc := NewTraceLocator()
	// Concurrent adds.
	for i := 0; i < 100; i++ {
		go func(n int) {
			loc.Add("shared", int64(n))
		}(i)
	}
	for i := 0; i < 100; i++ {
		go func() {
			_ = loc.Find("shared")
		}()
	}
	// Just ensure no panic.
	_ = loc.Size()
}

func TestTraceLocatorMergeIdempotent(t *testing.T) {
	loc := NewTraceLocator()
	idx := &SegmentTraceIndex{Offsets: map[string][]int64{
		"t1": {0},
	}}
	loc.Merge(1, idx)
	loc.Merge(1, idx) // same seq again
	seqs := loc.Find("t1")
	if len(seqs) != 1 {
		t.Fatalf("expected 1 unique seq for t1 after merge, got %d", len(seqs))
	}
}

func TestTraceLocatorRemoveAll(t *testing.T) {
	loc := NewTraceLocator()
	loc.Add("trace-a", 1)
	loc.Add("trace-a", 2)
	loc.Add("trace-b", 2)
	loc.Add("trace-c", 3)

	loc.RemoveAll([]int64{1, 2})

	if loc.Size() != 1 {
		t.Fatalf("expected size 1 after remove, got %d", loc.Size())
	}
	seqs := loc.Find("trace-c")
	if len(seqs) != 1 || seqs[0] != 3 {
		t.Fatalf("expected trace-c in seq 3, got %v", seqs)
	}
	if loc.Find("trace-a") != nil {
		t.Fatal("expected trace-a removed")
	}
	if loc.Find("trace-b") != nil {
		t.Fatal("expected trace-b removed")
	}
}

// TestTraceLocatorRemoveAllPartial verifies RemoveAll with some non-existent seqs.
func TestTraceLocatorRemoveAllPartial(t *testing.T) {
	loc := NewTraceLocator()
	loc.Add("x", 10)
	loc.Add("x", 20)

	// Remove seq that doesn't exist among other existing seqs.
	loc.RemoveAll([]int64{99})
	if loc.Size() != 1 {
		t.Fatalf("expected size 1, got %d", loc.Size())
	}

	loc.RemoveAll([]int64{10})
	if loc.Size() != 1 {
		t.Fatalf("expected size 1 after partial remove, got %d", loc.Size())
	}
	seqs := loc.Find("x")
	if len(seqs) != 1 || seqs[0] != 20 {
		t.Fatalf("expected seq 20, got %v", seqs)
	}
}

// ---------------------------------------------------------------------------
// Index Save atomicity tests
// ---------------------------------------------------------------------------

func TestSegmentTraceIndexSaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "atomic.trace.index")

	idx := &SegmentTraceIndex{Offsets: map[string][]int64{
		"aaa": {0, 100},
	}}
	if err := idx.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Verify no temp file left behind.
	tmpPath := path + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("temp file should be removed after atomic save: %s", tmpPath)
	}

	// Verify file exists and is valid JSON.
	loaded, err := LoadSegmentTraceIndex(path)
	if err != nil {
		t.Fatalf("load after save: %v", err)
	}
	if len(loaded.Offsets) != 1 {
		t.Fatalf("expected 1 offset, got %d", len(loaded.Offsets))
	}
}
