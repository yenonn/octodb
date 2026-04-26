package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSortKeyFromString(t *testing.T) {
	raw := "tenant1\x00svc1\x00some_extra"
	k := SortKeyFromString(raw)
	if k.TenantID != "tenant1" {
		t.Fatalf("tenant mismatch: got %q", k.TenantID)
	}
	if k.Service != "svc1" {
		t.Fatalf("service mismatch: got %q", k.Service)
	}
	if k.Str != raw {
		t.Fatalf("raw mismatch: got %q", k.Str)
	}
}

func TestManagerAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}

	rec := Record{
		Sequence:      1,
		SegmentFile:   "sst-00000001.pb",
		BloomFile:     "sst-00000001.bloom",
		WALCheckpoint: "000001.wal:1024",
		MinKey:        SortKey{TenantID: "t1", Service: "s1"},
		MaxKey:        SortKey{TenantID: "t1", Service: "s2"},
		SpanCount:     10,
	}
	if err := m.AppendRecord(rec); err != nil {
		t.Fatalf("append: %v", err)
	}

	records := m.AllRecords()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].SegmentFile != "sst-00000001.pb" {
		t.Fatalf("segment mismatch: %q", records[0].SegmentFile)
	}

	// Reopen — should replay.
	m2, err := Open(dir)
	if err != nil {
		t.Fatalf("open manifest2: %v", err)
	}
	records2 := m2.AllRecords()
	if len(records2) != 1 {
		t.Fatalf("expected 1 record after replay, got %d", len(records2))
	}
	if records2[0].Sequence != 1 {
		t.Fatalf("sequence mismatch after replay: %d", records2[0].Sequence)
	}
}

func TestNextSequenceMonotonic(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	for i := 1; i <= 5; i++ {
		rec := Record{
			Sequence:    m.NextSequence(),
			SegmentFile: filepath.Join(dir, "sst.pb"),
		}
		if err := m.AppendRecord(rec); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if got := m.NextSequence(); got != int64(i+1) {
			t.Fatalf("expected next seq %d, got %d", i+1, got)
		}
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	if err := m.WriteCheckpoint("000001.wal", 2048); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	cp, err := m.ReadCheckpoint()
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if cp.File != "000001.wal" {
		t.Fatalf("checkpoint file mismatch: %q", cp.File)
	}
	if cp.Offset != 2048 {
		t.Fatalf("checkpoint offset mismatch: %d", cp.Offset)
	}
}

func TestParseWALCheckpoint(t *testing.T) {
	pos, err := ParseWALCheckpoint("my.wal:12345")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pos.File != "my.wal" || pos.Offset != 12345 {
		t.Fatalf("unexpected values: %+v", pos)
	}
	if _, err := ParseWALCheckpoint("bad"); err == nil {
		t.Fatal("expected error for bad checkpoint")
	}
}

func TestManifestJSONLineFormat(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	_ = m.AppendRecord(Record{Sequence: 1, SegmentFile: "seg1.pb"})
	_ = m.AppendRecord(Record{Sequence: 2, SegmentFile: "seg2.pb"})

	data, err := os.ReadFile(filepath.Join(dir, "manifest.log"))
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	if lines != 2 {
		t.Fatalf("expected 2 JSON lines, got %d", lines)
	}

	// Validate each line is valid JSON.
	start := 0
	for i, b := range data {
		if b == '\n' {
			var rec Record
			if err := json.Unmarshal(data[start:i], &rec); err != nil {
				t.Fatalf("line %d not valid JSON: %v", lines, err)
			}
			start = i + 1
		}
	}
}

func TestRemoveRecords(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	_ = m.AppendRecord(Record{Sequence: 1, SegmentFile: "seg1.pb"})
	_ = m.AppendRecord(Record{Sequence: 2, SegmentFile: "seg2.pb"})
	_ = m.AppendRecord(Record{Sequence: 3, SegmentFile: "seg3.pb"})

	if err := m.RemoveRecords(func(rec Record) bool {
		return rec.Sequence != 2
	}); err != nil {
		t.Fatalf("remove: %v", err)
	}

	records := m.AllRecords()
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	for _, rec := range records {
		if rec.Sequence == 2 {
			t.Fatal("record 2 should have been removed")
		}
	}
}

func TestCompactRecords(t *testing.T) {
	dir := t.TempDir()
	m, _ := Open(dir)
	_ = m.AppendRecord(Record{Sequence: 1, SegmentFile: "seg1.pb"})
	_ = m.AppendRecord(Record{Sequence: 2, SegmentFile: "seg2.pb"})
	_ = m.AppendRecord(Record{Sequence: 3, SegmentFile: "seg3.pb"})

	newRec := Record{Sequence: 10, SegmentFile: "seg10.pb"}
	if err := m.CompactRecords([]int64{1, 2}, []Record{newRec}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	records := m.AllRecords()
	if len(records) != 2 {
		t.Fatalf("expected 2 records (3 old - 2 removed + 1 new), got %d", len(records))
	}
	for _, rec := range records {
		if rec.Sequence == 1 || rec.Sequence == 2 {
			t.Fatal("old compacted records should be removed")
		}
	}
	foundNew := false
	for _, rec := range records {
		if rec.Sequence == 10 {
			foundNew = true
		}
	}
	if !foundNew {
		t.Fatal("expected new compacted record")
	}
}
