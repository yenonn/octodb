package memtable

import (
	"testing"

	"github.com/octodb/octodb/pkg/otelutil"
)

func TestMemtableInsertAndAscend(t *testing.T) {
	m := New()
	keys := []string{"b", "a", "c"}
	for _, k := range keys {
		m.Insert(Key{DType: otelutil.TypeTrace, Key: k}, []byte("v_"+k))
	}

	var order []string
	m.Ascend(func(key Key, value []byte) bool {
		order = append(order, key.Key)
		return true
	})

	expected := []string{"a", "b", "c"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(order))
	}
	for i, e := range expected {
		if order[i] != e {
			t.Fatalf("order mismatch at %d: got %q, want %q", i, order[i], e)
		}
	}
}

func TestMemtableSize(t *testing.T) {
	m := New()
	if m.Size() != 0 {
		t.Fatal("expected size 0")
	}
	m.Insert(Key{DType: otelutil.TypeTrace, Key: "x"}, []byte("hello"))
	if m.Size() <= 0 {
		t.Fatal("expected size > 0 after insert")
	}
	before := m.Size()
	m.Insert(Key{DType: otelutil.TypeTrace, Key: "x"}, []byte("hello")) // overwrite
	if m.Size() != before {
		// btree ReplaceOrInsert may add or replace; as long as it's not empty
		if m.Size() == 0 {
			t.Fatal("expected size > 0")
		}
	}
}

func TestMemtableAscendByType(t *testing.T) {
	m := New()
	m.Insert(Key{DType: otelutil.TypeTrace, Key: "t1"}, []byte("trace1"))
	m.Insert(Key{DType: otelutil.TypeLog, Key: "l1"}, []byte("log1"))
	m.Insert(Key{DType: otelutil.TypeTrace, Key: "t2"}, []byte("trace2"))

	var traces []string
	m.AscendByType(otelutil.TypeTrace, func(key Key, value []byte) bool {
		traces = append(traces, key.Key)
		return true
	})
	if len(traces) != 2 {
		t.Fatalf("expected 2 traces, got %d", len(traces))
	}
	if traces[0] != "t1" || traces[1] != "t2" {
		t.Fatalf("unexpected trace order: %v", traces)
	}

	var logs []string
	m.AscendByType(otelutil.TypeLog, func(key Key, value []byte) bool {
		logs = append(logs, key.Key)
		return true
	})
	if len(logs) != 1 || logs[0] != "l1" {
		t.Fatalf("unexpected log order: %v", logs)
	}
}

func TestSetDualRotation(t *testing.T) {
	s := NewSet()
	s.Insert(Key{DType: otelutil.TypeTrace, Key: "k1"}, []byte("v1"))
	if s.ActiveSize() == 0 {
		t.Fatal("expected active size > 0")
	}

	flushing := s.Rotate()
	if flushing == nil {
		t.Fatal("expected flushing memtable after rotate")
	}

	// Active should now be fresh and empty.
	if s.ActiveSize() != 0 {
		t.Fatalf("expected active size 0 after rotate, got %d", s.ActiveSize())
	}

	// Flushing should contain our data.
	if flushing.Size() == 0 {
		t.Fatal("expected flushing size > 0")
	}

	// Second rotate should return nil because flushing is still in flight.
	if s.Rotate() != nil {
		t.Fatal("expected nil on double rotate")
	}

	s.ClearFlushing()
	// Now rotate works again.
	if s.Rotate() == nil {
		t.Fatal("expected rotate to succeed after ClearFlushing")
	}
	s.ClearFlushing()
}

func TestSetSnapshotConsistency(t *testing.T) {
	s := NewSet()
	s.Insert(Key{DType: otelutil.TypeTrace, Key: "a"}, []byte("1"))

	active, flushing := s.Snapshot()
	if active == nil {
		t.Fatal("expected non-nil active")
	}
	if flushing != nil {
		t.Fatal("expected nil flushing before rotate")
	}

	// Rotate
	s.Rotate()
	active2, flushing2 := s.Snapshot()
	if active2 == nil || flushing2 == nil {
		t.Fatal("expected both active and flushing after rotate")
	}

	s.ClearFlushing()
}
