package memtable

import (
	"fmt"
	"testing"

	"github.com/octodb/octodb/pkg/otelutil"
)

// BenchmarkMemtableInsert measures raw B-tree insert throughput.
func BenchmarkMemtableInsert(b *testing.B) {
	m := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Key{DType: otelutil.TypeTrace, Key: fmt.Sprintf("tenant\x00svc\x00%d\x00span-%d", i, i)}
		value := []byte(fmt.Sprintf("value-%d", i))
		m.Insert(key, value)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "inserts/sec")
}

// BenchmarkMemtableInsertBatch measures insert throughput of 100 items.
func BenchmarkMemtableInsertBatch(b *testing.B) {
	batchSize := 100
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := New()
		for j := 0; j < batchSize; j++ {
			key := Key{DType: otelutil.TypeTrace, Key: fmt.Sprintf("tenant\x00svc\x00%d\x00span-%d", i*batchSize+j, j)}
			value := []byte(fmt.Sprintf("value-%d", j))
			m.Insert(key, value)
		}
	}
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "inserts/sec")
}

// BenchmarkMemtableAscend measures full table scan performance.
func BenchmarkMemtableAscend(b *testing.B) {
	m := New()
	for i := 0; i < 10000; i++ {
		key := Key{DType: otelutil.TypeTrace, Key: fmt.Sprintf("tenant\x00svc\x00%d\x00span-%d", i, i)}
		value := []byte(fmt.Sprintf("value-%d", i))
		m.Insert(key, value)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Ascend(func(key Key, value []byte) bool {
			_ = key
			_ = value
			return true
		})
	}
	b.ReportMetric(float64(b.N*10000)/b.Elapsed().Seconds(), "rows/sec")
}

// BenchmarkMemtableSnapshotRead measures read throughput during snapshot.
func BenchmarkMemtableSnapshotRead(b *testing.B) {
	s := NewSet()
	for i := 0; i < 10000; i++ {
		key := Key{DType: otelutil.TypeTrace, Key: fmt.Sprintf("tenant\x00svc\x00%d\x00span-%d", i, i)}
		value := []byte(fmt.Sprintf("value-%d", i))
		s.Insert(key, value)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		active, flushing := s.Snapshot()
		_ = active
		_ = flushing
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "snapshots/sec")
}

// BenchmarkMemtableRotate measures dual-memtable rotation performance.
func BenchmarkMemtableRotate(b *testing.B) {
	s := NewSet()
	for i := 0; i < 10000; i++ {
		key := Key{DType: otelutil.TypeTrace, Key: fmt.Sprintf("tenant\x00svc\x00%d\x00span-%d", i, i)}
		value := []byte(fmt.Sprintf("value-%d", i))
		s.Insert(key, value)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Rotate()
		s.ClearFlushing() // simulate flush completion
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rotations/sec")
}
