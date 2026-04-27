package manifest

import (
	"fmt"
	"testing"
)

// BenchmarkManifestAppend measures manifest append + fsync throughput.
func BenchmarkManifestAppend(b *testing.B) {
	dir := b.TempDir()
	m, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}

	rec := Record{
		Sequence:      1,
		SegmentFile:   "sst-00000001.pb",
		BloomFile:     "sst-00000001.bloom",
		WALCheckpoint: "000001.wal:1024",
		MinKey:        SortKey{TenantID: "default", Service: "svc", TimeNano: 123456789, Str: "default\x00svc\x00"},
		MaxKey:        SortKey{TenantID: "default", Service: "svc", TimeNano: 987654321, Str: "default\x00svc\x00"},
		SpanCount:     1000,
		CreatedAt:     1234567890000000000,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := rec
		r.Sequence = int64(i)
		if err := m.AppendRecord(r); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/sec")
}

// BenchmarkManifestCheckpoint measures checkpoint write + fsync throughput.
func BenchmarkManifestCheckpoint(b *testing.B) {
	dir := b.TempDir()
	m, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("%06d.wal", i)
		if err := m.WriteCheckpoint(name, int64(i)*1024); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "checkpoints/sec")
}

// BenchmarkManifestReplay measures startup replay of 1000 records.
func BenchmarkManifestReplay(b *testing.B) {
	dir := b.TempDir()
	m, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		_ = m.AppendRecord(Record{
			Sequence:      int64(i),
			SegmentFile:   fmt.Sprintf("sst-%08d.pb", i),
			BloomFile:     fmt.Sprintf("sst-%08d.bloom", i),
			WALCheckpoint: fmt.Sprintf("%06d.wal:%d", i+1, i*1024),
			MinKey:        SortKey{TenantID: "default", Service: "svc", TimeNano: uint64(i), Str: fmt.Sprintf("default\x00svc\x00%d", i)},
			MaxKey:        SortKey{TenantID: "default", Service: "svc", TimeNano: uint64(i + 1), Str: fmt.Sprintf("default\x00svc\x00%d", i+1)},
			SpanCount:     1000,
			CreatedAt:     int64(i),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m2, err := Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		recs := m2.AllRecords()
		_ = recs
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "replays/sec")
}
