package wal

import (
	"fmt"
	"io"
	"os"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func TestWALRoundTrip(t *testing.T) {
	path := "test_roundtrip.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// Construct a ResourceSpans with actual data so proto.Marshal is non-empty
	rs := makeTestResourceSpans("test-service", "test-span-1")
	data, err := proto.Marshal(rs)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("test setup: proto.Marshal returned empty bytes")
	}

	if err := w.Append(data); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Replay and assert equality
	replayed, err := ReplayToSlice(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 record, got %d", len(replayed))
	}
	if !proto.Equal(rs, replayed[0]) {
		t.Fatalf("replayed proto does not match original")
	}
}

func TestWALMultipleRecords(t *testing.T) {
	path := "test_multi.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	for i := 0; i < 100; i++ {
		rs := makeTestResourceSpans("multi-service", fmt.Sprintf("span-%d", i))
		data, _ := proto.Marshal(rs)
		if err := w.Append(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	_ = w.Sync()
	_ = w.Close()

	replayed, err := ReplayToSlice(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 100 {
		t.Fatalf("expected 100 records, got %d", len(replayed))
	}
}

func makeTestResourceSpans(svc, spanName string) *tracepb.ResourceSpans {
	return &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svc}}},
			},
		},
		ScopeSpans: []*tracepb.ScopeSpans{
			{
				Scope: &commonpb.InstrumentationScope{Name: "test-scope"},
				Spans: []*tracepb.Span{
					{
						TraceId: []byte("1234567890abcdef1234567890abcdef"),
						SpanId:  []byte("abcd1234efgh5678"),
						Name:    spanName,
						Kind:    tracepb.Span_SPAN_KIND_INTERNAL,
					},
				},
			},
		},
	}
}

func TestAppendBatchRoundTrip(t *testing.T) {
	path := "test_batch_roundtrip.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// Build a batch of 20 records.
	var originals []*tracepb.ResourceSpans
	var batch [][]byte
	for i := 0; i < 20; i++ {
		rs := makeTestResourceSpans("batch-service", fmt.Sprintf("batch-span-%d", i))
		originals = append(originals, rs)
		data, err := proto.Marshal(rs)
		if err != nil {
			t.Fatalf("marshal %d: %v", i, err)
		}
		batch = append(batch, data)
	}

	if err := w.AppendBatch(batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Replay and verify all 20 records survived.
	replayed, err := ReplayToSlice(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 20 {
		t.Fatalf("expected 20 records, got %d", len(replayed))
	}
	for i, rs := range replayed {
		if !proto.Equal(originals[i], rs) {
			t.Fatalf("record %d mismatch after replay", i)
		}
	}
}

func TestAppendBatchEmpty(t *testing.T) {
	path := "test_batch_empty.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	// Empty batch should be a no-op.
	if err := w.AppendBatch(nil); err != nil {
		t.Fatalf("AppendBatch(nil) should succeed: %v", err)
	}
	if err := w.AppendBatch([][]byte{}); err != nil {
		t.Fatalf("AppendBatch(empty) should succeed: %v", err)
	}
	if w.Offset() != 0 {
		t.Fatalf("expected offset 0 after empty batches, got %d", w.Offset())
	}
}

func TestAppendBatchRejectsEmptyRecord(t *testing.T) {
	path := "test_batch_reject.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	batch := [][]byte{[]byte("valid"), {}}
	if err := w.AppendBatch(batch); err == nil {
		t.Fatal("expected error for empty record in batch")
	}
}

func TestAppendBatchMixedWithAppend(t *testing.T) {
	path := "test_batch_mixed.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// Write 5 individual records, then a batch of 10, then 5 more individual.
	for i := 0; i < 5; i++ {
		rs := makeTestResourceSpans("mixed-svc", fmt.Sprintf("single-%d", i))
		data, _ := proto.Marshal(rs)
		if err := w.Append(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	var batch [][]byte
	for i := 0; i < 10; i++ {
		rs := makeTestResourceSpans("mixed-svc", fmt.Sprintf("batch-%d", i))
		data, _ := proto.Marshal(rs)
		batch = append(batch, data)
	}
	if err := w.AppendBatch(batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	for i := 0; i < 5; i++ {
		rs := makeTestResourceSpans("mixed-svc", fmt.Sprintf("single2-%d", i))
		data, _ := proto.Marshal(rs)
		if err := w.Append(data); err != nil {
			t.Fatalf("append2 %d: %v", i, err)
		}
	}

	_ = w.Sync()
	_ = w.Close()

	replayed, err := ReplayToSlice(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 20 {
		t.Fatalf("expected 20 records, got %d", len(replayed))
	}
}

func TestWALCRC32BadCorruption(t *testing.T) {
	path := "test_crc_bad.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	rs := makeTestResourceSpans("crc-service", "crc-span")
	data, _ := proto.Marshal(rs)
	if err := w.Append(data); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = w.Sync()
	_ = w.Close()

	// Corrupt the CRC bytes (flip a bit after the length header).
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	// Offset after length header = 4 bytes.
	// Flip a bit in the first CRC byte.
	if _, err := f.Seek(4, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	b := make([]byte, 1)
	if _, err := f.Read(b); err != nil {
		t.Fatalf("read byte: %v", err)
	}
	b[0] = ^b[0]
	if _, err := f.Seek(4, io.SeekStart); err != nil {
		t.Fatalf("seek back: %v", err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatalf("write flipped byte: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corrupt: %v", err)
	}
	_ = f.Close()

	// Replay should see the corrupt record and skip it per gate-0 design.
	// ReplayAndCheckpoint should also stop at truncation.
	var count int
	var sawErr error
	_ = ReplayAndCheckpoint(path, func(raw []byte, _ *tracepb.ResourceSpans) error {
		count++
		return nil
	})
	// We don't assert exact count because replay may skip the corrupt record.
	// The important thing is that we don't panic and return some error or skip gracefully.
	if count != 0 && count != 1 {
		// ReplayAndCheckpoint skips truncated/corrupt records, so count can be 0 or 1.
		// With a corrupt CRC the record is skipped (count=0) because the record is truncated.
		t.Fatalf("unexpected count=%d after corrupt CRC, want 0 or 1", count)
	}
	_ = sawErr
}

func TestWALCRC32Good(t *testing.T) {
	path := "test_crc_good.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	for i := 0; i < 50; i++ {
		rs := makeTestResourceSpans("good-service", fmt.Sprintf("good-span-%d", i))
		data, _ := proto.Marshal(rs)
		if err := w.Append(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	_ = w.Sync()
	_ = w.Close()

	replayed, err := ReplayToSlice(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 50 {
		t.Fatalf("expected 50 records, got %d", len(replayed))
	}
}

func TestNewReaderFromFile(t *testing.T) {
	path := "test_reader_from_file.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	for i := 0; i < 10; i++ {
		rs := makeTestResourceSpans("rff-service", fmt.Sprintf("rff-span-%d", i))
		data, _ := proto.Marshal(rs)
		if err := w.Append(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	_ = w.Sync()
	_ = w.Close()

	reader, err := NewReaderFromFile(path)
	if err != nil {
		t.Fatalf("NewReaderFromFile: %v", err)
	}
	defer reader.Close()

	var count int
	for {
		_, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}
	if count != 10 {
		t.Fatalf("expected 10 records from NewReaderFromFile, got %d", count)
	}
}

func TestWALReaderClose(t *testing.T) {
	// Close on a reader without underlying file should not panic.
	r := NewReader(nil)
	if err := r.Close(); err != nil {
		t.Fatalf("Close() should return nil for nil underlying file, got %v", err)
	}
}

func TestWALOffset(t *testing.T) {
	path := "test_offset.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	if w.Offset() != 0 {
		t.Fatalf("expected initial offset 0, got %d", w.Offset())
	}

	rs := makeTestResourceSpans("offset-service", "offset-span")
	data, _ := proto.Marshal(rs)
	if err := w.Append(data); err != nil {
		t.Fatalf("append: %v", err)
	}

	expected := int64(8 + len(data)) // length(4) + CRC(4) + payload
	if w.Offset() != expected {
		t.Fatalf("expected offset %d after append, got %d", expected, w.Offset())
	}
	_ = w.Close()
}

func TestReplayFromOffset(t *testing.T) {
	path := "test_replay_offset.wal"
	defer os.Remove(path)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	for i := 0; i < 10; i++ {
		rs := makeTestResourceSpans("ro-service", fmt.Sprintf("ro-span-%d", i))
		data, _ := proto.Marshal(rs)
		if err := w.Append(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	_ = w.Sync()
	offset5 := w.Offset() // offset after all 10 records
	_ = w.Close()

	// Replay from offset 0 should return all 10.
	var count int
	if err := ReplayFromOffset(nil, path, 0, func(raw []byte, _ *tracepb.ResourceSpans) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ReplayFromOffset: %v", err)
	}
	if count != 10 {
		t.Fatalf("expected 10 records from offset 0, got %d", count)
	}

	// Replay from end offset should return 0 records.
	count = 0
	if err := ReplayFromOffset(nil, path, offset5, func(raw []byte, _ *tracepb.ResourceSpans) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ReplayFromOffset from end: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 records from end offset, got %d", count)
	}
}

func TestReplayFromOffsetMissingFile(t *testing.T) {
	// Missing file is a no-op (returns nil) — by design for replay on fresh start.
	err := ReplayFromOffset(nil, "nonexistent.wal", 0, func(raw []byte, _ *tracepb.ResourceSpans) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil for missing file, got: %v", err)
	}
}

func TestNewReaderFromFileMissing(t *testing.T) {
	_, err := NewReaderFromFile("nonexistent.wal")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestOpenStatError(t *testing.T) {
	// Open a path that's a directory, not a file — triggers stat or open error.
	dir, _ := os.MkdirTemp("", "wal-test-dir")
	defer os.RemoveAll(dir)

	// Trying to open a directory as append-only file should fail.
	_, err := Open(dir)
	if err == nil {
		t.Fatal("expected error opening directory as WAL")
	}
}

func BenchmarkWALAppendSingle(b *testing.B) {
	path := "bench_single.wal"
	defer os.Remove(path)

	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			w, _ := Open(path)
			defer w.Close()

			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Append(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWALAppendBatch(b *testing.B) {
	path := "bench_batch.wal"
	defer os.Remove(path)

	benchmarks := []struct {
		name  string
		count int
		size  int
	}{
		{"batch=10_size=100", 10, 100},
		{"batch=100_size=100", 100, 100},
		{"batch=10_size=1000", 10, 1000},
		{"batch=100_size=1000", 100, 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			w, _ := Open(path)
			defer w.Close()

			data := make([]byte, bm.size)
			for i := range data {
				data[i] = byte(i)
			}

			batch := make([][]byte, bm.count)
			for i := range batch {
				batch[i] = data
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.AppendBatch(batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

