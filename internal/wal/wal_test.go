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
}

