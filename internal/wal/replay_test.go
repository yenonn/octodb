package wal

import (
	"os"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func makeTestSpans(svc, spanName string) *tracepb.ResourceSpans {
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

func TestReplayBytesRoundTrip(t *testing.T) {
	path := "test_replay_bytes.wal"
	defer os.Remove(path)

	w, _ := Open(path)
	for i := 0; i < 5; i++ {
		rs := makeTestSpans("svc", "span")
		data, _ := proto.Marshal(rs)
		_ = w.Append(data)
	}
	_ = w.Sync()
	_ = w.Close()

	var count int
	err := ReplayBytes(path, func(raw []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("replay bytes: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5, got %d", count)
	}
}

func TestReplayToRawSlice(t *testing.T) {
	path := "test_raw_slice.wal"
	defer os.Remove(path)

	w, _ := Open(path)
	rs := makeTestSpans("svc", "raw-span")
	data, _ := proto.Marshal(rs)
	_ = w.Append(data)
	_ = w.Close()

	result, err := ReplayToRawSlice(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
}

func TestReplayBytesMissingFile(t *testing.T) {
	// Should not error — missing file means nothing to replay.
	err := ReplayBytes("does_not_exist.wal", func(raw []byte) error { return nil })
	if err != nil {
		t.Fatalf("expected nil for missing file: %v", err)
	}
}

func TestReplayAndCheckpointNormal(t *testing.T) {
	path := "test_chkpt.wal"
	defer os.Remove(path)

	w, _ := Open(path)
	for i := 0; i < 3; i++ {
		rs := makeTestSpans("svc", "chkpt-span")
		data, _ := proto.Marshal(rs)
		_ = w.Append(data)
	}
	_ = w.Sync()
	_ = w.Close()

	var count int
	err := ReplayAndCheckpoint(path, func(raw []byte, rs *tracepb.ResourceSpans) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("replay and checkpoint: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 records, got %d", count)
	}
}

func TestReplayAndCheckpointEmptyFile(t *testing.T) {
	path := "test_chkpt_empty.wal"
	defer os.Remove(path)
	_ = os.WriteFile(path, []byte{}, 0644)

	err := ReplayAndCheckpoint(path, func(raw []byte, rs *tracepb.ResourceSpans) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil for empty file: %v", err)
	}
}

func TestReplayAndCheckpointTruncated(t *testing.T) {
	path := "test_chkpt_truncated.wal"
	defer os.Remove(path)

	w, _ := Open(path)
	for i := 0; i < 2; i++ {
		rs := makeTestSpans("svc", "trunc-span")
		data, _ := proto.Marshal(rs)
		_ = w.Append(data)
	}
	_ = w.Close()

	// Truncate the last 4 bytes (half of the length prefix of the second record).
	f, _ := os.OpenFile(path, os.O_WRONLY, 0644)
	info, _ := f.Stat()
	_ = f.Truncate(info.Size() - 4)
	_ = f.Close()

	var count int
	// ReplayAndCheckpoint should skip the truncated last record (best-effort),
	// so we get only the first record successfully.
	_ = ReplayAndCheckpoint(path, func(raw []byte, rs *tracepb.ResourceSpans) error {
		count++
		return nil
	})
	if count != 1 {
		t.Fatalf("expected 1 complete record after truncation, got %d", count)
	}
}

func TestReplayAndCheckpointCorruptRecord(t *testing.T) {
	path := "test_chkpt_corrupt.wal"
	defer os.Remove(path)

	w, _ := Open(path)
	rs := makeTestSpans("svc", "good")
	data, _ := proto.Marshal(rs)
	_ = w.Append(data)
	_ = w.Close()

	// Append a valid length prefix for a 10-byte record but no actual record data.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	_, _ = f.Write([]byte{0, 0, 0, 10}) // length 10, but no payload
	_ = f.Close()

	var count int
	_ = ReplayAndCheckpoint(path, func(raw []byte, rs *tracepb.ResourceSpans) error {
		count++
		return nil
	})
	// ReplayAndCheckpoint skips truncated records at the boundary, so
	// it only replays the 1 valid complete record.
	if count != 1 {
		t.Fatalf("expected 1 valid record, got %d", count)
	}
}

func TestCheckpoint(t *testing.T) {
	path := "test_checkpoint.wal"
	defer os.Remove(path)

	w, _ := Open(path)
	data := []byte("hello checkpoint")
	_ = w.Append(data)

	off, err := w.Checkpoint()
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if off <= 0 {
		t.Fatalf("expected offset > 0, got %d", off)
	}
	_ = w.Close()
}

func TestEmptyAppend(t *testing.T) {
	path := "test_empty.wal"
	defer os.Remove(path)

	w, _ := Open(path)
	err := w.Append([]byte{})
	if err == nil {
		t.Fatal("expected error for empty append")
	}
	_ = w.Close()
}
