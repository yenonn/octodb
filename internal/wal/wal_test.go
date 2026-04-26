package wal

import (
	"fmt"
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

func TestWALMissingFile(t *testing.T) {
	result, err := ReplayToSlice("nonexistent.wal")
	if err != nil {
		t.Fatalf("replay of missing file should succeed silently: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result for missing file")
	}
}
