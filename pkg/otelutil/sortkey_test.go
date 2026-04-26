package otelutil

import (
	"strings"
	"testing"
)

func TestTraceSortKeyOrdering(t *testing.T) {
	k1 := TraceSortKey{TenantID: "a", Service: "b", TimeNano: 100, SpanID: "c"}
	k2 := TraceSortKey{TenantID: "a", Service: "b", TimeNano: 200, SpanID: "c"}
	if k1.Key() >= k2.Key() {
		t.Fatal("expected earlier time to sort first")
	}
}

func TestLogSortKeyOrdering(t *testing.T) {
	k1 := LogSortKey{TenantID: "a", Service: "b", TimeNano: 100, TraceID: "none", LogID: "x"}
	k2 := LogSortKey{TenantID: "a", Service: "b", TimeNano: 200, TraceID: "none", LogID: "x"}
	if k1.Key() >= k2.Key() {
		t.Fatal("expected earlier time to sort first")
	}
}

func TestTraceSortKeyFormat(t *testing.T) {
	k := TraceSortKey{TenantID: "t1", Service: "svc", TimeNano: 255, SpanID: "abc"}
	s := k.Key()
	parts := strings.Split(s, "\x00")
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts, got %d: %q", len(parts), s)
	}
	if parts[0] != "t1" || parts[1] != "svc" || parts[3] != "abc" {
		t.Fatalf("unexpected parts: %v", parts)
	}
}

func TestLogSortKeyFormat(t *testing.T) {
	k := LogSortKey{TenantID: "t1", Service: "svc", TimeNano: 255, TraceID: "abc", LogID: "def"}
	s := k.Key()
	parts := strings.Split(s, "\x00")
	if len(parts) != 5 {
		t.Fatalf("expected 5 parts, got %d: %q", len(parts), s)
	}
}

func TestMetricSortKeyFormat(t *testing.T) {
	k := MetricSortKey{TenantID: "t1", Service: "svc", MetricName: "cpu", TimeNano: 123}
	s := k.Key()
	parts := strings.Split(s, "\x00")
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts, got %d: %q", len(parts), s)
	}
	if parts[0] != "t1" || parts[1] != "svc" || parts[2] != "cpu" {
		t.Fatalf("unexpected parts: %v", parts)
	}
}

func TestTraceFromGeneric(t *testing.T) {
	k := TraceSortKey{TenantID: "t1", Service: "s1", TimeNano: 0xAB, SpanID: "sid"}
	r := TraceFromGeneric(k.Key())
	if r.TenantID != k.TenantID || r.Service != k.Service || r.SpanID != k.SpanID {
		t.Fatalf("round-trip string mismatch: %+v vs %+v", r, k)
	}
	if r.TimeNano != k.TimeNano {
		t.Fatalf("timeNano mismatch: got %d want %d", r.TimeNano, k.TimeNano)
	}
}

func TestLogFromGeneric(t *testing.T) {
	k := LogSortKey{TenantID: "t1", Service: "s1", TimeNano: 0xCD, TraceID: "tid", LogID: "lid"}
	r := LogFromGeneric(k.Key())
	if r.TenantID != k.TenantID || r.Service != k.Service || r.TraceID != k.TraceID || r.LogID != k.LogID {
		t.Fatalf("round-trip mismatch: %+v vs %+v", r, k)
	}
	if r.TimeNano != k.TimeNano {
		t.Fatalf("timeNano mismatch: got %d want %d", r.TimeNano, k.TimeNano)
	}
}

func TestSortKeyUniqueness(t *testing.T) {
	k1 := TraceSortKey{TenantID: "a", Service: "b", TimeNano: 1, SpanID: "x"}
	k2 := TraceSortKey{TenantID: "a", Service: "b", TimeNano: 1, SpanID: "y"}
	if k1.Key() == k2.Key() {
		t.Fatal("expected different span IDs to produce different keys")
	}
}

func TestDataTypeOrdering(t *testing.T) {
	// String comparison ordering (ASCII first character):
	// 'l'=108 < 'm'=109 < 't'=116
	// So: log < metric < trace
	if TypeLog >= TypeMetric {
		t.Fatal("expected log < metric")
	}
	if TypeMetric >= TypeTrace {
		t.Fatal("expected metric < trace")
	}
	if TypeLog >= TypeTrace {
		t.Fatal("expected log < trace")
	}
}
