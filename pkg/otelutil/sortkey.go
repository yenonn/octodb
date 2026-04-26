// Package otelutil provides helpers for working with OTel data structures and sort keys.
package otelutil

import "fmt"

// DataType identifies the OTel signal type.
type DataType string

const (
	TypeTrace DataType = "trace"
	TypeLog   DataType = "log"
)

// ---------------------------------------------------------------------------
// Traces: SortKey defines the segment sort order for ResourceSpans.
// (tenant, service.name, start_time_unix_nano, span_id).
// ---------------------------------------------------------------------------

// TraceSortKey defines physical layout for traces.
type TraceSortKey struct {
	TenantID string
	Service  string
	TimeNano uint64
	SpanID   string
}

func (k TraceSortKey) Key() string {
	return fmt.Sprintf("%s\x00%s\x00%016x\x00%s", k.TenantID, k.Service, k.TimeNano, k.SpanID)
}

// ---------------------------------------------------------------------------
// Logs: SortKey for ResourceLogs.
// (tenant, service.name, observed_time_unix_nano).
// Primary: trace_id (if present) for trace-to-log correlation.
// Fallback: observed_time for ordering within the same service.
// ---------------------------------------------------------------------------

// LogSortKey defines physical layout for logs.
type LogSortKey struct {
	TenantID string
	Service  string
	TimeNano uint64 // observed_time_unix_nano
	TraceID  string // hex-encoded trace_id; "none" if absent
	LogID    string // deterministic composite: severity+body hash for uniqueness
}

func (k LogSortKey) Key() string {
	return fmt.Sprintf("%s\x00%s\x00%016x\x00%s\x00%s",
		k.TenantID, k.Service, k.TimeNano, k.TraceID, k.LogID)
}

// ---------------------------------------------------------------------------
// Generic sort key type used by memtable / store (string-based).
// ---------------------------------------------------------------------------

// GenericSortKey carries a type tag so the memtable can store traces and
// logs in separate logical views without having two B-trees.
type GenericSortKey struct {
	DType DataType
	Key   string // the sort key string
}

// TraceFromGeneric reconstructs a TraceSortKey from its string representation.
// Not used in hot paths (mostly debug / tests).
func TraceFromGeneric(key string) TraceSortKey {
	var k TraceSortKey
	fmt.Sscanf(key, "%s\x00%s\x00%016x\x00%s", &k.TenantID, &k.Service, &k.TimeNano, &k.SpanID)
	return k
}

// LogFromGeneric reconstructs a LogSortKey from its string representation.
func LogFromGeneric(key string) LogSortKey {
	var k LogSortKey
	fmt.Sscanf(key, "%s\x00%s\x00%016x\x00%s\x00%s",
		&k.TenantID, &k.Service, &k.TimeNano, &k.TraceID, &k.LogID)
	return k
}
