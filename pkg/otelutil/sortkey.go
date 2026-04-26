// Package otelutil provides helpers for working with OTel data structures and sort keys.
package otelutil

import (
	"fmt"
	"strings"
)

// DataType identifies the OTel signal type.
type DataType string

const (
	TypeTrace  DataType = "trace"
	TypeLog    DataType = "log"
	TypeMetric DataType = "metric"
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
	parts := strings.Split(key, "\x00")
	k := TraceSortKey{TenantID: parts[0], Service: parts[1], SpanID: parts[3]}
	if len(parts) > 2 {
		fmt.Sscanf(parts[2], "%016x", &k.TimeNano)
	}
	return k
}

// LogFromGeneric reconstructs a LogSortKey from its string representation.
func LogFromGeneric(key string) LogSortKey {
	parts := strings.Split(key, "\x00")
	k := LogSortKey{TenantID: parts[0], Service: parts[1], TraceID: parts[3], LogID: parts[4]}
	if len(parts) > 2 {
		fmt.Sscanf(parts[2], "%016x", &k.TimeNano)
	}
	return k
}

// ---------------------------------------------------------------------------
// Metrics: SortKey for ResourceMetrics.
// (tenant, service.name, metric_name, time).
// ---------------------------------------------------------------------------

// MetricSortKey defines physical layout for metrics.
// Metrics are primarily queried by metric name + time range.
type MetricSortKey struct {
	TenantID  string
	Service   string
	MetricName string
	TimeNano  uint64 // start_time_unix_nano of first data point
	// Optional: metric type suffix for grouping like metrics
}

func (k MetricSortKey) Key() string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%016x",
		k.TenantID, k.Service, k.MetricName, k.TimeNano)
}

