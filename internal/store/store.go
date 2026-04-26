package store

import (
	"context"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// DataType is a local mirror of otelutil.DataType so consumers can import store directly.
type DataType string

const (
	TypeTrace  DataType = "trace"
	TypeLog    DataType = "log"
	TypeMetric DataType = "metric"
)

// Store is the persistence boundary for OctoDB.
type Store interface {
	WriteTraces(ctx context.Context, tenantID string, spans []*tracepb.ResourceSpans) error
	ReadTraces(ctx context.Context, req TraceReadRequest) ([]*tracepb.ResourceSpans, error)

	// ReadTraceByID returns all spans (ResourceSpans) for a given trace_id.
	// Uses two-level index: global locator → per-segment sidecar offsets.
	ReadTraceByID(ctx context.Context, tenantID, traceID string) ([]*tracepb.ResourceSpans, error)

	WriteLogs(ctx context.Context, tenantID string, logs []*logspb.ResourceLogs) error
	ReadLogs(ctx context.Context, req LogReadRequest) ([]*logspb.ResourceLogs, error)

	WriteMetrics(ctx context.Context, tenantID string, metrics []*metricspb.ResourceMetrics) error
	ReadMetrics(ctx context.Context, req MetricReadRequest) ([]*metricspb.ResourceMetrics, error)

	// DeleteTraces marks spans matching the predicate as deleted.
	// Tombstones are inserted into the memtable and WAL.
	DeleteTraces(ctx context.Context, tenantID string, req TraceReadRequest) error

	// DeleteLogs marks logs matching the predicate as deleted.
	DeleteLogs(ctx context.Context, tenantID string, req LogReadRequest) error

	// DeleteMetrics marks metrics matching the predicate as deleted.
	DeleteMetrics(ctx context.Context, tenantID string, req MetricReadRequest) error

	Close() error
}

// TraceReadRequest is the query shape for the traces read path.
type TraceReadRequest struct {
	TraceID    string            // hex-encoded trace_id, empty = no filter
	TenantID   string            // required
	Service    string            // optional filter
	StartTime  int64             // unix nano, inclusive
	EndTime    int64             // unix nano, exclusive
	Predicates map[string]string // simple key=value attribute filters (Block 3+)
}

// LogReadRequest is the query shape for the logs read path.
type LogReadRequest struct {
	TraceID    string            // hex-encoded trace_id, empty = no filter
	TenantID   string            // required
	Service    string            // optional filter
	StartTime  int64             // unix nano, inclusive
	EndTime    int64             // unix nano, exclusive
	Severity   int32             // optional severity filter
	Predicates map[string]string // simple key=value attribute filters
}

// MetricReadRequest is the query shape for the metrics read path.
type MetricReadRequest struct {
	TenantID   string            // required
	Service    string            // optional filter
	MetricName string            // optional filter
	StartTime  int64             // unix nano, inclusive
	EndTime    int64             // unix nano, exclusive
	MetricType string            // optional: "gauge", "sum", "histogram", "summary"
	Predicates map[string]string // simple key=value attribute filters
}
