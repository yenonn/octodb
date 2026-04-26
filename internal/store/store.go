package store

import (
	"context"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// DataType is a local mirror of otelutil.DataType so consumers can import store directly.
type DataType string

const (
	TypeTrace DataType = "trace"
	TypeLog   DataType = "log"
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
