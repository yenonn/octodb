package store

import (
	"context"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Store is the persistence boundary for OctoDB.
type Store interface {
	WriteTraces(ctx context.Context, tenantID string, spans []*tracepb.ResourceSpans) error
	ReadTraces(ctx context.Context, req ReadRequest) ([]*tracepb.ResourceSpans, error)

	// ReadTraceByID returns all spans (ResourceSpans) for a given trace_id.
	// Uses two-level index: global locator → per-segment sidecar offsets.
	ReadTraceByID(ctx context.Context, tenantID, traceID string) ([]*tracepb.ResourceSpans, error)

	Close() error
}

// ReadRequest is the query shape for the Block 2 read path.
type ReadRequest struct {
	TraceID   string            // hex-encoded trace_id, empty = no filter
	TenantID  string            // required
	Service   string            // optional filter
	StartTime int64             // unix nano, inclusive
	EndTime   int64             // unix nano, exclusive
	Predicates map[string]string // simple key=value attribute filters (Block 3+)
}
