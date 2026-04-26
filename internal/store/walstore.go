package store

import (
	"context"
	"fmt"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/octodb/octodb/internal/wal"
)

// WALStore is the Block 1 Store implementation.
// It appends OTLP ResourceSpans to a length-prefixed WAL file and fsyncs before ACK.
type WALStore struct {
	w *wal.Writer
}

// NewWALStore opens (or creates) a WAL-backed store at the given path.
func NewWALStore(path string) (*WALStore, error) {
	writer, err := wal.Open(path)
	if err != nil {
		return nil, fmt.Errorf("walstore: open wal: %w", err)
	}
	return &WALStore{w: writer}, nil
}

// WriteTraces writes each ResourceSpans as a separate WAL record,
// then fsyncs the entire batch before returning.
func (s *WALStore) WriteTraces(ctx context.Context, tenantID string, spans []*tracepb.ResourceSpans) error {
	for _, rs := range spans {
		data, err := proto.Marshal(rs)
		if err != nil {
			return fmt.Errorf("walstore: marshal ResourceSpans: %w", err)
		}
		if err := s.w.Append(data); err != nil {
			return fmt.Errorf("walstore: append: %w", err)
		}
	}
	if err := s.w.Sync(); err != nil {
		return fmt.Errorf("walstore: sync: %w", err)
	}
	return nil
}

// WriteLogs writes each ResourceLogs as a separate WAL record, then fsyncs.
func (s *WALStore) WriteLogs(ctx context.Context, tenantID string, logs []*logspb.ResourceLogs) error {
	for _, rl := range logs {
		data, err := proto.Marshal(rl)
		if err != nil {
			return err
		}
		if err := s.w.Append(data); err != nil {
			return err
		}
	}
	return s.w.Sync()
}

// WriteMetrics writes each ResourceMetrics as a separate WAL record, then fsyncs.
func (s *WALStore) WriteMetrics(ctx context.Context, tenantID string, metrics []*metricspb.ResourceMetrics) error {
	for _, rm := range metrics {
		data, err := proto.Marshal(rm)
		if err != nil {
			return err
		}
		if err := s.w.Append(data); err != nil {
			return err
		}
	}
	return s.w.Sync()
}

// ReadTraces is not supported in Block 1.
func (s *WALStore) ReadTraces(ctx context.Context, req TraceReadRequest) ([]*tracepb.ResourceSpans, error) {
	return nil, fmt.Errorf("walstore: ReadTraces not implemented in Block 1")
}

// ReadTraceByID is not supported in Block 1.
func (s *WALStore) ReadTraceByID(ctx context.Context, tenantID, traceID string) ([]*tracepb.ResourceSpans, error) {
	return nil, fmt.Errorf("walstore: ReadTraceByID not implemented in Block 1")
}

// ReadLogs is not supported in Block 1.
func (s *WALStore) ReadLogs(ctx context.Context, req LogReadRequest) ([]*logspb.ResourceLogs, error) {
	return nil, fmt.Errorf("walstore: ReadLogs not implemented in Block 1")
}

// ReadMetrics is not supported in Block 1.
func (s *WALStore) ReadMetrics(ctx context.Context, req MetricReadRequest) ([]*metricspb.ResourceMetrics, error) {
	return nil, fmt.Errorf("walstore: ReadMetrics not implemented in Block 1")
}

// Close cleanly shuts down the WAL.
func (s *WALStore) Close() error {
	return s.w.Close()
}
