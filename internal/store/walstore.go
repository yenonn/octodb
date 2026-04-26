package store

import (
	"context"
	"fmt"

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

	// Durability boundary: fsync before ACK.
	if err := s.w.Sync(); err != nil {
		return fmt.Errorf("walstore: sync: %w", err)
	}
	return nil
}

// ReadTraces is not supported in Block 1.
func (s *WALStore) ReadTraces(ctx context.Context, req ReadRequest) ([]*tracepb.ResourceSpans, error) {
	return nil, fmt.Errorf("walstore: ReadTraces not implemented in Block 1")
}

// Close cleanly shuts down the WAL.
func (s *WALStore) Close() error {
	return s.w.Close()
}
