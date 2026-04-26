package wal

import (
	"context"
	"io"
	"os"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Record is a helper that pairs a raw WAL entry with its (unmarshaled) ResourceSpans.
// Used during replay.
type Record struct {
	Raw         []byte
	ResourceSpan *tracepb.ResourceSpans
}

// Replay replays every record in a WAL file, calling fn for each.
// If fn returns an error, replay stops and that error is returned.
//
// Example:
//
//	err := wal.Replay("octodb.wal", func(data []byte, rs *tracepb.ResourceSpans) error {
//	    // process rs
//	    return nil
//	})
func Replay(path string, fn func(raw []byte, rs *tracepb.ResourceSpans) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Empty WAL is valid — nothing to replay.
			return nil
		}
		return err
	}
	defer f.Close()

	r := NewReader(f)

	for {
		data, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		var rs tracepb.ResourceSpans
		if err := proto.Unmarshal(data, &rs); err != nil {
			return err
		}

		if err := fn(data, &rs); err != nil {
			return err
		}
	}
	return nil
}

// ReplayToSlice replays the WAL into a slice.
// Convenience for crash-recovery tests.
func ReplayToSlice(path string) ([]*tracepb.ResourceSpans, error) {
	var result []*tracepb.ResourceSpans
	err := Replay(path, func(_ []byte, rs *tracepb.ResourceSpans) error {
		result = append(result, rs)
		return nil
	})
	return result, err
}

// TODO(gate-0): Replay from a specific offset (requires manifest checkpoint).
// For Block 1, replay always starts at offset 0.
func ReplayFromOffset(ctx context.Context, path string, offset int64, fn func(raw []byte, rs *tracepb.ResourceSpans) error) error {
	// TBD when manifest checkpointing is implemented in Block 2.
	_ = ctx
	_ = offset
	return Replay(path, fn)
}
