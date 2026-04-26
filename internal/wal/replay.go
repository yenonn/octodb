package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

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
	return ReplayBytes(path, func(raw []byte) error {
		var rs tracepb.ResourceSpans
		if err := proto.Unmarshal(raw, &rs); err != nil {
			return err
		}
		return fn(raw, &rs)
	})
}

// ReplayBytes replays raw WAL bytes without forcing a protobuf type.
// Used for log signal path where the signal type is determined at runtime.
func ReplayBytes(path string, fn func(raw []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
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
		if err := fn(data); err != nil {
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

// ReplayToRawSlice replays WAL into raw byte slices (type-agnostic).
func ReplayToRawSlice(path string) ([][]byte, error) {
	var result [][]byte
	err := ReplayBytes(path, func(raw []byte) error {
		result = append(result, raw)
		return nil
	})
	return result, err
}

// TODO(gate-0): Replay from a specific offset (requires manifest checkpoint).
// For Block 1, replay always starts at offset 0.
func ReplayFromOffset(ctx context.Context, path string, offset int64, fn func(raw []byte, rs *tracepb.ResourceSpans) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	// Skip to offset.
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("wal: seek to offset %d: %w", offset, err)
		}
	}

	// Consume corrupt records until a valid one is found or EOF.
	r := NewReader(f)
	for {
		data, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if err.Error() == "wal: record checksum mismatch" {
				fmt.Fprintf(os.Stderr, "wal: skipping corrupt record after offset %d: %v\n", offset, err)
				continue
			}
			// Truncated record at the write boundary is expected — stop here.
			if errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "truncated") {
				break
			}
			return err
		}
		var rs tracepb.ResourceSpans
		if err := proto.Unmarshal(data, &rs); err != nil {
			// Proto unmarshal failure implies corrupt payload even though CRC passed.
			fmt.Fprintf(os.Stderr, "wal: skipping unmarshalable record: %v\n", err)
			continue
		}
		if err := fn(data, &rs); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// WAL crash-recovery helpers
// ---------------------------------------------------------------------------

// Checkpoint writes a "done" marker at the end of WAL so replay knows where to stop.
// For now, returns the current offset as the logical checkpoint.
func (w *Writer) Checkpoint() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset, w.file.Sync()
}

// ReplayAndCheckpoint replays up to the last fsynced offset (current file size).
func ReplayAndCheckpoint(path string, fn func(raw []byte, rs *tracepb.ResourceSpans) error) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size() == 0 {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := NewReader(f)
	var count int
	for {
		data, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Truncated WAL at the write boundary — fsync may not have completed.
			// Skip the last partial record (best-effort).
			fmt.Fprintf(os.Stderr, "wal checkpoint: skipping truncated record at count=%d: %v\n", count, err)
			return nil
		}
		count++
		var rs tracepb.ResourceSpans
		if err := proto.Unmarshal(data, &rs); err != nil {
			// Corrupt record — skip and keep recovering.
			continue
		}
		if err := fn(data, &rs); err != nil {
			return err
		}
	}
	return nil
}
