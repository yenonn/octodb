// Package index provides two-level trace indexing for fast trace_id lookups.
//
// Layer 1: Global in-memory locator — trace_id → set of segment sequence numbers.
// Layer 2: Per-segment sidecar — trace_id → []offset within that segment.
package index

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// SegmentTraceIndex maps trace_id → list of absolute byte offsets within one segment.
// This is the Layer 2 sidecar file (e.g., sst-00000001.trace.index).
type SegmentTraceIndex struct {
	Offsets map[string][]int64 `json:"offsets"`
}

// Save writes a SegmentTraceIndex to a JSON file.
func (idx *SegmentTraceIndex) Save(path string) error {
	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("trace index marshal: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("trace index save: %w", err)
	}
	return nil
}

// Load reads a SegmentTraceIndex from a JSON file.
func LoadSegmentTraceIndex(path string) (*SegmentTraceIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("trace index read: %w", err)
	}
	var idx SegmentTraceIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("trace index unmarshal: %w", err)
	}
	return &idx, nil
}

// ---------------------------------------------------------------------------
// Layer 1: Global in-memory locator
// ---------------------------------------------------------------------------

// TraceLocator maps trace_id → set of segment sequence numbers that contain it.
// Rebuilt on startup from all segment .trace.index sidecars.
type TraceLocator struct {
	mu    sync.RWMutex
	index map[string]map[int64]struct{} // trace_id → {seq1, seq2, ...}
}

// NewTraceLocator creates an empty locator.
func NewTraceLocator() *TraceLocator {
	return &TraceLocator{
		index: make(map[string]map[int64]struct{}),
	}
}

// Add registers that a trace_id exists in a given segment sequence.
func (t *TraceLocator) Add(traceID string, seq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.index[traceID] == nil {
		t.index[traceID] = make(map[int64]struct{})
	}
	t.index[traceID][seq] = struct{}{}
}

// Find returns all segment sequences that might contain the trace_id.
// Returns nil if not found.
func (t *TraceLocator) Find(traceID string) []int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	seqs, ok := t.index[traceID]
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(seqs))
	for seq := range seqs {
		out = append(out, seq)
	}
	return out
}

// Merge adds all trace_ids from a segment's sidecar index into the global locator.
func (t *TraceLocator) Merge(seq int64, idx *SegmentTraceIndex) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for traceID := range idx.Offsets {
		if t.index[traceID] == nil {
			t.index[traceID] = make(map[int64]struct{})
		}
		t.index[traceID][seq] = struct{}{}
	}
}

// Size returns the number of unique trace_ids tracked.
func (t *TraceLocator) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.index)
}
