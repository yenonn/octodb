// Package otelutil provides helpers for working with OTel data structures.
package otelutil

import (
	"fmt"
)

// SortKey defines the segment sort order: (tenant, service.name, time, span_id).
// This is the most important decision in the read path — it determines physical layout.
type SortKey struct {
	TenantID string
	Service  string
	TimeNano uint64
	SpanID   string
}

// Key returns a string representation for btree comparison.
func (k SortKey) Key() string {
	return fmt.Sprintf("%s\x00%s\x00%016x\x00%s", k.TenantID, k.Service, k.TimeNano, k.SpanID)
}

// Less compares two SortKeys for btree ordering.
func Less(a, b SortKey) bool {
	return a.Key() < b.Key()
}
