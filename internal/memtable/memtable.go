// Package memtable provides a sorted in-memory structure for hot trace data.
//
// Block 2: B-tree memtable with dual rotation (active + immutable for flush).
// Sort key: (tenant_id, service.name, start_time_unix_nano, span_id).
package memtable

import (
	"sync"

	"github.com/google/btree"

	"github.com/octodb/octodb/pkg/otelutil"
)

// Item is a single entry in the memtable.
type Item struct {
	Key   otelutil.SortKey
	Value []byte // raw proto bytes
}

func (i Item) Less(than btree.Item) bool {
	return i.Key.Key() < than.(Item).Key.Key()
}

// Memtable is a sorted in-memory buffer backed by a B-tree.
type Memtable struct {
	data  *btree.BTree
	size  int64 // estimated bytes
	mu    sync.RWMutex
}

// New creates an empty memtable.
func New() *Memtable {
	return &Memtable{
		data: btree.New(32),
	}
}

// Insert adds an item. Not safe for concurrent use without caller locking.
func (m *Memtable) Insert(key otelutil.SortKey, value []byte) {
	m.data.ReplaceOrInsert(Item{Key: key, Value: value})
	m.size += int64(len(key.Key()) + len(value))
}

// Size returns estimated bytes held by the memtable.
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// SetSize is used during WAL replay to set accurate size.
func (m *Memtable) SetSize(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.size = n
}

// Ascend calls fn for every item in ascending sort key order.
// Safe for concurrent reads.
func (m *Memtable) Ascend(fn func(key otelutil.SortKey, value []byte) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.data.Ascend(func(item btree.Item) bool {
		it := item.(Item)
		return fn(it.Key, it.Value)
	})
}

// ---------------------------------------------------------------------------
// Set — dual memtable rotation
// ---------------------------------------------------------------------------

// Set manages active (writable) + flushing (immutable) memtables.
type Set struct {
	active   *Memtable
	flushing *Memtable
	mu       sync.RWMutex
}

// NewSet creates a dual-memtable set with one empty active memtable.
func NewSet() *Set {
	return &Set{active: New()}
}

// Insert writes an item into the active memtable.
func (s *Set) Insert(key otelutil.SortKey, value []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.active.Insert(key, value)
}

// ActiveSize returns the current active memtable size.
func (s *Set) ActiveSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active.size
}

// Rotate swaps active → flushing, creates new active.
// Returns the old active (now flushing).
func (s *Set) Rotate() *Memtable {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.flushing != nil {
		// Already rotating — caller should wait.
		return nil
	}
	immutable := s.active
	s.active = New()
	s.flushing = immutable
	return immutable
}

// ClearFlushing frees the flushing memtable after successful flush.
func (s *Set) ClearFlushing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushing = nil
}

// Snapshot returns both memtables for concurrent reads.
func (s *Set) Snapshot() (active, flushing *Memtable) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active, s.flushing
}
