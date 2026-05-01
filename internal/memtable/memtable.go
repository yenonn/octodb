// Package memtable provides sorted in-memory structures for hot trace and log data.
//
// Block 2: B-tree memtable with dual rotation (active + immutable for flush).
// Sort key: (data_type, tenant_id, service.name, time, id).
//
// Block 4+: Added log support via dual signal path.
package memtable

import (
	"sync"

	"github.com/google/btree"
	"github.com/octodb/octodb/pkg/otelutil"
)

// Key is the unified sort key used in the memtable.
type Key struct {
	DType otelutil.DataType // "trace" or "log"
	Key   string            // full sort key string
}

// Item is a single entry in the memtable.
type Item struct {
	Key     Key
	Value   []byte // raw proto bytes
	Deleted bool   // tombstone marker
}

func (i Item) Less(than btree.Item) bool {
	j := than.(Item)
	if i.Key.DType != j.Key.DType {
		return i.Key.DType < j.Key.DType
	}
	return i.Key.Key < j.Key.Key
}

// Memtable is a sorted in-memory buffer backed by a B-tree.
type Memtable struct {
	data *btree.BTree
	size int64 // estimated bytes
	mu   sync.RWMutex
}

// New creates an empty memtable.
func New() *Memtable {
	return &Memtable{
		data: btree.New(32),
	}
}

// Insert adds an item. Not safe for concurrent use without caller locking.
func (m *Memtable) Insert(key Key, value []byte) {
	m.data.ReplaceOrInsert(Item{Key: key, Value: value})
	m.size += int64(len(key.Key) + len(value))
}

// InsertTombstone adds a tombstone entry for deletion.
func (m *Memtable) InsertTombstone(key Key) {
	m.data.ReplaceOrInsert(Item{Key: key, Deleted: true})
	m.size += int64(len(key.Key))
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
// Tombstones (Deleted=true) are skipped.
// Safe for concurrent reads.
func (m *Memtable) Ascend(fn func(key Key, value []byte) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.data.Ascend(func(item btree.Item) bool {
		it := item.(Item)
		if it.Deleted {
			return true
		}
		if it.Value == nil || len(it.Value) == 0 {
			return true
		}
		return fn(it.Key, it.Value)
	})
}

// AscendAll calls fn for every item including tombstones.
func (m *Memtable) AscendAll(fn func(key Key, value []byte, deleted bool) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.data.Ascend(func(item btree.Item) bool {
		it := item.(Item)
		return fn(it.Key, it.Value, it.Deleted)
	})
}

// AscendByType calls fn only for items matching the given data type.
// Tombstones (Deleted=true) are skipped.
func (m *Memtable) AscendByType(dtype otelutil.DataType, fn func(key Key, value []byte) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.data.Ascend(func(item btree.Item) bool {
		it := item.(Item)
		if it.Key.DType != dtype {
			// Since the B-tree is sorted by DType first, if we've passed the target,
			// we can stop (if dtype < current, keep going until we reach it;
			// if dtype > current, we've gone past)
			if it.Key.DType > dtype {
				return false
			}
			return true
		}
		if it.Deleted {
			return true
		}
		if it.Value == nil || len(it.Value) == 0 {
			return true
		}
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
func (s *Set) Insert(key Key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active.Insert(key, value)
}

// InsertTombstone writes a tombstone into the active memtable.
func (s *Set) InsertTombstone(key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active.InsertTombstone(key)
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
