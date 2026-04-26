// Package manifest manages the segment manifest and WAL checkpoint files.
//
// Block 2: Append-only manifest log + atomic checkpoint.
package manifest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Record is one line in the manifest log — represents one flushed segment.
type Record struct {
	Sequence      int64      `json:"sequence"`
	SegmentFile   string     `json:"segment_file"`
	BloomFile     string     `json:"bloom_file"`
	WALCheckpoint string     `json:"wal_checkpoint"` // "filename:offset"
	MinKey        SortKey    `json:"min_key"`
	MaxKey        SortKey    `json:"max_key"`
	SpanCount     int64      `json:"span_count"`
	Checksum      string     `json:"checksum"`
}

// SortKey mirrors otelutil.SortKey as JSON-friendly strings.
type SortKey struct {
	TenantID string `json:"tenant"`
	Service  string `json:"service"`
	TimeNano uint64 `json:"time"`
	SpanID   string `json:"span_id"`
	Str      string `json:"_str"` // original sort key string for ordering
}

// SortKeyFromString converts a raw memtable sort key string into a manifest SortKey.
// It does a best-effort parse; caller should ensure the string was generated
// by otelutil.TraceSortKey.Key() or otelutil.LogSortKey.Key().
func SortKeyFromString(s string) SortKey {
	parts := splitNul(s)
	k := SortKey{Str: s}
	if len(parts) > 0 {
		k.TenantID = parts[0]
	}
	if len(parts) > 1 {
		k.Service = parts[1]
	}
	if len(parts) > 2 {
		if v, err := strconv.ParseUint(parts[2], 16, 64); err == nil {
			k.TimeNano = v
		}
	}
	return k
}

func splitNul(s string) []string {
	var out []string
	start := 0
	for i, b := range s {
		if b == 0 {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// Manager coordinates manifest writes, checkpoints, and segment indexing.
type Manager struct {
	mu           sync.Mutex
	manifestPath string
	checkpointPath string
	dir          string
	nextSeq      int64
	records      []Record
	walFiles     map[string]int64 // wal_filename -> last checkpointed offset
}

// Open creates or opens a manifest manager at the given directory.
func Open(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("manifest: mkdir %s: %w", dir, err)
	}

	m := &Manager{
		manifestPath:   filepath.Join(dir, "manifest.log"),
		checkpointPath: filepath.Join(dir, "manifest.log.checkpoint"),
		dir:            dir,
		nextSeq:        1,
		walFiles:       make(map[string]int64),
	}

	// Replay existing manifest to recover state.
	if err := m.replay(); err != nil {
		return nil, err
	}

	return m, nil
}

// replay reads the manifest log from the beginning and rebuilds in-memory state.
func (m *Manager) replay() error {
	f, err := os.Open(m.manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh start
		}
		return fmt.Errorf("manifest replay open: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec Record
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return fmt.Errorf("manifest replay unmarshal: %w", err)
		}
		m.records = append(m.records, rec)
		m.nextSeq = rec.Sequence + 1
		if pos, err := ParseWALCheckpoint(rec.WALCheckpoint); err == nil {
			m.walFiles[pos.File] = pos.Offset
		}
	}
	return scanner.Err()
}

// WALPosition holds a parsed wal_checkpoint string.
type WALPosition struct {
	File   string
	Offset int64
}

// ParseWALCheckpoint converts "000001.wal:1847294" into structured form.
func ParseWALCheckpoint(s string) (WALPosition, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return WALPosition{}, fmt.Errorf("invalid wal checkpoint format: %q", s)
	}
	offset, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return WALPosition{}, fmt.Errorf("invalid wal checkpoint offset: %w", err)
	}
	return WALPosition{File: parts[0], Offset: offset}, nil
}

// AppendRecord appends a manifest record and fsyncs the file.
func (m *Manager) AppendRecord(rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := os.OpenFile(m.manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("manifest append open: %w", err)
	}
	defer f.Close()

	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("manifest append write: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("manifest append fsync: %w", err)
	}

	m.records = append(m.records, rec)
	m.nextSeq = rec.Sequence + 1
	return nil
}

// WriteCheckpoint writes the wal filename:offset to the checkpoint file atomically.
func (m *Manager) WriteCheckpoint(walFile string, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := fmt.Sprintf("%s:%d", walFile, offset)

	// Atomic write via temp file + rename.
	tmpPath := m.checkpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(cp), 0644); err != nil {
		return fmt.Errorf("manifest checkpoint write: %w", err)
	}
	if err := os.Rename(tmpPath, m.checkpointPath); err != nil {
		return fmt.Errorf("manifest checkpoint rename: %w", err)
	}

	// Sync directory.
	dir, err := os.Open(m.dir)
	if err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	m.walFiles[walFile] = offset
	return nil
}

// ReadCheckpoint returns the last successfully checkpointed WAL position.
func (m *Manager) ReadCheckpoint() (WALPosition, error) {
	data, err := os.ReadFile(m.checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return WALPosition{}, nil // no checkpoint yet
		}
		return WALPosition{}, err
	}
	return ParseWALCheckpoint(strings.TrimSpace(string(data)))
}

// AllRecords returns a copy of all committed segment records.
func (m *Manager) AllRecords() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, len(m.records))
	copy(out, m.records)
	return out
}

// NextSequence returns the next sequence number to use.
func (m *Manager) NextSequence() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextSeq
}
