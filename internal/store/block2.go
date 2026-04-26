// Package store provides Block 2 storage: WAL + sorted memtable + segment flush.
// Block 3 additions: block index footer, bloom filter, time-range seek.
// Block 4 additions: two-level trace_id index (global locator + per-segment sidecar).
// Block 4b additions: log + metric signal paths, triple WAL triple memtable.
package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/octodb/octodb/internal/bloom"
	"github.com/octodb/octodb/internal/index"
	"github.com/octodb/octodb/internal/manifest"
	"github.com/octodb/octodb/internal/memtable"
	"github.com/octodb/octodb/internal/segment"
	"github.com/octodb/octodb/internal/wal"
	"github.com/octodb/octodb/pkg/otelutil"
)

const (
	DefaultMemtableFlushThreshold = 64 * 1024 * 1024 // 64 MB — production default
	TestMemtableFlushThreshold    = 1024              // 1 KB — easy to trigger during testing
	FlushPollInterval             = 1 * time.Second
	DefaultRetention              = 7 * 24 * time.Hour // default segment retention for TTL
	CompactionInterval            = 6 * time.Hour      // how often to trigger compaction
)

// MemtableMaxIdleFlush is the maximum duration between flushes even if size threshold isn't met.
const MemtableMaxIdleFlush = 30 * time.Second

// StoreConfig holds tunable parameters for the block2 store.
type StoreConfig struct {
	MemtableFlushThreshold int64
}

// DefaultStoreConfig returns production defaults.
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		MemtableFlushThreshold: DefaultMemtableFlushThreshold,
	}
}

// signalBundle holds everything for one signal type (traces or logs).
type signalBundle struct {
	walDir     string                          // directory containing numbered WAL files
	walSeq     int64                           // current WAL file sequence number
	wal        *wal.Writer
	subdir     string                          // subdirectory under dataDir, e.g. "traces" / "logs"
	man        *manifest.Manager               // per-signal manifest
	set        *memtable.Set                   // dual memtable
	locator    *index.TraceLocator             // two-level trace index
	mu         sync.RWMutex                    // protects bundle-level mutable fields
	lastWrite  time.Time                       // last time data was written to memtable
	retention  time.Duration                  // TTL for this signal type (default = 7 days)
	compactInterval time.Duration             // compaction ticker interval
}

// walPath returns the active WAL file path.
func (b *signalBundle) walPath() string {
	return filepath.Join(b.walDir, fmt.Sprintf("%06d.wal", b.walSeq))
}

// walFileName returns the active WAL file name (without directory).
func (b *signalBundle) walFileName() string {
	return fmt.Sprintf("%06d.wal", b.walSeq)
}

// syncDir syncs the directory to ensure rename is durable.
func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

type block2Store struct {
	dataDir string
	cfg     StoreConfig

	traceBundle  *signalBundle
	logBundle    *signalBundle
	metricBundle *signalBundle

	stopFlush chan struct{}
	flushDone sync.WaitGroup
}

// NewBlock2Store creates a store with test-friendly defaults (1KB flush threshold).
func NewBlock2Store(dataDir string) (Store, error) {
	return NewBlock2StoreWithConfig(dataDir, StoreConfig{
		MemtableFlushThreshold: TestMemtableFlushThreshold,
	})
}

// NewBlock2StoreWithConfig creates a store with the given configuration.
func NewBlock2StoreWithConfig(dataDir string, cfg StoreConfig) (Store, error) {
	if dataDir == "" {
		dataDir = "."
	}
	if cfg.MemtableFlushThreshold <= 0 {
		cfg.MemtableFlushThreshold = DefaultMemtableFlushThreshold
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("block2: mkdir: %w", err)
	}

	s := &block2Store{
		dataDir:   dataDir,
		cfg:       cfg,
		stopFlush: make(chan struct{}),
	}

	// --- Trace bundle ---
	tb, err := s.createBundle("traces")
	if err != nil {
		return nil, fmt.Errorf("block2: trace bundle: %w", err)
	}
	s.traceBundle = tb

	// --- Log bundle ---
	lb, err := s.createBundle("logs")
	if err != nil {
		return nil, fmt.Errorf("block2: log bundle: %w", err)
	}
	s.logBundle = lb

	// --- Metric bundle ---
	mb, err := s.createBundle("metrics")
	if err != nil {
		return nil, fmt.Errorf("block2: metric bundle: %w", err)
	}
	s.metricBundle = mb

	// Rebuild locators from existing segments.
	if err := s.rebuildLocator(s.traceBundle); err != nil {
		return nil, fmt.Errorf("block2: rebuild trace locator: %w", err)
	}
	if err := s.rebuildLocator(s.logBundle); err != nil {
		return nil, fmt.Errorf("block2: rebuild log locator: %w", err)
	}
	if err := s.rebuildLocator(s.metricBundle); err != nil {
		return nil, fmt.Errorf("block2: rebuild metric locator: %w", err)
	}

	// Replay WAL for three bundles.
	if err := s.replayBundle(s.traceBundle, func(raw []byte) {
		key, data := s.extractTraceSortKey(raw)
		s.traceBundle.set.Insert(key, data)
	}); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("block2: replay traces: %w", err)
	}
	if err := s.replayBundle(s.logBundle, func(raw []byte) {
		key, data := s.extractLogSortKey(raw)
		s.logBundle.set.Insert(key, data)
	}); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("block2: replay logs: %w", err)
	}
	if err := s.replayBundle(s.metricBundle, func(raw []byte) {
		key, data := s.extractMetricSortKey(raw)
		s.metricBundle.set.Insert(key, data)
	}); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("block2: replay metrics: %w", err)
	}

	// If anything is in memtable, flush now (covers immediate-restoration cases).
	if s.traceBundle.set.ActiveSize() > 0 {
		_ = s.flushBundle(s.traceBundle)
	}
	if s.logBundle.set.ActiveSize() > 0 {
		_ = s.flushBundle(s.logBundle)
	}
	if s.metricBundle.set.ActiveSize() > 0 {
		_ = s.flushBundle(s.metricBundle)
	}

	s.flushDone.Add(1)
	go s.flushMonitor()

	// Start TTL + Compaction sweeper.
	s.flushDone.Add(1)
	go s.ttlAndCompactionSweep()

	return s, nil
}

func (s *block2Store) createBundle(subdir string) (*signalBundle, error) {
	dir := filepath.Join(s.dataDir, subdir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Find the highest-numbered WAL file in the directory.
	walSeq := highestWALSeq(dir)
	if walSeq == 0 {
		walSeq = 1 // start from 000001.wal
	}

	walPath := filepath.Join(dir, fmt.Sprintf("%06d.wal", walSeq))
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("wal open %s: %w", walPath, err)
	}
	m, err := manifest.Open(dir)
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("manifest open %s: %w", dir, err)
	}
	return &signalBundle{
		walDir:          dir,
		walSeq:          walSeq,
		wal:             w,
		subdir:          subdir,
		man:             m,
		set:             memtable.NewSet(),
		locator:         index.NewTraceLocator(),
		retention:       DefaultRetention,
		compactInterval: CompactionInterval,
	}, nil
}

// highestWALSeq scans a directory for *.wal files (not .tombstone) and returns
// the highest sequence number found, or 0 if none exist.
func highestWALSeq(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var maxSeq int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".wal") || strings.HasSuffix(name, ".tombstone") {
			continue
		}
		// Parse "000003.wal" → 3
		base := strings.TrimSuffix(name, ".wal")
		seq, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
}

func (s *block2Store) rebuildLocator(b *signalBundle) error {
	for _, rec := range b.man.AllRecords() {
		idxPath := filepath.Join(s.dataDir, b.subdir, rec.SegmentFile+".trace.index")
		idx, err := index.LoadSegmentTraceIndex(idxPath)
		if err != nil {
			continue
		}
		b.locator.Merge(rec.Sequence, idx)
	}
	return nil
}

func (s *block2Store) replayBundle(b *signalBundle, fn func(raw []byte)) error {
	// Read checkpoint to know where to start replay.
	cp, err := b.man.ReadCheckpoint()
	if err != nil {
		return fmt.Errorf("replay: read checkpoint: %w", err)
	}

	// Find all non-tombstoned WAL files, sorted by sequence.
	walFiles := listWALFiles(b.walDir)
	if len(walFiles) == 0 {
		return nil
	}

	for _, wf := range walFiles {
		walPath := filepath.Join(b.walDir, wf.name)

		var startOffset int64
		if cp.File != "" && wf.name == cp.File {
			startOffset = cp.Offset
		} else if cp.File != "" && wf.seq < walSeqFromName(cp.File) {
			// This WAL file is before the checkpoint — skip entirely.
			continue
		}

		if err := replayWALFile(walPath, startOffset, fn); err != nil {
			return fmt.Errorf("replay %s: %w", wf.name, err)
		}
	}
	return nil
}

type walFileInfo struct {
	name string
	seq  int64
}

// listWALFiles returns all non-tombstoned .wal files sorted by sequence.
func listWALFiles(dir string) []walFileInfo {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var files []walFileInfo
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".wal") || strings.HasSuffix(name, ".tombstone") {
			continue
		}
		seq := walSeqFromName(name)
		if seq > 0 {
			files = append(files, walFileInfo{name: name, seq: seq})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].seq < files[j].seq })
	return files
}

// walSeqFromName parses "000003.wal" → 3.
func walSeqFromName(name string) int64 {
	base := strings.TrimSuffix(name, ".wal")
	seq, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0
	}
	return seq
}

// replayWALFile replays a single WAL file from the given offset.
func replayWALFile(path string, offset int64, fn func(raw []byte)) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	if offset > 0 {
		if _, err := f.Seek(offset, 0); err != nil {
			return fmt.Errorf("seek to %d: %w", offset, err)
		}
	}

	r := wal.NewReader(f)
	for {
		data, err := r.Next()
		if err != nil {
			break // EOF or truncated record — done with this file
		}
		fn(data)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Trace sort key / extraction helpers
// ---------------------------------------------------------------------------

func (s *block2Store) extractTraceSortKey(raw []byte) (memtable.Key, []byte) {
	var rs tracepb.ResourceSpans
	if err := proto.Unmarshal(raw, &rs); err != nil {
		return memtable.Key{}, nil
	}
	svc := ""
	if rs.Resource != nil {
		for _, attr := range rs.Resource.Attributes {
			if attr.Key == "service.name" {
				svc = attr.Value.GetStringValue()
				break
			}
		}
	}
	var timeNano uint64
	var spanID string
	if len(rs.ScopeSpans) > 0 && len(rs.ScopeSpans[0].Spans) > 0 {
		span := rs.ScopeSpans[0].Spans[0]
		timeNano = span.StartTimeUnixNano
		spanID = fmt.Sprintf("%x", span.SpanId)
	}
	if spanID == "" {
		spanID = "0000000000000000"
	}
	key := otelutil.TraceSortKey{TenantID: "default", Service: svc, TimeNano: timeNano, SpanID: spanID}
	return memtable.Key{DType: otelutil.TypeTrace, Key: key.Key()}, raw
}

func (s *block2Store) extractTraceIDFromData(raw []byte) string {
	var rs tracepb.ResourceSpans
	if err := proto.Unmarshal(raw, &rs); err != nil {
		return ""
	}
	if len(rs.ScopeSpans) > 0 && len(rs.ScopeSpans[0].Spans) > 0 {
		return fmt.Sprintf("%x", rs.ScopeSpans[0].Spans[0].TraceId)
	}
	return ""
}

// ---------------------------------------------------------------------------
// Log sort key / extraction helpers
// ---------------------------------------------------------------------------

func (s *block2Store) extractLogSortKey(raw []byte) (memtable.Key, []byte) {
	var rl logspb.ResourceLogs
	if err := proto.Unmarshal(raw, &rl); err != nil {
		return memtable.Key{}, nil
	}
	svc := ""
	if rl.Resource != nil {
		for _, attr := range rl.Resource.Attributes {
			if attr.Key == "service.name" {
				svc = attr.Value.GetStringValue()
				break
			}
		}
	}
	var timeNano uint64
	var traceID string
	var logID string
	if len(rl.ScopeLogs) > 0 && len(rl.ScopeLogs[0].LogRecords) > 0 {
		lr := rl.ScopeLogs[0].LogRecords[0]
		timeNano = lr.ObservedTimeUnixNano
		if timeNano == 0 {
			timeNano = lr.TimeUnixNano
		}
		traceID = fmt.Sprintf("%x", lr.TraceId)
		logID = fmt.Sprintf("%d_%x", lr.SeverityNumber, lr.SpanId)
	}
	if traceID == "" {
		traceID = "none"
	}
	if logID == "" {
		logID = "none"
	}
	key := otelutil.LogSortKey{TenantID: "default", Service: svc, TimeNano: timeNano, TraceID: traceID, LogID: logID}
	return memtable.Key{DType: otelutil.TypeLog, Key: key.Key()}, raw
}

func (s *block2Store) extractLogTraceID(raw []byte) string {
	var rl logspb.ResourceLogs
	if err := proto.Unmarshal(raw, &rl); err != nil {
		return ""
	}
	if len(rl.ScopeLogs) > 0 && len(rl.ScopeLogs[0].LogRecords) > 0 {
		return fmt.Sprintf("%x", rl.ScopeLogs[0].LogRecords[0].TraceId)
	}
	return ""
}

// ---------------------------------------------------------------------------
// Metric sort key / extraction helpers
// ---------------------------------------------------------------------------

func (s *block2Store) extractMetricSortKey(raw []byte) (memtable.Key, []byte) {
	var rm metricspb.ResourceMetrics
	if err := proto.Unmarshal(raw, &rm); err != nil {
		return memtable.Key{}, nil
	}
	svc := ""
	if rm.Resource != nil {
		for _, attr := range rm.Resource.Attributes {
			if attr.Key == "service.name" {
				svc = attr.Value.GetStringValue()
				break
			}
		}
	}
	var timeNano uint64
	var metricName string
	if len(rm.ScopeMetrics) > 0 && len(rm.ScopeMetrics[0].Metrics) > 0 {
		m := rm.ScopeMetrics[0].Metrics[0]
		metricName = m.Name
		// Use start_time_unix_nano from first gauge / sum / histogram data point.
		switch d := m.Data.(type) {
		case *metricspb.Metric_Gauge:
			if len(d.Gauge.DataPoints) > 0 {
				timeNano = d.Gauge.DataPoints[0].TimeUnixNano
			}
		case *metricspb.Metric_Sum:
			if len(d.Sum.DataPoints) > 0 {
				timeNano = d.Sum.DataPoints[0].TimeUnixNano
			}
		case *metricspb.Metric_Histogram:
			if len(d.Histogram.DataPoints) > 0 {
				timeNano = d.Histogram.DataPoints[0].TimeUnixNano
			}
		case *metricspb.Metric_Summary:
			if len(d.Summary.DataPoints) > 0 {
				timeNano = d.Summary.DataPoints[0].TimeUnixNano
			}
		}
	}
	if metricName == "" {
		metricName = "none"
	}
	key := otelutil.MetricSortKey{TenantID: "default", Service: svc, MetricName: metricName, TimeNano: timeNano}
	return memtable.Key{DType: otelutil.TypeMetric, Key: key.Key()}, raw
}

// insert adds an item to the active memtable and records the write time.
func (b *signalBundle) insert(key memtable.Key, value []byte) {
	b.set.Insert(key, value)
	b.lastWrite = time.Now()
}

// flushIdleSize returns the effective size for flush decisions.
// If idle timeout has elapsed, returns the threshold to force a flush.
func (b *signalBundle) flushIdleSize(threshold int64) int64 {
	if time.Since(b.lastWrite) > MemtableMaxIdleFlush && b.set.ActiveSize() > 0 {
		return threshold
	}
	return b.set.ActiveSize()
}

// ---------------------------------------------------------------------------
// WriteTraces
// ---------------------------------------------------------------------------

func (s *block2Store) WriteTraces(ctx context.Context, tenantID string, spans []*tracepb.ResourceSpans) error {
	batch := make([][]byte, 0, len(spans))
	for _, rs := range spans {
		data, err := proto.Marshal(rs)
		if err != nil {
			return err
		}
		batch = append(batch, data)
	}
	s.traceBundle.mu.Lock()
	defer s.traceBundle.mu.Unlock()
	if err := s.traceBundle.wal.AppendBatch(batch); err != nil {
		return err
	}
	if err := s.traceBundle.wal.Sync(); err != nil {
		return err
	}
	for _, data := range batch {
		key, data2 := s.extractTraceSortKey(data)
		s.traceBundle.insert(key, data2)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ReadTraces
// ---------------------------------------------------------------------------

func (s *block2Store) ReadTraces(ctx context.Context, req TraceReadRequest) ([]*tracepb.ResourceSpans, error) {
	var result []*tracepb.ResourceSpans

	active, flushing := s.traceBundle.set.Snapshot()
	for _, tbl := range []*memtable.Memtable{active, flushing} {
		if tbl == nil {
			continue
		}
		tbl.AscendByType(otelutil.TypeTrace, func(key memtable.Key, value []byte) bool {
			var rs tracepb.ResourceSpans
			if err := proto.Unmarshal(value, &rs); err != nil {
				return true
			}
			if !s.traceKeyMatches(rs, req) {
				return true
			}
			result = append(result, &rs)
			return true
		})
	}

	for _, rec := range s.traceBundle.man.AllRecords() {
		if rec.MinKey.TenantID != req.TenantID && rec.MaxKey.TenantID != req.TenantID {
			continue
		}
		if req.Service != "" && rec.MinKey.Service != req.Service && rec.MaxKey.Service != req.Service {
			continue
		}
		if req.EndTime > 0 && uint64(req.EndTime) <= rec.MinKey.TimeNano {
			continue
		}
		if req.StartTime > 0 && uint64(req.StartTime) >= rec.MaxKey.TimeNano {
			continue
		}
		if req.Service != "" && rec.BloomFile != "" {
			bloomPath := filepath.Join(s.dataDir, s.traceBundle.subdir, rec.BloomFile)
			if bf, err := bloom.Load(bloomPath); err == nil {
				target := req.TenantID + "\x00" + req.Service
				if !bf.MightContain(target) {
					continue
				}
			}
		}

		segPath := filepath.Join(s.dataDir, s.traceBundle.subdir, rec.SegmentFile)
		seg, err := segment.Open(segPath)
		if err != nil {
			continue
		}

		if req.StartTime > 0 && len(seg.Footer().Blocks) > 0 {
			target := otelutil.TraceSortKey{TenantID: req.TenantID, Service: req.Service, TimeNano: uint64(req.StartTime), SpanID: "0000000000000000"}
			_ = seg.SeekTo(target.Key())
		}

		for {
			data, err := seg.Next()
			if err != nil {
				break
			}
			var rs tracepb.ResourceSpans
			if err := proto.Unmarshal(data, &rs); err != nil {
				continue
			}
			if !s.traceKeyMatches(rs, req) {
				continue
			}
			result = append(result, &rs)
		}
		_ = seg.Close()
	}

	return result, nil
}

func (s *block2Store) traceKeyMatches(rs tracepb.ResourceSpans, req TraceReadRequest) bool {
	svc := ""
	if rs.Resource != nil {
		for _, a := range rs.Resource.Attributes {
			if a.Key == "service.name" {
				svc = a.Value.GetStringValue()
				break
			}
		}
	}
	tid := "default"
	if req.TenantID != "" {
		tid = req.TenantID
	}
	if tid != "default" && tid != "" {
		// tenant check not strictly enforced in this prototype; default passes.
	}
	if req.Service != "" && svc != req.Service {
		return false
	}
	if req.TraceID != "" {
		var found bool
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				if fmt.Sprintf("%x", sp.TraceId) == req.TraceID {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return false
		}
	}
	// Time check — use first span.
	var timeNano uint64
	if len(rs.ScopeSpans) > 0 && len(rs.ScopeSpans[0].Spans) > 0 {
		timeNano = rs.ScopeSpans[0].Spans[0].StartTimeUnixNano
	}
	if req.StartTime > 0 && int64(timeNano) < req.StartTime {
		return false
	}
	if req.EndTime > 0 && int64(timeNano) >= req.EndTime {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// ReadTraceByID
// ---------------------------------------------------------------------------

func (s *block2Store) ReadTraceByID(ctx context.Context, tenantID, traceID string) ([]*tracepb.ResourceSpans, error) {
	var result []*tracepb.ResourceSpans

	seqs := s.traceBundle.locator.Find(traceID)
	for _, seq := range seqs {
		segPath := filepath.Join(s.dataDir, s.traceBundle.subdir, fmt.Sprintf("sst-%08d.pb", seq))
		idxPath := segPath + ".trace.index"

		idx, err := index.LoadSegmentTraceIndex(idxPath)
		if err != nil {
			continue
		}
		offsets, ok := idx.Offsets[traceID]
		if !ok || len(offsets) == 0 {
			continue
		}

		seg, err := segment.Open(segPath)
		if err != nil {
			continue
		}

		for _, off := range offsets {
			if err := seg.SeekToOffset(off); err != nil {
				continue
			}
			data, err := seg.Next()
			if err != nil {
				continue
			}
			var rs tracepb.ResourceSpans
			if err := proto.Unmarshal(data, &rs); err != nil {
				continue
			}
			result = append(result, &rs)
		}
		_ = seg.Close()
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// WriteLogs
// ---------------------------------------------------------------------------

func (s *block2Store) WriteLogs(ctx context.Context, tenantID string, logs []*logspb.ResourceLogs) error {
	batch := make([][]byte, 0, len(logs))
	for _, rl := range logs {
		data, err := proto.Marshal(rl)
		if err != nil {
			return err
		}
		batch = append(batch, data)
	}
	s.logBundle.mu.Lock()
	defer s.logBundle.mu.Unlock()
	if err := s.logBundle.wal.AppendBatch(batch); err != nil {
		return err
	}
	if err := s.logBundle.wal.Sync(); err != nil {
		return err
	}
	for _, data := range batch {
		key, data2 := s.extractLogSortKey(data)
		s.logBundle.insert(key, data2)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ReadLogs
// ---------------------------------------------------------------------------

func (s *block2Store) ReadLogs(ctx context.Context, req LogReadRequest) ([]*logspb.ResourceLogs, error) {
	var result []*logspb.ResourceLogs

	active, flushing := s.logBundle.set.Snapshot()
	for _, tbl := range []*memtable.Memtable{active, flushing} {
		if tbl == nil {
			continue
		}
		tbl.AscendByType(otelutil.TypeLog, func(key memtable.Key, value []byte) bool {
			var rl logspb.ResourceLogs
			if err := proto.Unmarshal(value, &rl); err != nil {
				return true
			}
			if !s.logKeyMatches(rl, req) {
				return true
			}
			result = append(result, &rl)
			return true
		})
	}

	for _, rec := range s.logBundle.man.AllRecords() {
		if rec.MinKey.TenantID != req.TenantID && rec.MaxKey.TenantID != req.TenantID {
			continue
		}
		if req.Service != "" && rec.MinKey.Service != req.Service && rec.MaxKey.Service != req.Service {
			continue
		}
		if req.EndTime > 0 && uint64(req.EndTime) <= rec.MinKey.TimeNano {
			continue
		}
		if req.StartTime > 0 && uint64(req.StartTime) >= rec.MaxKey.TimeNano {
			continue
		}
		if req.Service != "" && rec.BloomFile != "" {
			bloomPath := filepath.Join(s.dataDir, s.logBundle.subdir, rec.BloomFile)
			if bf, err := bloom.Load(bloomPath); err == nil {
				target := req.TenantID + "\x00" + req.Service
				if !bf.MightContain(target) {
					continue
				}
			}
		}

		segPath := filepath.Join(s.dataDir, s.logBundle.subdir, rec.SegmentFile)
		seg, err := segment.Open(segPath)
		if err != nil {
			continue
		}

		if req.StartTime > 0 && len(seg.Footer().Blocks) > 0 {
			target := otelutil.LogSortKey{TenantID: req.TenantID, Service: req.Service, TimeNano: uint64(req.StartTime)}
			_ = seg.SeekTo(target.Key())
		}

		for {
			data, err := seg.Next()
			if err != nil {
				break
			}
			var rl logspb.ResourceLogs
			if err := proto.Unmarshal(data, &rl); err != nil {
				continue
			}
			if !s.logKeyMatches(rl, req) {
				continue
			}
			result = append(result, &rl)
		}
		_ = seg.Close()
	}

	return result, nil
}

func (s *block2Store) logKeyMatches(rl logspb.ResourceLogs, req LogReadRequest) bool {
	svc := ""
	if rl.Resource != nil {
		for _, a := range rl.Resource.Attributes {
			if a.Key == "service.name" {
				svc = a.Value.GetStringValue()
				break
			}
		}
	}
	if req.Service != "" && svc != req.Service {
		return false
	}
	// Time check — use first log record.
	var timeNano uint64
	var severity int32
	if len(rl.ScopeLogs) > 0 && len(rl.ScopeLogs[0].LogRecords) > 0 {
		lr := rl.ScopeLogs[0].LogRecords[0]
		timeNano = lr.ObservedTimeUnixNano
		if timeNano == 0 {
			timeNano = lr.TimeUnixNano
		}
		severity = int32(lr.SeverityNumber)
	}
	if req.StartTime > 0 && int64(timeNano) < req.StartTime {
		return false
	}
	if req.EndTime > 0 && int64(timeNano) >= req.EndTime {
		return false
	}
	if req.Severity > 0 && severity != req.Severity {
		return false
	}
	if req.TraceID != "" {
		var found bool
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				if fmt.Sprintf("%x", lr.TraceId) == req.TraceID {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func (s *block2Store) Close() error {
	close(s.stopFlush)
	s.flushDone.Wait()
	if s.traceBundle != nil {
		_ = s.traceBundle.wal.Close()
	}
	if s.logBundle != nil {
		_ = s.logBundle.wal.Close()
	}
	if s.metricBundle != nil {
		_ = s.metricBundle.wal.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Flush monitor (single goroutine polls both bundles)
// ---------------------------------------------------------------------------

func (s *block2Store) flushMonitor() {
	defer s.flushDone.Done()
	ticker := time.NewTicker(FlushPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopFlush:
			if s.traceBundle.set.ActiveSize() > 0 {
				_ = s.flushBundle(s.traceBundle)
			}
			if s.logBundle.set.ActiveSize() > 0 {
				_ = s.flushBundle(s.logBundle)
			}
			if s.metricBundle.set.ActiveSize() > 0 {
				_ = s.flushBundle(s.metricBundle)
			}
			return
		case <-ticker.C:
			thresh := s.cfg.MemtableFlushThreshold
			if s.traceBundle.flushIdleSize(thresh) >= thresh {
				_ = s.flushBundle(s.traceBundle)
			}
			if s.logBundle.flushIdleSize(thresh) >= thresh {
				_ = s.flushBundle(s.logBundle)
			}
			if s.metricBundle.flushIdleSize(thresh) >= thresh {
				_ = s.flushBundle(s.metricBundle)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// TTL + Compaction sweeper
// ---------------------------------------------------------------------------
// TTL + Compaction sweeper
// ---------------------------------------------------------------------------

func (s *block2Store) ttlAndCompactionSweep() {
	defer s.flushDone.Done()
	// Production tuning: per-bundle interval config (default: 1h) — can be overridden via signalBundle.compactInterval.
	interval := s.traceBundle.compactInterval
	if interval <= 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopFlush:
			return
		case <-ticker.C:
			s.sweepBundleTTL(s.traceBundle)
			s.sweepBundleTTL(s.logBundle)
			s.sweepBundleTTL(s.metricBundle)
			_ = s.compactBundle(s.traceBundle)
			_ = s.compactBundle(s.logBundle)
			_ = s.compactBundle(s.metricBundle)
			s.cleanupWALTombstones(s.traceBundle)
			s.cleanupWALTombstones(s.logBundle)
			s.cleanupWALTombstones(s.metricBundle)
		}
	}
}

// ---------------------------------------------------------------------------
// Delete APIs
// ---------------------------------------------------------------------------

func (s *block2Store) DeleteTraces(ctx context.Context, tenantID string, req TraceReadRequest) error {
	// Find matching traces and insert tombstones.
	active, flushing := s.traceBundle.set.Snapshot()
	for _, tbl := range []*memtable.Memtable{active, flushing} {
		if tbl == nil {
			continue
		}
		tbl.AscendByType(otelutil.TypeTrace, func(key memtable.Key, value []byte) bool {
			var rs tracepb.ResourceSpans
			if err := proto.Unmarshal(value, &rs); err != nil {
				return true
			}
			if !s.traceKeyMatches(rs, req) {
				return true
			}
			// Write tombstone to WAL.
			ts := fmt.Sprintf(`{"deleted":true,"key":%q}`, key.Key)
			_ = s.traceBundle.wal.Append([]byte(ts))
			// Insert tombstone into memtable.
			s.traceBundle.set.InsertTombstone(key)
			return true
		})
	}
	_ = s.traceBundle.wal.Sync()
	return nil
}

func (s *block2Store) DeleteLogs(ctx context.Context, tenantID string, req LogReadRequest) error {
	active, flushing := s.logBundle.set.Snapshot()
	for _, tbl := range []*memtable.Memtable{active, flushing} {
		if tbl == nil {
			continue
		}
		tbl.AscendByType(otelutil.TypeLog, func(key memtable.Key, value []byte) bool {
			var rl logspb.ResourceLogs
			if err := proto.Unmarshal(value, &rl); err != nil {
				return true
			}
			if !s.logKeyMatches(rl, req) {
				return true
			}
			ts := fmt.Sprintf(`{"deleted":true,"key":%q}`, key.Key)
			_ = s.logBundle.wal.Append([]byte(ts))
			s.logBundle.set.InsertTombstone(key)
			return true
		})
	}
	_ = s.logBundle.wal.Sync()
	return nil
}

func (s *block2Store) DeleteMetrics(ctx context.Context, tenantID string, req MetricReadRequest) error {
	active, flushing := s.metricBundle.set.Snapshot()
	for _, tbl := range []*memtable.Memtable{active, flushing} {
		if tbl == nil {
			continue
		}
		tbl.AscendByType(otelutil.TypeMetric, func(key memtable.Key, value []byte) bool {
			var rm metricspb.ResourceMetrics
			if err := proto.Unmarshal(value, &rm); err != nil {
				return true
			}
			if !s.metricKeyMatches(rm, req) {
				return true
			}
			ts := fmt.Sprintf(`{"deleted":true,"key":%q}`, key.Key)
			_ = s.metricBundle.wal.Append([]byte(ts))
			s.metricBundle.set.InsertTombstone(key)
			return true
		})
	}
	_ = s.metricBundle.wal.Sync()
	return nil
}

// ---------------------------------------------------------------------------
// Sweep bundle
// ---------------------------------------------------------------------------

func (s *block2Store) sweepBundleTTL(b *signalBundle) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	retention := b.retention
	if retention <= 0 {
		retention = DefaultRetention
	}
	cutoff := time.Now().Add(-retention).UnixNano()
	var removedSeqs []int64
	for _, rec := range b.man.AllRecords() {
		if rec.CreatedAt > 0 && rec.CreatedAt < cutoff {
			segPath := filepath.Join(s.dataDir, b.subdir, rec.SegmentFile)
			bloomPath := filepath.Join(s.dataDir, b.subdir, rec.BloomFile)
			idxPath := segPath + ".trace.index"
			_ = os.Remove(segPath)
			_ = os.Remove(bloomPath)
			_ = os.Remove(idxPath)
			removedSeqs = append(removedSeqs, rec.Sequence)
		}
	}
	if len(removedSeqs) > 0 {
		_ = b.man.RemoveRecords(func(rec manifest.Record) bool {
			for _, seq := range removedSeqs {
				if rec.Sequence == seq {
					return false
				}
			}
			return true
		})
	}
}

// cleanupWALTombstones deletes flushed WAL files marked with .tombstone suffix.
func (s *block2Store) cleanupWALTombstones(b *signalBundle) {
	if b == nil {
		return
	}
	matches, _ := filepath.Glob(filepath.Join(b.walDir, "*.wal.tombstone"))
	for _, m := range matches {
		_ = os.Remove(m)
	}
}

// compactBundle merges adjacent segments and rewrites manifest.
func (s *block2Store) compactBundle(b *signalBundle) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	records := b.man.AllRecords()
	if len(records) < 2 {
		return nil
	}
	// Find two adjacent old segments to merge.
	// Simple heuristic: merge segments 0+1.
	oldRecs := records[:2]

	mergePath := filepath.Join(s.dataDir, b.subdir, fmt.Sprintf("sst-%08d.pb", b.man.NextSequence()))
	mergeSeg, err := segment.Create(mergePath)
	if err != nil {
		return fmt.Errorf("compact: create segment: %w", err)
	}

	var bloomKeys []string
	var minKey, maxKey manifest.SortKey
	var first bool
	// TODO: rebuild trace index from old segments' trace.index files.

	for _, rec := range oldRecs {
		segPath := filepath.Join(s.dataDir, b.subdir, rec.SegmentFile)
		seg, err := segment.Open(segPath)
		if err != nil {
			continue
		}
		for {
			data, err := seg.Next()
			if err != nil {
				break
			}
			if len(data) == 0 {
				continue
			}
			// Skip tombstones in compaction.
			if len(data) >= 9 {
				if bytes.HasPrefix(data, []byte("{'deleted':true")) {
					continue
				}
			}
			_ = mergeSeg.Append(data)
			bloomKeys = append(bloomKeys, extractServiceBloomKey(string(data)))
		}
		if len(seg.Footer().Blocks) > 0 {
			for _, blk := range seg.Footer().Blocks {
				if !first {
					first = true
					minKey = manifest.SortKeyFromString(blk.Key)
					maxKey = minKey
				} else {
					mk := manifest.SortKeyFromString(blk.Key)
					if blk.Key < minKey.Str {
						minKey = mk
					}
					if blk.Key > maxKey.Str {
						maxKey = mk
					}
				}
			}
		}
		// TODO: rebuild trace index from old segments' trace.index files.
		_ = seg.Close()
	}

	if err := mergeSeg.Close(""); err != nil {
		return fmt.Errorf("compact: close segment: %w", err)
	}

	bf := bloom.Build(bloomKeys, 0.01)
	bloomPath := mergePath + ".bloom"
	_ = bf.Save(bloomPath)

	// Use oldest CreatedAt among merged so TTL still applies.
	var oldestCreated int64
	for _, rec := range oldRecs {
		if oldestCreated == 0 || (rec.CreatedAt > 0 && rec.CreatedAt < oldestCreated) {
			oldestCreated = rec.CreatedAt
		}
	}

	newRec := manifest.Record{
		Sequence:      b.man.NextSequence(),
		SegmentFile:   filepath.Base(mergePath),
		BloomFile:     filepath.Base(bloomPath),
		WALCheckpoint: "compact",
		MinKey:        minKey,
		MaxKey:        maxKey,
		SpanCount:     int64(len(bloomKeys)),
		CreatedAt:     oldestCreated,
	}
	if oldestCreated == 0 {
		newRec.CreatedAt = time.Now().UnixNano()
	}

	oldSeqs := []int64{oldRecs[0].Sequence, oldRecs[1].Sequence}
	if err := b.man.CompactRecords(oldSeqs, []manifest.Record{newRec}); err != nil {
		return fmt.Errorf("compact: manifest rewrite: %w", err)
	}

	// Remove old files.
	for _, rec := range oldRecs {
		_ = os.Remove(filepath.Join(s.dataDir, b.subdir, rec.SegmentFile))
		_ = os.Remove(filepath.Join(s.dataDir, b.subdir, rec.BloomFile))
		_ = os.Remove(filepath.Join(s.dataDir, b.subdir, rec.SegmentFile+".trace.index"))
	}

	return nil
}

// ---------------------------------------------------------------------------
// Flush bundle
// ---------------------------------------------------------------------------

func (s *block2Store) flushBundle(b *signalBundle) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	flushing := b.set.Rotate()
	if flushing == nil {
		return nil
	}
	defer b.set.ClearFlushing()

	seq := b.man.NextSequence()
	segPath := filepath.Join(s.dataDir, b.subdir, fmt.Sprintf("sst-%08d.pb", seq))

	seg, err := segment.Create(segPath)
	if err != nil {
		return fmt.Errorf("flush: create segment in %s: %w", b.subdir, err)
	}

	var minKey, maxKey manifest.SortKey
	var first bool
	var bloomKeys []string
	var lastSortKey string

	traceIndex := index.SegmentTraceIndex{Offsets: make(map[string][]int64)}

	flushing.Ascend(func(key memtable.Key, value []byte) bool {
		offset, _ := seg.CurrentOffset()

		var traceID string
		if key.DType == otelutil.TypeTrace {
			traceID = s.extractTraceIDFromData(value)
		} else if key.DType == otelutil.TypeLog {
			traceID = s.extractLogTraceID(value)
		}
		if traceID != "" && traceID != "none" {
			traceIndex.Offsets[traceID] = append(traceIndex.Offsets[traceID], offset)
		}

		_ = seg.Append(value)
		mk := manifest.SortKeyFromString(key.Key)
		if !first {
			minKey, maxKey = mk, mk
			first = true
		} else {
			if key.Key < minKey.Str {
				minKey = mk
			}
			if key.Key > maxKey.Str {
				maxKey = mk
			}
		}
		bloomKeys = append(bloomKeys, extractServiceBloomKey(key.Key))
		lastSortKey = key.Key
		return true
	})

	if err := seg.Close(lastSortKey); err != nil {
		return fmt.Errorf("flush: close segment: %w", err)
	}

	idxPath := segPath + ".trace.index"
	if err := traceIndex.Save(idxPath); err != nil {
		return fmt.Errorf("flush: save trace index: %w", err)
	}

	b.locator.Merge(seq, &traceIndex)

	bf := bloom.Build(bloomKeys, 0.01)
	bloomPath := filepath.Join(s.dataDir, b.subdir, fmt.Sprintf("sst-%08d.bloom", seq))
	if err := bf.Save(bloomPath); err != nil {
		return fmt.Errorf("flush: save bloom: %w", err)
	}

	walName := b.walFileName()
	offset := b.wal.Offset()

	rec := manifest.Record{
		Sequence:      seq,
		SegmentFile:   filepath.Base(segPath),
		BloomFile:     filepath.Base(bloomPath),
		WALCheckpoint: fmt.Sprintf("%s:%d", walName, offset),
		MinKey:        minKey,
		MaxKey:        maxKey,
		SpanCount:     int64(len(bloomKeys)),
		Checksum:      "",
		CreatedAt:     time.Now().UnixNano(),
	}
	if err := b.man.AppendRecord(rec); err != nil {
		return fmt.Errorf("flush: manifest: %w", err)
	}

	// Write checkpoint pointing to the NEXT WAL file (post-rotation).
	nextWALName := fmt.Sprintf("%06d.wal", b.walSeq+1)
	if err := b.man.WriteCheckpoint(nextWALName, 0); err != nil {
		return fmt.Errorf("flush: checkpoint: %w", err)
	}

	// Sync directory to ensure rename is durable.
	if err := syncDir(b.walDir); err != nil {
		return fmt.Errorf("flush: sync dir: %w", err)
	}

	// Rotate WAL: close current, mark as tombstone, open next.
	oldWAL := b.wal
	oldPath := b.walPath()
	_ = oldWAL.Close()
	_ = os.Rename(oldPath, oldPath+".tombstone")
	b.walSeq++
	w, err := wal.Open(b.walPath())
	if err != nil {
		return fmt.Errorf("flush: reopen wal: %w", err)
	}
	b.wal = w

	return nil
}



func (s *block2Store) WriteMetrics(ctx context.Context, tenantID string, metrics []*metricspb.ResourceMetrics) error {
	batch := make([][]byte, 0, len(metrics))
	for _, rm := range metrics {
		data, err := proto.Marshal(rm)
		if err != nil {
			return err
		}
		batch = append(batch, data)
	}
	s.metricBundle.mu.Lock()
	defer s.metricBundle.mu.Unlock()
	if err := s.metricBundle.wal.AppendBatch(batch); err != nil {
		return err
	}
	if err := s.metricBundle.wal.Sync(); err != nil {
		return err
	}
	for _, data := range batch {
		key, data2 := s.extractMetricSortKey(data)
		s.metricBundle.insert(key, data2)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ReadMetrics
// ---------------------------------------------------------------------------

func (s *block2Store) ReadMetrics(ctx context.Context, req MetricReadRequest) ([]*metricspb.ResourceMetrics, error) {
	var result []*metricspb.ResourceMetrics

	active, flushing := s.metricBundle.set.Snapshot()
	for _, tbl := range []*memtable.Memtable{active, flushing} {
		if tbl == nil {
			continue
		}
		tbl.AscendByType(otelutil.TypeMetric, func(key memtable.Key, value []byte) bool {
			var rm metricspb.ResourceMetrics
			if err := proto.Unmarshal(value, &rm); err != nil {
				return true
			}
			if !s.metricKeyMatches(rm, req) {
				return true
			}
			result = append(result, &rm)
			return true
		})
	}

	for _, rec := range s.metricBundle.man.AllRecords() {
		if rec.MinKey.TenantID != req.TenantID && rec.MaxKey.TenantID != req.TenantID {
			continue
		}
		if req.Service != "" && rec.MinKey.Service != req.Service && rec.MaxKey.Service != req.Service {
			continue
		}
		if req.EndTime > 0 && uint64(req.EndTime) <= rec.MinKey.TimeNano {
			continue
		}
		if req.StartTime > 0 && uint64(req.StartTime) >= rec.MaxKey.TimeNano {
			continue
		}
		if req.Service != "" && rec.BloomFile != "" {
			bloomPath := filepath.Join(s.dataDir, s.metricBundle.subdir, rec.BloomFile)
			if bf, err := bloom.Load(bloomPath); err == nil {
				target := req.TenantID + "\x00" + req.Service
				if !bf.MightContain(target) {
					continue
				}
			}
		}

		segPath := filepath.Join(s.dataDir, s.metricBundle.subdir, rec.SegmentFile)
		seg, err := segment.Open(segPath)
		if err != nil {
			continue
		}

		if req.StartTime > 0 && len(seg.Footer().Blocks) > 0 {
			target := otelutil.MetricSortKey{TenantID: req.TenantID, Service: req.Service, MetricName: req.MetricName, TimeNano: uint64(req.StartTime)}
			_ = seg.SeekTo(target.Key())
		}

		for {
			data, err := seg.Next()
			if err != nil {
				break
			}
			var rm metricspb.ResourceMetrics
			if err := proto.Unmarshal(data, &rm); err != nil {
				continue
			}
			if !s.metricKeyMatches(rm, req) {
				continue
			}
			result = append(result, &rm)
		}
		_ = seg.Close()
	}

	return result, nil
}

func extractServiceBloomKey(sortKey string) string {
	parts := splitNul(sortKey)
	if len(parts) < 2 {
		return sortKey
	}
	return parts[0] + "\x00" + parts[1]
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

func (s *block2Store) metricKeyMatches(rm metricspb.ResourceMetrics, req MetricReadRequest) bool {
	svc := ""
	if rm.Resource != nil {
		for _, a := range rm.Resource.Attributes {
			if a.Key == "service.name" {
				svc = a.Value.GetStringValue()
				break
			}
		}
	}
	if req.Service != "" && svc != req.Service {
		return false
	}
	// Metric name + time check.
	var timeNano uint64
	var metricName string
	var mType string
	if len(rm.ScopeMetrics) > 0 && len(rm.ScopeMetrics[0].Metrics) > 0 {
		m := rm.ScopeMetrics[0].Metrics[0]
		metricName = m.Name
		switch d := m.Data.(type) {
		case *metricspb.Metric_Gauge:
			mType = "gauge"
			if len(d.Gauge.DataPoints) > 0 {
				timeNano = d.Gauge.DataPoints[0].TimeUnixNano
			}
		case *metricspb.Metric_Sum:
			mType = "sum"
			if len(d.Sum.DataPoints) > 0 {
				timeNano = d.Sum.DataPoints[0].TimeUnixNano
			}
		case *metricspb.Metric_Histogram:
			mType = "histogram"
			if len(d.Histogram.DataPoints) > 0 {
				timeNano = d.Histogram.DataPoints[0].TimeUnixNano
			}
		case *metricspb.Metric_Summary:
			mType = "summary"
			if len(d.Summary.DataPoints) > 0 {
				timeNano = d.Summary.DataPoints[0].TimeUnixNano
			}
		}
	}
	if req.MetricName != "" && metricName != req.MetricName {
		return false
	}
	if req.MetricType != "" && mType != req.MetricType {
		return false
	}
	if req.StartTime > 0 && int64(timeNano) < req.StartTime {
		return false
	}
	if req.EndTime > 0 && int64(timeNano) >= req.EndTime {
		return false
	}
	return true
}
