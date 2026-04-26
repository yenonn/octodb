// Package store provides Block 2 storage: WAL + sorted memtable + segment flush.
// Block 3 additions: block index footer, bloom filter, time-range seek.
// Block 4 additions: two-level trace_id index (global locator + per-segment sidecar).
// Block 4b additions: log + metric signal paths, triple WAL triple memtable.
package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	MemtableFlushThreshold = 1024 // 1 KB — easy to trigger during testing
	FlushPollInterval      = 1 * time.Second
)

// MemtableMaxIdleFlush is the maximum duration between flushes even if size threshold isn't met.
const MemtableMaxIdleFlush = 30 * time.Second

// signalBundle holds everything for one signal type (traces or logs).
type signalBundle struct {
	walPath    string
	wal        *wal.Writer
	subdir     string                          // subdirectory under dataDir, e.g. "traces" / "logs"
	man        *manifest.Manager               // per-signal manifest
	set        *memtable.Set                   // dual memtable
	locator    *index.TraceLocator             // two-level trace index
	mu         sync.RWMutex                    // protects bundle-level mutable fields
	lastWrite  time.Time                       // last time data was written to memtable
}

type block2Store struct {
	dataDir string

	traceBundle  *signalBundle
	logBundle    *signalBundle
	metricBundle *signalBundle

	stopFlush chan struct{}
	flushDone sync.WaitGroup
}

func NewBlock2Store(dataDir string) (Store, error) {
	if dataDir == "" {
		dataDir = "."
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("block2: mkdir: %w", err)
	}

	s := &block2Store{
		dataDir:   dataDir,
		stopFlush: make(chan struct{}),
	}

	// --- Trace bundle ---
	tb, err := s.createBundle("traces", "traces.wal")
	if err != nil {
		return nil, fmt.Errorf("block2: trace bundle: %w", err)
	}
	s.traceBundle = tb

	// --- Log bundle ---
	lb, err := s.createBundle("logs", "logs.wal")
	if err != nil {
		return nil, fmt.Errorf("block2: log bundle: %w", err)
	}
	s.logBundle = lb

	// --- Metric bundle ---
	mb, err := s.createBundle("metrics", "metrics.wal")
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

	return s, nil
}

func (s *block2Store) createBundle(subdir, walBase string) (*signalBundle, error) {
	dir := filepath.Join(s.dataDir, subdir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, walBase)
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
		walPath: walPath,
		wal:     w,
		subdir:  subdir,
		man:     m,
		set:     memtable.NewSet(),
		locator: index.NewTraceLocator(),
	}, nil
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
	if _, err := os.Stat(b.walPath); os.IsNotExist(err) {
		return nil
	}
	return wal.ReplayBytes(b.walPath, func(raw []byte) error {
		fn(raw)
		return nil
	})
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

// flushIdleSize returns true if the bundle should be flushed based on idle timeout.
func (b *signalBundle) flushIdleSize() int64 {
	if time.Since(b.lastWrite) > MemtableMaxIdleFlush && b.set.ActiveSize() > 0 {
		// treat as threshold met to force flush
		return MemtableFlushThreshold
	}
	return b.set.ActiveSize()
}

// ---------------------------------------------------------------------------
// WriteTraces
// ---------------------------------------------------------------------------

func (s *block2Store) WriteTraces(ctx context.Context, tenantID string, spans []*tracepb.ResourceSpans) error {
	for _, rs := range spans {
		data, err := proto.Marshal(rs)
		if err != nil {
			return err
		}
		if err := s.traceBundle.wal.Append(data); err != nil {
			return err
		}
	}
	if err := s.traceBundle.wal.Sync(); err != nil {
		return err
	}
	for _, rs := range spans {
		data, _ := proto.Marshal(rs)
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
	for _, rl := range logs {
		data, err := proto.Marshal(rl)
		if err != nil {
			return err
		}
		if err := s.logBundle.wal.Append(data); err != nil {
			return err
		}
	}
	if err := s.logBundle.wal.Sync(); err != nil {
		return err
	}
	for _, rl := range logs {
		data, _ := proto.Marshal(rl)
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
			if s.traceBundle.flushIdleSize() >= MemtableFlushThreshold {
				_ = s.flushBundle(s.traceBundle)
			}
			if s.logBundle.flushIdleSize() >= MemtableFlushThreshold {
				_ = s.flushBundle(s.logBundle)
			}
			if s.metricBundle.flushIdleSize() >= MemtableFlushThreshold {
				_ = s.flushBundle(s.metricBundle)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Flush bundle
// ---------------------------------------------------------------------------

func (s *block2Store) flushBundle(b *signalBundle) error {
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

	walName := filepath.Base(b.walPath)
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
	}
	if err := b.man.AppendRecord(rec); err != nil {
		return fmt.Errorf("flush: manifest: %w", err)
	}

	if err := b.man.WriteCheckpoint(walName, offset); err != nil {
		return fmt.Errorf("flush: checkpoint: %w", err)
	}

	_ = b.wal.Close()
	_ = os.Truncate(b.walPath, 0)
	w, err := wal.Open(b.walPath)
	if err != nil {
		return fmt.Errorf("flush: reopen wal: %w", err)
	}
	b.wal = w

	return nil
}

// ---------------------------------------------------------------------------
// WriteMetrics
// ---------------------------------------------------------------------------

func (s *block2Store) WriteMetrics(ctx context.Context, tenantID string, metrics []*metricspb.ResourceMetrics) error {
	for _, rm := range metrics {
		data, err := proto.Marshal(rm)
		if err != nil {
			return err
		}
		if err := s.metricBundle.wal.Append(data); err != nil {
			return err
		}
	}
	if err := s.metricBundle.wal.Sync(); err != nil {
		return err
	}
	for _, rm := range metrics {
		data, _ := proto.Marshal(rm)
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
