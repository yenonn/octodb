// Package store provides Block 2 storage: WAL + sorted memtable + segment flush.
// Block 3 additions: block index footer, bloom filter, time-range seek.
// Block 4 additions: two-level trace_id index (global locator + per-segment sidecar).
package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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

type block2Store struct {
	walPath     string
	dataDir     string
	wal         *wal.Writer
	man         *manifest.Manager
	memtableSet *memtable.Set
	locator     *index.TraceLocator
	mu          sync.RWMutex
	stopFlush   chan struct{}
	flushDone   sync.WaitGroup
}

func NewBlock2Store(walPath string) (Store, error) {
	dataDir := filepath.Dir(walPath)
	if dataDir == "" {
		dataDir = "."
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("block2: mkdir: %w", err)
	}

	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("block2: wal: %w", err)
	}

	m, err := manifest.Open(dataDir)
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("block2: manifest: %w", err)
	}

	s := &block2Store{
		walPath:     walPath,
		dataDir:     dataDir,
		wal:         w,
		man:         m,
		memtableSet: memtable.NewSet(),
		locator:     index.NewTraceLocator(),
		stopFlush:   make(chan struct{}),
	}

	if err := s.rebuildLocator(); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("block2: rebuild locator: %w", err)
	}

	if err := s.replayWAL(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("block2: replay: %w", err)
	}

	if s.memtableSet.ActiveSize() > 0 {
		_ = s.flush()
	}

	s.flushDone.Add(1)
	go s.flushMonitor()

	return s, nil
}

func (s *block2Store) rebuildLocator() error {
	for _, rec := range s.man.AllRecords() {
		idxPath := filepath.Join(s.dataDir, rec.SegmentFile+".trace.index")
		idx, err := index.LoadSegmentTraceIndex(idxPath)
		if err != nil {
			continue
		}
		s.locator.Merge(rec.Sequence, idx)
	}
	return nil
}

func (s *block2Store) replayWAL() error {
	if _, err := os.Stat(s.walPath); os.IsNotExist(err) {
		return nil
	}
	return wal.Replay(s.walPath, func(raw []byte, rs *tracepb.ResourceSpans) error {
		key, data := s.extractSortKey(rs)
		if data != nil {
			s.memtableSet.Insert(key, data)
		}
		return nil
	})
}

func (s *block2Store) extractSortKey(rs *tracepb.ResourceSpans) (otelutil.SortKey, []byte) {
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
	key := otelutil.SortKey{TenantID: "default", Service: svc, TimeNano: timeNano, SpanID: spanID}
	data, _ := proto.Marshal(rs)
	return key, data
}

func (s *block2Store) extractTraceID(raw []byte) string {
	var rs tracepb.ResourceSpans
	if err := proto.Unmarshal(raw, &rs); err != nil {
		return ""
	}
	if len(rs.ScopeSpans) > 0 && len(rs.ScopeSpans[0].Spans) > 0 {
		return fmt.Sprintf("%x", rs.ScopeSpans[0].Spans[0].TraceId)
	}
	return ""
}

func (s *block2Store) WriteTraces(ctx context.Context, tenantID string, spans []*tracepb.ResourceSpans) error {
	for _, rs := range spans {
		data, err := proto.Marshal(rs)
		if err != nil {
			return err
		}
		if err := s.wal.Append(data); err != nil {
			return err
		}
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}

	for _, rs := range spans {
		key, data := s.extractSortKey(rs)
		s.memtableSet.Insert(key, data)
	}

	return nil
}

func (s *block2Store) ReadTraces(ctx context.Context, req ReadRequest) ([]*tracepb.ResourceSpans, error) {
	var result []*tracepb.ResourceSpans

	active, flushing := s.memtableSet.Snapshot()
	for _, tbl := range []*memtable.Memtable{active, flushing} {
		if tbl == nil {
			continue
		}
		tbl.Ascend(func(key otelutil.SortKey, value []byte) bool {
			if !s.keyMatches(key, req) {
				return true
			}
			var rs tracepb.ResourceSpans
			if err := proto.Unmarshal(value, &rs); err == nil {
				result = append(result, &rs)
			}
			return true
		})
	}

	for _, rec := range s.man.AllRecords() {
		if rec.MinKey.TenantID != req.TenantID && rec.MaxKey.TenantID != req.TenantID {
			continue
		}
		if req.Service != "" && rec.MinKey.Service != req.Service && rec.MaxKey.Service != req.Service {
			continue
		}
		if uint64(req.EndTime) <= rec.MinKey.TimeNano || uint64(req.StartTime) >= rec.MaxKey.TimeNano {
			continue
		}
		if req.Service != "" && rec.BloomFile != "" {
			bloomPath := filepath.Join(s.dataDir, rec.BloomFile)
			if bf, err := bloom.Load(bloomPath); err == nil {
				targetKey := otelutil.SortKey{TenantID: req.TenantID, Service: req.Service, TimeNano: 0, SpanID: "0000000000000000"}.Key()
				if !bf.MightContain(targetKey) {
					continue
				}
			}
		}

		segPath := filepath.Join(s.dataDir, rec.SegmentFile)
		seg, err := segment.Open(segPath)
		if err != nil {
			continue
		}

		if req.StartTime > 0 && len(seg.Footer().Blocks) > 0 {
			targetKey := otelutil.SortKey{TenantID: req.TenantID, Service: req.Service, TimeNano: uint64(req.StartTime), SpanID: "0000000000000000"}.Key()
			_ = seg.SeekTo(targetKey)
		}

		for {
			data, err := seg.Next()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				break
			}
			var rs tracepb.ResourceSpans
			if err := proto.Unmarshal(data, &rs); err != nil {
				continue
			}
			key, _ := s.extractSortKey(&rs)
			if req.EndTime > 0 && key.TimeNano >= uint64(req.EndTime) {
				break
			}
			if !s.keyMatches(key, req) {
				continue
			}
			result = append(result, &rs)
		}
		_ = seg.Close()
	}

	return result, nil
}

// ReadTraceByID uses a two-level index: global locator (Layer 1) + per-segment sidecars (Layer 2).
func (s *block2Store) ReadTraceByID(ctx context.Context, tenantID, traceID string) ([]*tracepb.ResourceSpans, error) {
	var result []*tracepb.ResourceSpans

	seqs := s.locator.Find(traceID)
	for _, seq := range seqs {
		segPath := filepath.Join(s.dataDir, fmt.Sprintf("sst-%08d.pb", seq))
		idxPath := segPath + ".trace.index"

		idx, err := index.LoadSegmentTraceIndex(idxPath)
		if err != nil {
			continue
		}
		offsets := idx.Offsets[traceID]
		if len(offsets) == 0 {
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

func (s *block2Store) keyMatches(key otelutil.SortKey, req ReadRequest) bool {
	if key.TenantID != req.TenantID {
		return false
	}
	if req.Service != "" && key.Service != req.Service {
		return false
	}
	if key.TimeNano < uint64(req.StartTime) || key.TimeNano >= uint64(req.EndTime) {
		return false
	}
	return true
}

func (s *block2Store) Close() error {
	close(s.stopFlush)
	s.flushDone.Wait()
	_ = s.wal.Close()
	return nil
}

func (s *block2Store) flushMonitor() {
	defer s.flushDone.Done()
	ticker := time.NewTicker(FlushPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopFlush:
			if s.memtableSet.ActiveSize() > 0 {
				_ = s.flush()
			}
			return
		case <-ticker.C:
			if s.memtableSet.ActiveSize() >= MemtableFlushThreshold {
				_ = s.flush()
			}
		}
	}
}

func (s *block2Store) flush() error {
	flushing := s.memtableSet.Rotate()
	if flushing == nil {
		return nil
	}
	defer s.memtableSet.ClearFlushing()

	seq := s.man.NextSequence()
	segPath := filepath.Join(s.dataDir, fmt.Sprintf("sst-%08d.pb", seq))

	seg, err := segment.Create(segPath)
	if err != nil {
		return fmt.Errorf("flush: create segment: %w", err)
	}

	var minKey, maxKey otelutil.SortKey
	var first bool
	var bloomKeys []string
	var lastKey string

	traceIndex := index.SegmentTraceIndex{Offsets: make(map[string][]int64)}

	flushing.Ascend(func(key otelutil.SortKey, value []byte) bool {
		offset, _ := seg.CurrentOffset()

		traceID := s.extractTraceID(value)
		if traceID != "" {
			traceIndex.Offsets[traceID] = append(traceIndex.Offsets[traceID], offset)
		}

		_ = seg.Append(value)
		if !first {
			minKey, maxKey = key, key
			first = true
		} else {
			if key.Key() < minKey.Key() {
				minKey = key
			}
			if key.Key() > maxKey.Key() {
				maxKey = key
			}
		}
		bloomKeys = append(bloomKeys, key.Key())
		lastKey = key.Key()
		return true
	})

	if err := seg.Close(lastKey); err != nil {
		return fmt.Errorf("flush: close segment: %w", err)
	}

	idxPath := segPath + ".trace.index"
	if err := traceIndex.Save(idxPath); err != nil {
		return fmt.Errorf("flush: save trace index: %w", err)
	}

	s.locator.Merge(seq, &traceIndex)

	bf := bloom.Build(bloomKeys, 0.01)
	bloomPath := filepath.Join(s.dataDir, fmt.Sprintf("sst-%08d.bloom", seq))
	if err := bf.Save(bloomPath); err != nil {
		return fmt.Errorf("flush: save bloom: %w", err)
	}

	walName := filepath.Base(s.walPath)
	offset := s.wal.Offset()

	rec := manifest.Record{
		Sequence:      seq,
		SegmentFile:   filepath.Base(segPath),
		BloomFile:     filepath.Base(bloomPath),
		WALCheckpoint: fmt.Sprintf("%s:%d", walName, offset),
		MinKey: manifest.SortKey{
			TenantID: minKey.TenantID,
			Service:  minKey.Service,
			TimeNano: minKey.TimeNano,
			SpanID:   minKey.SpanID,
		},
		MaxKey: manifest.SortKey{
			TenantID: maxKey.TenantID,
			Service:  maxKey.Service,
			TimeNano: maxKey.TimeNano,
			SpanID:   maxKey.SpanID,
		},
		SpanCount: int64(len(bloomKeys)),
		Checksum:  "",
	}
	if err := s.man.AppendRecord(rec); err != nil {
		return fmt.Errorf("flush: manifest: %w", err)
	}

	if err := s.man.WriteCheckpoint(walName, offset); err != nil {
		return fmt.Errorf("flush: checkpoint: %w", err)
	}

	_ = s.wal.Close()
	_ = os.Truncate(s.walPath, 0)
	w, err := wal.Open(s.walPath)
	if err != nil {
		return fmt.Errorf("flush: reopen wal: %w", err)
	}
	s.wal = w

	return nil
}
