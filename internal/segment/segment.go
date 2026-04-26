// Package segment writes and reads immutable sorted segment files (SSTable-like).
//
// Block 2+: Flush memtable to sorted segment files on disk.
// Format: length-prefixed proto records, sorted by SortKey, with optional Bloom filter.
//
// Block 3: Added block index footer for SeekTo, SeekToOffset for trace_id indexing.
package segment

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	// BlockSize is the approx number of bytes between index entries.
	BlockSize = 1024
)

// BlockEntry records the starting SortKey and file offset of a data block.
type BlockEntry struct {
	Key    string `json:"key"`
	Offset int64  `json:"offset"`
}

// Footer is the JSON metadata at the end of a segment file.
type Footer struct {
	Version    int          `json:"version"`
	NumBlocks  int          `json:"num_blocks"`
	NumRecords int64        `json:"num_records"`
	Blocks     []BlockEntry `json:"blocks"`
}

// Writer serializes sorted records to a segment file.
// Not safe for concurrent use.
type Writer struct {
	path              string
	f                 *os.File
	blocks            []BlockEntry
	count             int64
	pendingBlockBytes int64
}

// Create opens a new segment file for writing.
func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("segment create %s: %w", path, err)
	}
	return &Writer{
		path: path,
		f:    f,
	}, nil
}

// CurrentOffset returns the current absolute byte offset in the file.
func (w *Writer) CurrentOffset() (int64, error) {
	return w.f.Seek(0, io.SeekCurrent)
}

// Append writes one length-prefixed record. Must be called in ascending SortKey order.
func (w *Writer) Append(value []byte) error {
	n := len(value)
	if n > 0x7FFFFFFF {
		return fmt.Errorf("segment: record too large (%d bytes)", n)
	}

	if err := binary.Write(w.f, binary.BigEndian, uint32(n)); err != nil {
		return fmt.Errorf("segment: write length: %w", err)
	}
	if _, err := w.f.Write(value); err != nil {
		return fmt.Errorf("segment: write data: %w", err)
	}
	w.count++

	// Index every ~BlockSize bytes.
	w.pendingBlockBytes += int64(4 + n)
	if w.pendingBlockBytes >= BlockSize {
		offset, _ := w.f.Seek(0, io.SeekCurrent)
		w.blocks = append(w.blocks, BlockEntry{Offset: offset - int64(w.pendingBlockBytes)})
		w.pendingBlockBytes = 0
	}

	return nil
}

// Close flushes, writes footer with block index, and fsyncs.
func (w *Writer) Close(lastKey string) error {
	// Set the key for the last block if it was added at the start of a block but key was unknown.
	if len(w.blocks) > 0 {
		w.blocks[len(w.blocks)-1].Key = lastKey
	}

	footer := Footer{
		Version:    1,
		NumBlocks:  len(w.blocks),
		NumRecords: w.count,
		Blocks:     w.blocks,
	}
	footerData, err := json.Marshal(footer)
	if err != nil {
		return fmt.Errorf("segment: marshal footer: %w", err)
	}

	footerLen := uint32(len(footerData))
	if _, err := w.f.Write(footerData); err != nil {
		return fmt.Errorf("segment: write footer: %w", err)
	}
	if err := binary.Write(w.f, binary.BigEndian, footerLen); err != nil {
		return fmt.Errorf("segment: write footer length: %w", err)
	}

	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("segment: fsync: %w", err)
	}
	return w.f.Close()
}

// ---------------------------------------------------------------------------
// Reader with SeekTo (no bufio - direct reads for accurate position)
// ---------------------------------------------------------------------------

// Reader reads length-prefixed records from a segment file.
type Reader struct {
	f         *os.File
	footer    Footer
	recordEnd int64
}

// Open opens a segment file for reading and reads its footer.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment open %s: %w", path, err)
	}

	// Seek to end - 4 bytes to read footer length.
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment stat: %w", err)
	}
	size := info.Size()
	if size < 4 {
		_ = f.Close()
		return nil, fmt.Errorf("segment file too small")
	}

	var footerLen uint32
	if _, err := f.Seek(-4, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment seek: %w", err)
	}
	if err := binary.Read(f, binary.BigEndian, &footerLen); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment read footer length: %w", err)
	}

	// Read footer JSON.
	footerStart := size - 4 - int64(footerLen)
	if footerStart < 0 {
		_ = f.Close()
		return nil, fmt.Errorf("segment footer length exceeds file size")
	}
	if _, err := f.Seek(footerStart, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment seek footer: %w", err)
	}
	footerData := make([]byte, footerLen)
	if _, err := io.ReadFull(f, footerData); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment read footer: %w", err)
	}

	var footer Footer
	if err := json.Unmarshal(footerData, &footer); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment unmarshal footer: %w", err)
	}

	// Seek back to start for reading.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment seek start: %w", err)
	}

	return &Reader{
		f:         f,
		footer:    footer,
		recordEnd: footerStart,
	}, nil
}

// SeekTo binary-searches the block index for targetKey and seeks to that block.
func (r *Reader) SeekTo(targetKey string) error {
	blocks := r.footer.Blocks
	if len(blocks) == 0 {
		_, err := r.f.Seek(0, io.SeekStart)
		return err
	}

	lo, hi := 0, len(blocks)
	for lo < hi {
		mid := (lo + hi) / 2
		if blocks[mid].Key < targetKey {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo
	if idx >= len(blocks) {
		return io.EOF
	}

	_, err := r.f.Seek(blocks[idx].Offset, io.SeekStart)
	return err
}

// SeekToOffset seeks the file handle to an absolute byte offset.
func (r *Reader) SeekToOffset(offset int64) error {
	_, err := r.f.Seek(offset, io.SeekStart)
	return err
}

// Next reads the next record. Returns io.EOF when done or when past the data region.
func (r *Reader) Next() ([]byte, error) {
	pos, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("segment: get position: %w", err)
	}
	if pos >= r.recordEnd {
		return nil, io.EOF
	}

	var length uint32
	if err := binary.Read(r.f, binary.BigEndian, &length); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("segment: read length: %w", err)
	}

	// After reading length bytes, check if we would go past the footer.
	nextPos := pos + 4 + int64(length)
	if nextPos > r.recordEnd {
		return nil, fmt.Errorf("segment: record extends past data region")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r.f, data); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("segment: truncated record (expected %d)", length)
		}
		return nil, fmt.Errorf("segment: read data: %w", err)
	}
	return data, nil
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}

// Footer returns the parsed footer metadata.
func (r *Reader) Footer() Footer {
	return r.footer
}