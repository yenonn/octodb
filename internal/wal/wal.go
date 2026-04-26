package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Writer manages an append-only WAL file.
// Safe for concurrent use — a mutex serialises appenders.
type Writer struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	offset int64 // current write offset in bytes
}

// Open opens (or creates) a WAL file for appending.
func Open(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal %q: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat wal %q: %w", path, err)
	}

	return &Writer{
		file:   f,
		path:   path,
		offset: info.Size(),
	}, nil
}

// AppendBatch appends multiple records in one atomic write.
// Format: [n records] where each record is [length][CRC32][payload].
func (w *Writer) AppendBatch(batch [][]byte) error {
	if len(batch) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	var total int
	for _, data := range batch {
		n := len(data)
		if n == 0 {
			return fmt.Errorf("wal: cannot append empty record in batch")
		}
		if n > 0x7FFFFFFF {
			return fmt.Errorf("wal: record too large (%d bytes)", n)
		}
		total += 8 + n
	}

	buf := make([]byte, total)
	pos := 0
	for _, data := range batch {
		n := len(data)
		// Use direct byte manipulation to avoid BigEndian allocation overhead
		buf[pos] = byte(n >> 24)
		buf[pos+1] = byte(n >> 16)
		buf[pos+2] = byte(n >> 8)
		buf[pos+3] = byte(n)
		checksum := crc32.ChecksumIEEE(data)
		buf[pos+4] = byte(checksum >> 24)
		buf[pos+5] = byte(checksum >> 16)
		buf[pos+6] = byte(checksum >> 8)
		buf[pos+7] = byte(checksum)
		copy(buf[pos+8:], data)
		pos += 8 + n
	}

	written, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("wal: batch write error: %w", err)
	}
	w.offset += int64(written)
	return nil
}

// Append writes a length-prefixed record to the WAL.
// Format: [4 bytes big-endian length][4 bytes CRC32][N bytes payload].
func (w *Writer) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := len(data)
	if n == 0 {
		return fmt.Errorf("wal: cannot append empty record")
	}
	if n > 0x7FFFFFFF {
		return fmt.Errorf("wal: record too large (%d bytes)", n)
	}

	checksum := crc32.ChecksumIEEE(data)
	buf := make([]byte, 8+n)
	// Use direct byte manipulation to avoid BigEndian allocation overhead
	buf[0] = byte(n >> 24)
	buf[1] = byte(n >> 16)
	buf[2] = byte(n >> 8)
	buf[3] = byte(n)
	buf[4] = byte(checksum >> 24)
	buf[5] = byte(checksum >> 16)
	buf[6] = byte(checksum >> 8)
	buf[7] = byte(checksum)
	copy(buf[8:], data)

	written, err := w.file.Write(buf)
	if err != nil {
		return fmt.Errorf("wal: write error: %w", err)
	}
	w.offset += int64(written)
	return nil
}

// Sync flushes the WAL to disk (fsync).
// Must be called after a batch of Appends before ACKing the caller.
func (w *Writer) Sync() error {
	return w.file.Sync()
}

// Close closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Offset returns the current file size / next write offset.
func (w *Writer) Offset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

// Reader reads length-prefixed records from a WAL.
type Reader struct {
	r io.Reader
	f io.Closer // optional underlying file
}

// NewReader creates a reader from an io.Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// NewReaderFromFile opens path and returns a reader.
func NewReaderFromFile(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open wal for replay %q: %w", path, err)
	}
	return &Reader{r: f, f: f}, nil
}

// ErrBadCRC is returned when a WAL record fails its checksum.
var ErrBadCRC = fmt.Errorf("wal: record checksum mismatch")

// Next reads the next record.
// Returns io.EOF when there are no more complete records.
func (r *Reader) Next() ([]byte, error) {
	var length uint32
	if err := binary.Read(r.r, binary.BigEndian, &length); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("wal: read length: %w", err)
	}

	var storedCRC uint32
	if err := binary.Read(r.r, binary.BigEndian, &storedCRC); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("wal: record truncated after length (CRC)")
		}
		return nil, fmt.Errorf("wal: read crc: %w", err)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r.r, data); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("wal: record truncated (expected %d bytes, got EOF)", length)
		}
		return nil, fmt.Errorf("wal: read data: %w", err)
	}

	if crc32.ChecksumIEEE(data) != storedCRC {
		return nil, ErrBadCRC
	}
	return data, nil
}

// Close closes the underlying file if it was opened by NewReaderFromFile.
func (r *Reader) Close() error {
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}
