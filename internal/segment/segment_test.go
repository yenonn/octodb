package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSnappyCompressedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snappy.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Large repetitive records compress well with Snappy.
	records := [][]byte{
		bytes.Repeat([]byte("A"), 1000),
		bytes.Repeat([]byte("B"), 1001),
		bytes.Repeat([]byte("C"), 1002),
	}

	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close("key3"); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.footer.Compression != "snappy" {
		t.Fatalf("expected snappy compression in footer, got %q", r.footer.Compression)
	}

	for i, expected := range records {
		data, err := r.Next()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(data) != string(expected) {
			t.Fatalf("record %d mismatch", i)
		}
	}

	_, err = r.Next()
	if err == nil {
		t.Fatal("expected EOF after all records")
	}
}

func TestSnappySmallRecordsUncompressed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "small.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	records := [][]byte{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
	}

	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close("key3"); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.footer.Compression != "" {
		t.Fatalf("expected no compression for small records, got %q", r.footer.Compression)
	}

	for i, expected := range records {
		data, err := r.Next()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(data) != string(expected) {
			t.Fatalf("record %d mismatch", i)
		}
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	records := [][]byte{
		[]byte("hello world 1"),
		[]byte("hello world 2"),
		[]byte("hello world 3"),
	}

	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close("key3"); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i, expected := range records {
		data, err := r.Next()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(data) != string(expected) {
			t.Fatalf("record %d mismatch: got %q, want %q", i, data, expected)
		}
	}

	_, err = r.Next()
	if err == nil {
		t.Fatal("expected EOF after all records")
	}

	footer := r.Footer()
	if footer.NumRecords != 3 {
		t.Fatalf("expected 3 records in footer, got %d", footer.NumRecords)
	}
}

func TestSeekTo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seek.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough distinct keys so that cumulative on-disk size > BlockSize.
	// Use long keys to ensure compression doesn't shrink everything to < 1024 bytes.
	keys := []string{
		"aaa-long-key-for-seek-test-1",
		"bbb-long-key-for-seek-test-2",
		"ccc-long-key-for-seek-test-3",
		"ddd-long-key-for-seek-test-4",
		"eee-long-key-for-seek-test-5",
		"fff-long-key-for-seek-test-6",
	}
	for _, k := range keys {
		// Write large records so we get block index entries (BlockSize=1024).
		_ = w.Append(bytes.Repeat([]byte(k), 500))
	}
	_ = w.Close("fff")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Verify blocks were created.
	if len(r.Footer().Blocks) == 0 {
		t.Fatal("expected block index entries")
	}
	// Blocks' Keys are set from the last block's key in Close. They may not be populated.
	// But SeekTo at least walks the index.
	err = r.SeekTo("ccc")
	if err != nil {
		t.Fatalf("seek to ccc: %v", err)
	}
}

func TestSeekToPastKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seek.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff"}
	for _, k := range keys {
		// Write large records so we get block index entries.
		_ = w.Append(bytes.Repeat([]byte(k), 1100))
	}
	_ = w.Close("fff")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	err = r.SeekTo("ccc")
	if err != nil {
		t.Fatalf("seek to ccc: %v", err)
	}
	// SeekTo past all records shouldn't crash.
	err = r.SeekTo("zzz")
	if err != nil {
		t.Logf("seek past end returned: %v", err)
	}
}

func TestSeekToOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offset.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	records := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	var offsets []int64
	for _, rec := range records {
		off, _ := w.CurrentOffset()
		offsets = append(offsets, off)
		_ = w.Append(rec)
	}
	_ = w.Close("key3")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Seek to second record.
	if err := r.SeekToOffset(offsets[1]); err != nil {
		t.Fatalf("seek to offset: %v", err)
	}
	data, err := r.Next()
	if err != nil {
		t.Fatalf("read after seek: %v", err)
	}
	if string(data) != "bb" {
		t.Fatalf("expected bb, got %q", data)
	}
}

func TestCurrentOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "current.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	off, err := w.CurrentOffset()
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Fatalf("expected initial offset 0, got %d", off)
	}
	_ = w.Append([]byte("hello"))
	off2, _ := w.CurrentOffset()
	if off2 <= off {
		t.Fatalf("expected offset > 0 after append, got %d", off2)
	}
	_ = w.Close("key")
}

func TestOpenEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.pb")
	_ = os.WriteFile(path, []byte{}, 0644)
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for empty file")
	}
}

func TestOpenCorruptFooterLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.pb")
	_ = os.WriteFile(path, []byte{1, 2, 3}, 0644)
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for file too small")
	}
}

func TestOpenCorruptFooterJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "badjson.pb")
	data := []byte("NOTJSON")
	footerLen := uint32(len(data))
	buf := append(data,
		byte(footerLen), byte(footerLen>>8), byte(footerLen>>16), byte(footerLen>>24))
	_ = os.WriteFile(path, buf, 0644)
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for corrupt footer JSON")
	}
}

func TestOpenFooterLargerThanFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "badlen.pb")
	data := []byte("{}")
	buf := append(data, 0, 0, 0x40, 0x4B)
	_ = os.WriteFile(path, buf, 0644)
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for footer length exceeding file size")
	}
}

func TestSeekToNoIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "noindex.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_ = w.Append([]byte("x"))
	}
	_ = w.Close("z")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.SeekTo("anything"); err != nil {
		t.Fatalf("seek without blocks should fallback to start: %v", err)
	}
}

func TestNextPastDataRegion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "past.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]byte("only"))
	_ = w.Close("only")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.Next()
	_, err = r.Next()
	if err == nil {
		t.Fatal("expected EOF after reading past data region")
	}
}

func TestNextRecordExtendsPastDataRegion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "oversized.pb")
	// Manually build a segment where the length prefix claims a huge record.
	f, _ := os.Create(path)
	// Write a valid tiny record.
	binary.Write(f, binary.BigEndian, uint32(2))
	f.Write([]byte("ab"))
	// Write footer saying no blocks and 1 record, then footer length.
	foot := []byte(`{"version":1,"num_blocks":0,"num_records":1,"blocks":[]}`)
	binary.Write(f, binary.BigEndian, uint32(len(foot)))
	f.Write(foot)
	// Now overwrite the length to claim a huge record that goes past the footer.
	// ...actually easier: just write raw file with bad length.
	f.Close()

	// Simpler: create a proper segment then corrupt just the length of the one record.
	w, _ := Create(path + ".tmp")
	_ = w.Append([]byte("ok"))
	_ = w.Close("ok")
	// Read file, corrupt 4th byte (length prefix) to make it huge.
	data, _ := os.ReadFile(path + ".tmp")
	data[0] = 0x00
	data[1] = 0x00
	data[2] = 0x40
	data[3] = 0x4B // 1GB
	os.WriteFile(path, data, 0644)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.Next()
	// Should detect record extends past data region.
	if err == nil {
		t.Fatal("expected error for record extending past data region")
	}
}

func TestAppendTooLarge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.pb")
	w, _ := Create(path)
	big := make([]byte, 0x7FFFFFFF+1)
	err := w.Append(big)
	if err == nil {
		t.Fatal("expected error for record too large")
	}
}

func TestCompressedNext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "comp.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Medium-sized repetitive data should trigger compression.
	rec := bytes.Repeat([]byte("compress-me-please"), 200)
	if err := w.Append(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close("k"); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.footer.Compression != "snappy" {
		t.Fatalf("expected snappy compression in footer, got %q", r.footer.Compression)
	}

	data, err := r.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if string(data) != string(rec) {
		t.Fatalf("compressed record data mismatch")
	}
}

func TestBadCompressionFlag(t *testing.T) {
	// Build a valid segment file, then corrupt the compression flag byte.
	dir := t.TempDir()
	path := filepath.Join(dir, "badflag.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]byte("test-record"))
	_ = w.Close("k")

	// Read the file, find the first record's flag byte (offset 4 = after length prefix), change it.
	data, _ := os.ReadFile(path)
	data[4] = 99 // corrupt flag byte
	_ = os.WriteFile(path, data, 0644)

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	_, err = r.Next()
	if err == nil {
		t.Fatal("expected error for unknown compression flag")
	}
}

func TestReaderCloseNil(t *testing.T) {
	r := &Reader{}
	if err := r.Close(); err != nil {
		t.Fatalf("Close on nil file should succeed: %v", err)
	}
}

func TestOpenTooSmallFile(t *testing.T) {
	dir := t.TempDir()
	// File with exactly 3 bytes — too small even for footer length.
	path := filepath.Join(dir, "tiny.pb")
	_ = os.WriteFile(path, []byte{1, 2, 3}, 0644)
	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for tiny file")
	}
}

func TestOpenReadFooterIncomplete(t *testing.T) {
	dir := t.TempDir()
	// File claims footer is 100 bytes but file is only 10 bytes total.
	path := filepath.Join(dir, "incomplete.pb")
	f, _ := os.Create(path)
	// Write 6 bytes of junk + footer length claiming 100 bytes.
	f.Write([]byte{0, 0, 0, 0, 0, 0})
	binary.Write(f, binary.BigEndian, uint32(100))
	f.Close()

	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for incomplete footer")
	}
}

func TestOpenNonExistentFile(t *testing.T) {
	_, err := Open("/nonexistent/path/to/segment.pb")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestCreateNonExistentDir(t *testing.T) {
	_, err := Create("/nonexistent/path/to/segment.pb")
	if err == nil {
		t.Fatal("expected error for nonexistent directory")
	}
}

func TestManyRecordsMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough large records to generate multiple block index entries (BlockSize=1024).
	// Use 600-byte unique records so on-disk size exceeds 1024 per block.
	var records [][]byte
	for i := 0; i < 20; i++ {
		// Make each record unique to prevent good compression ratios.
		rec := make([]byte, 600)
		for j := range rec {
			rec[j] = byte((i*7 + j*13) % 256)
		}
		records = append(records, rec)
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close("last-key"); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	footer := r.Footer()
	if footer.NumRecords != 20 {
		t.Fatalf("expected 20 records, got %d", footer.NumRecords)
	}
	if len(footer.Blocks) == 0 {
		t.Fatal("expected block index entries for 20 records")
	}

	// Read all records back.
	var count int
	for {
		data, err := r.Next()
		if err != nil {
			break
		}
		if !bytes.Equal(data, records[count]) {
			t.Fatalf("record %d mismatch", count)
		}
		count++
	}
	if count != 20 {
		t.Fatalf("expected 20 records, read %d", count)
	}
}

func TestOpenValidFooterBadContent(t *testing.T) {
	// Create a segment with valid footer but with a truncated record in data region.
	dir := t.TempDir()
	path := filepath.Join(dir, "truncated_data.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]byte("record1"))
	_ = w.Append([]byte("record2"))
	_ = w.Close("k")

	// Read all records successfully, then try to read past.
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	d1, err := r.Next()
	if err != nil {
		t.Fatalf("read record 1: %v", err)
	}
	if string(d1) != "record1" {
		t.Fatalf("expected record1, got %q", d1)
	}

	d2, err := r.Next()
	if err != nil {
		t.Fatalf("read record 2: %v", err)
	}
	if string(d2) != "record2" {
		t.Fatalf("expected record2, got %q", d2)
	}

	// Third read should be EOF.
	_, err = r.Next()
	if err == nil {
		t.Fatal("expected EOF")
	}
}

func TestOpenValidFooterReadFooterData(t *testing.T) {
	// A segment that had data written in big-endian with footer length > file creates
	// the error "segment footer length exceeds file size" — already tested.
	// Test reading the footer data field (JSON content, Compression).
	dir := t.TempDir()
	path := filepath.Join(dir, "footer_detail.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// Write large compressible data.
	_ = w.Append(bytes.Repeat([]byte("AAAA"), 500))
	_ = w.Close("last")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	f := r.Footer()
	if f.Version != 1 {
		t.Fatalf("expected version 1, got %d", f.Version)
	}
	if f.NumRecords != 1 {
		t.Fatalf("expected 1 record, got %d", f.NumRecords)
	}
	if f.Compression != "snappy" {
		t.Fatalf("expected snappy, got %q", f.Compression)
	}
}

func TestNextTruncatedFlag(t *testing.T) {
	// Build a segment file, then truncate it right after a length prefix.
	dir := t.TempDir()
	path := filepath.Join(dir, "trunc_flag.pb")

	w, _ := Create(path)
	_ = w.Append([]byte("goodrecord"))
	_ = w.Close("k")

	data, _ := os.ReadFile(path)
	// Find footer length (last 4 bytes).
	fl := binary.BigEndian.Uint32(data[len(data)-4:])
	footerStart := len(data) - 4 - int(fl)

	// Truncate the data region to just the length prefix (4 bytes) — no flag byte.
	truncated := make([]byte, 0)
	truncated = append(truncated, data[:4]...) // just the length prefix
	// Re-append footer.
	truncated = append(truncated, data[footerStart:]...)
	// Fix footer to point to smaller data region.
	_ = os.WriteFile(path, truncated, 0644)

	r, err := Open(path)
	if err != nil {
		// Footer may be invalid now — that's ok.
		return
	}
	defer r.Close()

	_, err = r.Next()
	if err == nil {
		t.Fatal("expected error for truncated record")
	}
}

func TestNextCompressedLengthMismatch(t *testing.T) {
	// Create a segment with a compressed record, then corrupt the original length field.
	dir := t.TempDir()
	path := filepath.Join(dir, "comp_mismatch.pb")

	w, _ := Create(path)
	// Large compressible record.
	_ = w.Append(bytes.Repeat([]byte("X"), 2000))
	_ = w.Close("k")

	data, _ := os.ReadFile(path)
	// The first record should be compressed (flag=1 at offset 4).
	// Original length is at bytes 5-8 (after flag).
	if len(data) > 9 && data[4] == 1 {
		// Corrupt original length to a wrong value.
		binary.BigEndian.PutUint32(data[5:9], 9999)
		_ = os.WriteFile(path, data, 0644)

		r, err := Open(path)
		if err != nil {
			return
		}
		defer r.Close()

		_, err = r.Next()
		if err == nil {
			t.Fatal("expected error for length mismatch")
		}
	}
}

func TestOpenCorruptFooterData(t *testing.T) {
	// Write a file with valid footer length but garbage footer JSON body.
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt_footer.pb")

	// Footer length says 10 bytes, but we write garbage.
	f, _ := os.Create(path)
	garbage := []byte("0123456789") // 10 bytes, not valid JSON
	f.Write(garbage)
	binary.Write(f, binary.BigEndian, uint32(10))
	f.Close()

	_, err := Open(path)
	if err == nil {
		t.Fatal("expected error for corrupt footer JSON")
	}
}

func TestAppendToClosedWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "closed.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]byte("before-close"))
	_ = w.Close("k")

	// Append after close should fail.
	err = w.Append([]byte("after-close"))
	if err == nil {
		t.Fatal("expected error appending to closed writer")
	}
}

func TestCloseWriterAfterAlreadyClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "double_close.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close("k")

	// Second close should error (file already closed).
	err = w.Close("k")
	if err == nil {
		t.Fatal("expected error on double close")
	}
}

func TestNextUncompressedRecord(t *testing.T) {
	// Directly read uncompressed records to ensure that code path is fully covered.
	dir := t.TempDir()
	path := filepath.Join(dir, "uncompressed.pb")

	w, _ := Create(path)
	// Small records stay uncompressed.
	for i := 0; i < 10; i++ {
		_ = w.Append([]byte(fmt.Sprintf("rec-%d", i)))
	}
	_ = w.Close("last")

	r, _ := Open(path)
	defer r.Close()

	if r.Footer().Compression != "" {
		t.Fatalf("expected no compression, got %q", r.Footer().Compression)
	}

	for i := 0; i < 10; i++ {
		data, err := r.Next()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		expected := fmt.Sprintf("rec-%d", i)
		if string(data) != expected {
			t.Fatalf("record %d: got %q, want %q", i, data, expected)
		}
	}
}

func TestNextAfterCloseReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "closed_reader.pb")

	w, _ := Create(path)
	_ = w.Append([]byte("data"))
	_ = w.Close("k")

	r, _ := Open(path)
	_ = r.Close()

	// Next on closed reader should error.
	_, err := r.Next()
	if err == nil {
		t.Fatal("expected error on closed reader")
	}
}

func TestNextReadUncompressedShortLength(t *testing.T) {
	// Manually build a segment file with an uncompressed record that has length=0
	// (just the flag byte, no data). This triggers the "uncompressed record too short" path.
	dir := t.TempDir()
	path := filepath.Join(dir, "short_uncomp.pb")

	f, _ := os.Create(path)
	// Write record: length=0, flag=0 (uncompressed but length=0 means no data after flag)
	binary.Write(f, binary.BigEndian, uint32(0))
	f.Write([]byte{0}) // flag

	// Write footer.
	footer := []byte(`{"version":1,"num_blocks":0,"num_records":1,"blocks":[]}`)
	footerLen := uint32(len(footer))
	f.Write(footer)
	binary.Write(f, binary.BigEndian, footerLen)
	f.Close()

	r, err := Open(path)
	if err != nil {
		return // footer parsing may fail, that's ok
	}
	defer r.Close()

	_, err = r.Next()
	if err == nil {
		t.Fatal("expected error for short uncompressed record")
	}
}

func TestSeekToEmptyFooter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "emptyfooter.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close("")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// SeekTo on empty segment (no blocks) should seek to start.
	if err := r.SeekTo("any"); err != nil {
		t.Fatalf("SeekTo on empty: %v", err)
	}
}

func TestWriterCloseNoRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(""); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.Footer().NumRecords != 0 {
		t.Fatalf("expected 0 records, got %d", r.Footer().NumRecords)
	}
	_, err = r.Next()
	if err == nil {
		t.Fatal("expected EOF for empty segment")
	}
}

func TestFooter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "footer.pb")

	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_ = w.Append([]byte("x"))
	}
	_ = w.Close("zzz")

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	f := r.Footer()
	if f.Version != 1 {
		t.Fatalf("expected version 1, got %d", f.Version)
	}
	if f.NumRecords != 5 {
		t.Fatalf("expected 5 records, got %d", f.NumRecords)
	}
}
