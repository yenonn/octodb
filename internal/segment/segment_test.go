package segment

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

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

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff"}
	for _, k := range keys {
		// Write large records so we get block index entries (BlockSize=1024).
		_ = w.Append(bytes.Repeat([]byte(k), 1100))
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

func TestClose(t *testing.T) {
	r := &Reader{}
	if err := r.Close(); err != nil {
		t.Fatalf("close on nil file: %v", err)
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
