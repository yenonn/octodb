package segment

import (
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
