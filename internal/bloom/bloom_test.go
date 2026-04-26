package bloom

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildAndContains(t *testing.T) {
	keys := []string{"apple", "banana", "cherry", "date"}
	b := Build(keys, 0.01)

	for _, k := range keys {
		if !b.MightContain(k) {
			t.Errorf("expected %q to be in set", k)
		}
	}

	negative := []string{"eggplant", "fig", "grape"}
	fpCount := 0
	for _, k := range negative {
		if b.MightContain(k) {
			fpCount++
		}
	}
	t.Logf("false positive rate with negatives: %d/%d", fpCount, len(negative))
}

func TestSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bloom")

	keys := []string{"a", "b", "c", "d"}
	b1 := Build(keys, 0.01)
	if err := b1.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	b2, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	for _, k := range keys {
		if !b2.MightContain(k) {
			t.Errorf("loaded filter missing %q", k)
		}
	}
}

func TestEmptyLoad(t *testing.T) {
	_, err := Load("nonexistent.bloom")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestSaveEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bloom")
	b := Build([]string{}, 0.01)
	if err := b.Save(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("empty bloom file should still have header")
	}
}
