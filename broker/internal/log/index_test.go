package log

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestIndex_LookupReturnsLargestPrecedingEntry(t *testing.T) {
	idx := newIndex()
	idx.add(0, 0)
	idx.add(10, 1024)
	idx.add(20, 4096)
	idx.add(30, 9000)

	cases := []struct {
		query uint32
		want  uint32
	}{
		{0, 0},
		{5, 0},
		{10, 1024},
		{15, 1024},
		{20, 4096},
		{25, 4096},
		{30, 9000},
		{99, 9000},
	}
	for _, c := range cases {
		if got := idx.lookup(c.query); got != c.want {
			t.Errorf("lookup(%d): got %d want %d", c.query, got, c.want)
		}
	}
}

func TestIndex_LookupOnEmptyReturnsZero(t *testing.T) {
	if got := newIndex().lookup(42); got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}

func TestIndex_WriteThenReadRoundTrip(t *testing.T) {
	idx := newIndex()
	idx.add(0, 0)
	idx.add(50, 4096)
	idx.add(100, 8192)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")
	var buf bytes.Buffer
	if err := idx.writeTo(&buf); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := readIndexFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(got.entries))
	}
	for i, want := range idx.entries {
		if got.entries[i] != want {
			t.Errorf("entry %d: got %+v want %+v", i, got.entries[i], want)
		}
	}
}

func TestIndex_ReadMissingFileReturnsEmpty(t *testing.T) {
	got, err := readIndexFrom(filepath.Join(t.TempDir(), "absent.idx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.entries) != 0 {
		t.Fatalf("got %d entries, want 0", len(got.entries))
	}
}
