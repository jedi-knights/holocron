package groups

import (
	"path/filepath"
	"testing"
)

func TestMemoryOffsetStore_RoundTrip(t *testing.T) {
	s := NewMemoryOffsetStore()
	if err := s.Commit("g", "t", 0, 42); err != nil {
		t.Fatal(err)
	}
	got, err := s.Lookup("g", "t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d want 42", got)
	}
}

func TestMemoryOffsetStore_LookupMissingReturnsNoOffset(t *testing.T) {
	s := NewMemoryOffsetStore()
	got, err := s.Lookup("g", "t", 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != NoOffset {
		t.Fatalf("got %d want NoOffset", got)
	}
}

func TestJSONOffsetStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	{
		s, err := OpenJSONOffsetStore(path)
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Commit("g", "events", 0, 100)
		_ = s.Commit("g", "events", 1, 200)
		_ = s.Commit("h", "events", 0, 50)
	}

	s2, err := OpenJSONOffsetStore(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		group     string
		topic     string
		partition int32
		want      int64
	}{
		{"g", "events", 0, 100},
		{"g", "events", 1, 200},
		{"h", "events", 0, 50},
		{"unknown", "events", 0, NoOffset},
	}
	for _, c := range cases {
		got, err := s2.Lookup(c.group, c.topic, c.partition)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("(%s,%s,%d): got %d want %d", c.group, c.topic, c.partition, got, c.want)
		}
	}
}

func TestJSONOffsetStore_LatestCommitWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")
	s, err := OpenJSONOffsetStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []int64{1, 2, 3, 99} {
		_ = s.Commit("g", "t", 0, v)
	}
	got, _ := s.Lookup("g", "t", 0)
	if got != 99 {
		t.Fatalf("got %d, want 99", got)
	}
}
