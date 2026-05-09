package groups

import (
	"path/filepath"
	"sort"
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

// TestOffsetStore_ListEnumeratesByGroup proves List returns every
// (topic, partition, offset) the named group has committed and excludes
// commits from sibling groups. Without this, operators have no way to
// learn which partitions a group has touched and so can't compute lag
// without already knowing the assignment.
func TestOffsetStore_ListEnumeratesByGroup(t *testing.T) {
	cases := []struct {
		name string
		open func(t *testing.T) OffsetStore
	}{
		{
			name: "memory",
			open: func(*testing.T) OffsetStore { return NewMemoryOffsetStore() },
		},
		{
			name: "json",
			open: func(t *testing.T) OffsetStore {
				s, err := OpenJSONOffsetStore(filepath.Join(t.TempDir(), "offsets.json"))
				if err != nil {
					t.Fatal(err)
				}
				return s
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Arrange
			s := c.open(t)
			_ = s.Commit("g1", "events", 0, 100)
			_ = s.Commit("g1", "events", 1, 200)
			_ = s.Commit("g1", "metrics", 0, 5)
			_ = s.Commit("g2", "events", 0, 999)

			// Act
			got := s.List("g1")

			// Assert — three entries for g1, none for g2.
			if len(got) != 3 {
				t.Fatalf("List(g1): got %d entries, want 3 (got=%v)", len(got), got)
			}
			sort.Slice(got, func(i, j int) bool {
				if got[i].Topic != got[j].Topic {
					return got[i].Topic < got[j].Topic
				}
				return got[i].Partition < got[j].Partition
			})
			want := []OffsetEntry{
				{Topic: "events", Partition: 0, Offset: 100},
				{Topic: "events", Partition: 1, Offset: 200},
				{Topic: "metrics", Partition: 0, Offset: 5},
			}
			for i, w := range want {
				if got[i] != w {
					t.Errorf("entry %d: got %+v, want %+v", i, got[i], w)
				}
			}

			// Empty group returns no entries.
			if extras := s.List("g3"); len(extras) != 0 {
				t.Errorf("List(g3): got %d, want 0", len(extras))
			}
		})
	}
}

// TestOffsetStore_DeleteGroupRemovesEveryEntry proves DeleteGroup
// drops every (topic, partition) entry committed under the group
// while leaving sibling groups untouched. Without this an operator
// cleaning up an abandoned group would have to walk every partition
// it had ever touched and commit a tombstone.
func TestOffsetStore_DeleteGroupRemovesEveryEntry(t *testing.T) {
	cases := []struct {
		name string
		open func(t *testing.T) OffsetStore
	}{
		{
			name: "memory",
			open: func(*testing.T) OffsetStore { return NewMemoryOffsetStore() },
		},
		{
			name: "json",
			open: func(t *testing.T) OffsetStore {
				s, err := OpenJSONOffsetStore(filepath.Join(t.TempDir(), "offsets.json"))
				if err != nil {
					t.Fatal(err)
				}
				return s
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Arrange — two groups with overlapping topics.
			s := c.open(t)
			_ = s.Commit("g1", "events", 0, 100)
			_ = s.Commit("g1", "events", 1, 200)
			_ = s.Commit("g1", "metrics", 0, 5)
			_ = s.Commit("g2", "events", 0, 999)

			// Act
			if err := s.DeleteGroup("g1"); err != nil {
				t.Fatalf("DeleteGroup(g1): %v", err)
			}

			// Assert — g1 has no entries; g2 is untouched.
			if got := s.List("g1"); len(got) != 0 {
				t.Errorf("List(g1) after delete: got %d entries, want 0 (got=%v)", len(got), got)
			}
			if v, _ := s.Lookup("g2", "events", 0); v != 999 {
				t.Errorf("Lookup(g2): got %d, want 999 (sibling group leaked)", v)
			}
		})
	}
}

// TestJSONOffsetStore_DeleteGroupSurvivesRestart proves DeleteGroup
// persists across a reopen of the JSON store: writing the new state
// to disk before returning is a hard requirement so a broker restart
// doesn't resurface a deleted group's offsets.
func TestJSONOffsetStore_DeleteGroupSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	{
		s, err := OpenJSONOffsetStore(path)
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Commit("g1", "events", 0, 100)
		_ = s.Commit("g2", "events", 0, 200)
		if err := s.DeleteGroup("g1"); err != nil {
			t.Fatal(err)
		}
	}

	s2, err := OpenJSONOffsetStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.List("g1"); len(got) != 0 {
		t.Errorf("post-reopen List(g1): got %d, want 0", len(got))
	}
	if v, _ := s2.Lookup("g2", "events", 0); v != 200 {
		t.Errorf("post-reopen Lookup(g2): got %d, want 200", v)
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
