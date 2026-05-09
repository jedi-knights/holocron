package groups

import (
	"sort"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestRangeAssign_EvenSplit(t *testing.T) {
	got := rangeAssign([]string{"a", "b"}, "t", 4)
	if len(got["a"]) != 2 || len(got["b"]) != 2 {
		t.Fatalf("uneven split: %+v", got)
	}
	if got["a"][0].Index != 0 || got["a"][1].Index != 1 {
		t.Fatalf("a got %+v", got["a"])
	}
	if got["b"][0].Index != 2 || got["b"][1].Index != 3 {
		t.Fatalf("b got %+v", got["b"])
	}
}

func TestRangeAssign_ExtraGoesToFirstMembers(t *testing.T) {
	got := rangeAssign([]string{"a", "b", "c"}, "t", 7)
	if len(got["a"]) != 3 || len(got["b"]) != 2 || len(got["c"]) != 2 {
		t.Fatalf("unexpected counts: a=%d b=%d c=%d", len(got["a"]), len(got["b"]), len(got["c"]))
	}
}

func TestRangeAssign_DeterministicAcrossOrder(t *testing.T) {
	a := rangeAssign([]string{"alpha", "bravo", "charlie"}, "t", 6)
	b := rangeAssign([]string{"charlie", "alpha", "bravo"}, "t", 6)
	for _, id := range []string{"alpha", "bravo", "charlie"} {
		if !samePartitions(a[id], b[id]) {
			t.Fatalf("non-deterministic: %s differs", id)
		}
	}
}

func TestRangeAssign_EmptyInputs(t *testing.T) {
	if got := rangeAssign(nil, "t", 4); len(got) != 0 {
		t.Fatalf("empty members → %+v", got)
	}
	if got := rangeAssign([]string{"a"}, "t", 0); len(got["a"]) != 0 {
		t.Fatalf("zero partitions → %+v", got["a"])
	}
}

func TestAssign_AcrossMultipleTopics(t *testing.T) {
	partitionsFor := func(topic string) (int32, error) {
		return map[string]int32{"orders": 4, "events": 2}[topic], nil
	}
	got, err := assign([]string{"a", "b"}, []string{"orders", "events"}, partitionsFor)
	if err != nil {
		t.Fatal(err)
	}
	total := len(got["a"]) + len(got["b"])
	if total != 6 {
		t.Fatalf("total partitions assigned: %d, want 6", total)
	}
	// Every (topic, partition) appears exactly once.
	seen := make(map[proto.PartitionRef]int)
	for _, parts := range got {
		for _, p := range parts {
			seen[p]++
		}
	}
	for ref, count := range seen {
		if count != 1 {
			t.Errorf("%v assigned %d times", ref, count)
		}
	}
	if len(seen) != 6 {
		t.Fatalf("got %d unique partitions, want 6", len(seen))
	}
}

func TestRoundRobinAssign_SingleTopic(t *testing.T) {
	// Arrange / Act — 7 partitions, 3 members, dealt in order.
	got := roundRobinAssign([]string{"a", "b", "c"}, "t", 7)

	// Assert — each member gets a strided slice (a:0,3,6 b:1,4 c:2,5).
	want := map[string][]int32{
		"a": {0, 3, 6},
		"b": {1, 4},
		"c": {2, 5},
	}
	for id, indices := range want {
		if len(got[id]) != len(indices) {
			t.Fatalf("%s: got %d partitions, want %d", id, len(got[id]), len(indices))
		}
		for i, idx := range indices {
			if got[id][i].Index != idx {
				t.Errorf("%s[%d]: got %d, want %d", id, i, got[id][i].Index, idx)
			}
		}
	}
}

func TestRoundRobinAssign_DeterministicAcrossOrder(t *testing.T) {
	// Arrange / Act — same members in different input orders must produce
	// the same assignment (sorted internally by memberID).
	a := roundRobinAssign([]string{"alpha", "bravo", "charlie"}, "t", 6)
	b := roundRobinAssign([]string{"charlie", "alpha", "bravo"}, "t", 6)

	// Assert
	for _, id := range []string{"alpha", "bravo", "charlie"} {
		if !samePartitions(a[id], b[id]) {
			t.Fatalf("non-deterministic: %s differs", id)
		}
	}
}

func TestAssignRoundRobin_AcrossMultipleTopics(t *testing.T) {
	// Arrange
	partitionsFor := func(topic string) (int32, error) {
		return map[string]int32{"orders": 4, "events": 2}[topic], nil
	}

	// Act
	got, err := assignRoundRobin([]string{"a", "b"}, []string{"orders", "events"}, partitionsFor)
	if err != nil {
		t.Fatal(err)
	}

	// Assert — total = 6, every (topic, partition) appears exactly once.
	total := len(got["a"]) + len(got["b"])
	if total != 6 {
		t.Fatalf("total partitions assigned: %d, want 6", total)
	}
	seen := make(map[proto.PartitionRef]int)
	for _, parts := range got {
		for _, p := range parts {
			seen[p]++
		}
	}
	for ref, count := range seen {
		if count != 1 {
			t.Errorf("%v assigned %d times", ref, count)
		}
	}
}

// TestStickyAssign_PreservesPriorWhenMemberSetUnchanged proves a fresh
// rebalance with the same members and topics returns the prior
// assignment verbatim — no churn, no sorting changes.
func TestStickyAssign_PreservesPriorWhenMemberSetUnchanged(t *testing.T) {
	// Arrange — prior assignment from a previous rebalance.
	prior := map[string][]proto.PartitionRef{
		"a": {{Topic: "t", Index: 0}, {Topic: "t", Index: 3}, {Topic: "t", Index: 6}},
		"b": {{Topic: "t", Index: 1}, {Topic: "t", Index: 4}},
		"c": {{Topic: "t", Index: 2}, {Topic: "t", Index: 5}},
	}
	partitionsFor := func(_ string) (int32, error) { return 7, nil }

	// Act
	got, err := assignSticky([]string{"a", "b", "c"}, []string{"t"}, partitionsFor, prior)
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	for id, want := range prior {
		if !samePartitions(got[id], want) {
			t.Errorf("member %s: got %v, want %v (no churn expected)", id, got[id], want)
		}
	}
}

// TestStickyAssign_NewMemberStealsMinimum proves a new member joining
// only displaces the minimum partitions needed to reach its target.
// Other members keep most of their prior partitions.
func TestStickyAssign_NewMemberStealsMinimum(t *testing.T) {
	// Arrange — prior with two members owning all 6 partitions.
	prior := map[string][]proto.PartitionRef{
		"a": {{Topic: "t", Index: 0}, {Topic: "t", Index: 1}, {Topic: "t", Index: 2}},
		"b": {{Topic: "t", Index: 3}, {Topic: "t", Index: 4}, {Topic: "t", Index: 5}},
	}
	partitionsFor := func(_ string) (int32, error) { return 6, nil }

	// Act — third member joins. Target: 2 each.
	got, err := assignSticky([]string{"a", "b", "c"}, []string{"t"}, partitionsFor, prior)
	if err != nil {
		t.Fatal(err)
	}

	// Assert — a and b keep 2 of their 3 prior partitions; c gets 2.
	if len(got["a"]) != 2 {
		t.Errorf("a: got %d partitions, want 2", len(got["a"]))
	}
	if len(got["b"]) != 2 {
		t.Errorf("b: got %d partitions, want 2", len(got["b"]))
	}
	if len(got["c"]) != 2 {
		t.Errorf("c: got %d partitions, want 2", len(got["c"]))
	}
	// Each kept partition was in the member's prior set.
	for _, kept := range got["a"] {
		if !contains(prior["a"], kept) {
			t.Errorf("a: %v not in prior — sticky should retain", kept)
		}
	}
	for _, kept := range got["b"] {
		if !contains(prior["b"], kept) {
			t.Errorf("b: %v not in prior — sticky should retain", kept)
		}
	}
	// Total = 6, every partition assigned exactly once.
	seen := make(map[proto.PartitionRef]int)
	for _, parts := range got {
		for _, p := range parts {
			seen[p]++
		}
	}
	if len(seen) != 6 {
		t.Errorf("got %d distinct partitions, want 6", len(seen))
	}
	for ref, count := range seen {
		if count != 1 {
			t.Errorf("%v assigned %d times", ref, count)
		}
	}
}

// TestStickyAssign_LeavingMemberRedistributesMinimally proves a
// departing member's partitions are redistributed without touching the
// remaining members' prior assignments.
func TestStickyAssign_LeavingMemberRedistributesMinimally(t *testing.T) {
	// Arrange — three members prior; "c" leaves.
	prior := map[string][]proto.PartitionRef{
		"a": {{Topic: "t", Index: 0}, {Topic: "t", Index: 1}},
		"b": {{Topic: "t", Index: 2}, {Topic: "t", Index: 3}},
		"c": {{Topic: "t", Index: 4}, {Topic: "t", Index: 5}},
	}
	partitionsFor := func(_ string) (int32, error) { return 6, nil }

	// Act
	got, err := assignSticky([]string{"a", "b"}, []string{"t"}, partitionsFor, prior)
	if err != nil {
		t.Fatal(err)
	}

	// Assert — a + b keep their prior, c's two partitions split between them.
	for _, p := range prior["a"] {
		if !contains(got["a"], p) {
			t.Errorf("a lost prior partition %v", p)
		}
	}
	for _, p := range prior["b"] {
		if !contains(got["b"], p) {
			t.Errorf("b lost prior partition %v", p)
		}
	}
	if len(got["a"])+len(got["b"]) != 6 {
		t.Errorf("total partitions: %d, want 6", len(got["a"])+len(got["b"]))
	}
}

func contains(haystack []proto.PartitionRef, needle proto.PartitionRef) bool {
	for _, p := range haystack {
		if p == needle {
			return true
		}
	}
	return false
}

func samePartitions(a, b []proto.PartitionRef) bool {
	if len(a) != len(b) {
		return false
	}
	ax := append([]proto.PartitionRef(nil), a...)
	bx := append([]proto.PartitionRef(nil), b...)
	sort.Slice(ax, func(i, j int) bool { return ax[i].Index < ax[j].Index })
	sort.Slice(bx, func(i, j int) bool { return bx[i].Index < bx[j].Index })
	for i := range ax {
		if ax[i] != bx[i] {
			return false
		}
	}
	return true
}
