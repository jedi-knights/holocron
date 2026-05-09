package groups

import (
	"errors"
	"sort"
	"testing"
	"time"
)

func newTestManager(t *testing.T) (*Manager, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	partitionsFor := func(topic string) (int32, error) {
		switch topic {
		case "events":
			return 4, nil
		case "orders":
			return 2, nil
		}
		return 0, errors.New("unknown topic: " + topic)
	}
	return NewManager(NewMemoryOffsetStore(), partitionsFor,
		WithClock(clock.Now),
		WithSessionTimeout(15*time.Second),
	), clock
}

type fakeClock struct {
	t time.Time
}

func (f *fakeClock) Now() time.Time { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

func TestManager_SingleMemberGetsAllPartitions(t *testing.T) {
	m, _ := newTestManager(t)
	res, err := m.Join("g", "", []string{"events"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Assignments) != 4 {
		t.Fatalf("got %d assignments, want 4", len(res.Assignments))
	}
}

func TestManager_TwoMembersShareEvenly(t *testing.T) {
	m, _ := newTestManager(t)
	a, _ := m.Join("g", "", []string{"events"})
	b, _ := m.Join("g", "", []string{"events"})
	// a's response is now stale (it had 4 partitions before b joined).
	// Real consumers heartbeat, see RebalanceNeeded, and rejoin. Simulate that.
	a2, _ := m.Join("g", a.MemberID, []string{"events"})
	if len(a2.Assignments)+len(b.Assignments) != 4 {
		t.Fatalf("totals: a=%d b=%d", len(a2.Assignments), len(b.Assignments))
	}
	if len(a2.Assignments) == 0 || len(b.Assignments) == 0 {
		t.Fatalf("uneven split: a=%d b=%d", len(a2.Assignments), len(b.Assignments))
	}
}

func TestManager_RejoinUpdatesAssignment(t *testing.T) {
	m, _ := newTestManager(t)
	a1, _ := m.Join("g", "", []string{"events"})
	_, _ = m.Join("g", "", []string{"events"}) // triggers rebalance
	a2, _ := m.Join("g", a1.MemberID, []string{"events"})
	if a2.Generation <= a1.Generation {
		t.Fatalf("generation did not advance: %d → %d", a1.Generation, a2.Generation)
	}
}

func TestManager_HeartbeatSignalsRebalanceOnJoin(t *testing.T) {
	m, _ := newTestManager(t)
	a, _ := m.Join("g", "", []string{"events"})
	_, _ = m.Join("g", "", []string{"events"})
	hb, err := m.Heartbeat("g", a.MemberID, a.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if !hb.RebalanceNeeded {
		t.Fatal("expected RebalanceNeeded after peer joined")
	}
}

func TestManager_HeartbeatRejectsUnknownMember(t *testing.T) {
	m, _ := newTestManager(t)
	_, err := m.Heartbeat("g", "ghost", 0)
	if !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("expected ErrUnknownMember, got %v", err)
	}
}

func TestManager_LeaveTriggersRebalance(t *testing.T) {
	m, _ := newTestManager(t)
	a, _ := m.Join("g", "", []string{"events"})
	b, _ := m.Join("g", "", []string{"events"})
	if err := m.Leave("g", a.MemberID); err != nil {
		t.Fatal(err)
	}
	hb, _ := m.Heartbeat("g", b.MemberID, b.Generation)
	if !hb.RebalanceNeeded {
		t.Fatal("expected RebalanceNeeded after peer left")
	}
}

func TestManager_StaleMemberEvictedAfterSessionTimeout(t *testing.T) {
	m, clock := newTestManager(t)
	a, _ := m.Join("g", "", []string{"events"})
	b, _ := m.Join("g", "", []string{"events"})

	// b heartbeats once; a does not.
	clock.advance(20 * time.Second)
	hb, _ := m.Heartbeat("g", b.MemberID, b.Generation)
	if !hb.RebalanceNeeded {
		t.Fatal("expected eviction-driven rebalance")
	}

	// After eviction, b rejoining should now own all 4 partitions.
	rj, _ := m.Join("g", b.MemberID, []string{"events"})
	if len(rj.Assignments) != 4 {
		t.Fatalf("after eviction, b got %d partitions, want 4", len(rj.Assignments))
	}
	// a is gone now: heartbeat should err.
	if _, err := m.Heartbeat("g", a.MemberID, a.Generation); !errors.Is(err, ErrUnknownMember) {
		t.Fatalf("expected eviction, got %v", err)
	}
}

func TestManager_CommitAndLookup(t *testing.T) {
	m, _ := newTestManager(t)
	if err := m.Commit("g", "events", 0, 42); err != nil {
		t.Fatal(err)
	}
	got, err := m.Committed("g", "events", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestManager_JoinReturnsCommittedOffsets(t *testing.T) {
	m, _ := newTestManager(t)
	_ = m.Commit("g", "events", 0, 99)
	res, _ := m.Join("g", "", []string{"events"})

	var seen bool
	for _, a := range res.Assignments {
		if a.Partition.Topic == "events" && a.Partition.Index == 0 {
			seen = true
			if a.CommittedOffset != 99 {
				t.Fatalf("partition 0 committed: got %d, want 99", a.CommittedOffset)
			}
		} else if a.CommittedOffset != NoOffset {
			t.Errorf("partition %d should be NoOffset, got %d", a.Partition.Index, a.CommittedOffset)
		}
	}
	if !seen {
		t.Fatal("partition 0 not in assignment")
	}
}

func TestManager_NoOverlapAcrossMembers(t *testing.T) {
	m, _ := newTestManager(t)
	a, _ := m.Join("g", "", []string{"events"})
	b, _ := m.Join("g", "", []string{"events"})
	a2, _ := m.Join("g", a.MemberID, []string{"events"}) // refresh

	got := append([]Assignment(nil), a2.Assignments...)
	got = append(got, b.Assignments...)
	sort.Slice(got, func(i, j int) bool { return got[i].Partition.Index < got[j].Partition.Index })

	for i := 0; i < len(got)-1; i++ {
		if got[i].Partition == got[i+1].Partition {
			t.Fatalf("partition %v assigned twice", got[i].Partition)
		}
	}
}
