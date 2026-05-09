package groups

import (
	"context"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// fakeAppender is a minimal in-memory implementation of TopicAppender.
// Records are stored in a slice keyed by partition.
type fakeAppender struct {
	records map[proto.PartitionRef][]proto.Record
}

func newFakeAppender() *fakeAppender {
	return &fakeAppender{records: make(map[proto.PartitionRef][]proto.Record)}
}

func (f *fakeAppender) Append(_ context.Context, p proto.PartitionRef, r proto.Record) (int64, error) {
	r.Offset = int64(len(f.records[p]))
	if r.Timestamp == 0 {
		r.Timestamp = time.Now().UnixNano()
	}
	f.records[p] = append(f.records[p], r)
	return r.Offset, nil
}

func (f *fakeAppender) Read(_ context.Context, p proto.PartitionRef, fromOffset int64, maxRecords int) ([]proto.Record, error) {
	all := f.records[p]
	if fromOffset >= int64(len(all)) {
		return nil, nil
	}
	end := min(fromOffset+int64(maxRecords), int64(len(all)))
	out := make([]proto.Record, end-fromOffset)
	copy(out, all[fromOffset:end])
	return out, nil
}

func TestTopicOffsetStore_RoundTrip(t *testing.T) {
	// Arrange
	app := newFakeAppender()
	s, err := OpenTopicOffsetStore(context.Background(), app, "")
	if err != nil {
		t.Fatal(err)
	}

	// Act
	if err := s.Commit("g", "events", 0, 42); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("g", "events", 1, 17); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("g", "events", 0, 100); err != nil {
		t.Fatal(err)
	}

	// Assert
	got, _ := s.Lookup("g", "events", 0)
	if got != 100 {
		t.Errorf("p0: got %d, want 100", got)
	}
	got, _ = s.Lookup("g", "events", 1)
	if got != 17 {
		t.Errorf("p1: got %d, want 17", got)
	}
}

func TestTopicOffsetStore_LookupMissingReturnsNoOffset(t *testing.T) {
	app := newFakeAppender()
	s, _ := OpenTopicOffsetStore(context.Background(), app, "")
	got, err := s.Lookup("g", "events", 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != NoOffset {
		t.Fatalf("got %d, want NoOffset", got)
	}
}

func TestTopicOffsetStore_ReplayRebuildsState(t *testing.T) {
	// Arrange: store 1 commits twice, then replay creates store 2 with
	// identical state.
	app := newFakeAppender()
	s1, _ := OpenTopicOffsetStore(context.Background(), app, "")
	if err := s1.Commit("g", "events", 0, 42); err != nil {
		t.Fatal(err)
	}
	if err := s1.Commit("h", "events", 1, 7); err != nil {
		t.Fatal(err)
	}

	// Act
	s2, err := OpenTopicOffsetStore(context.Background(), app, "")
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	got, _ := s2.Lookup("g", "events", 0)
	if got != 42 {
		t.Errorf("after replay, g/events/0 = %d, want 42", got)
	}
	got, _ = s2.Lookup("h", "events", 1)
	if got != 7 {
		t.Errorf("after replay, h/events/1 = %d, want 7", got)
	}
}

func TestTopicOffsetStore_LatestCommitWins(t *testing.T) {
	app := newFakeAppender()
	s, _ := OpenTopicOffsetStore(context.Background(), app, "")
	for _, v := range []int64{1, 2, 3, 99} {
		_ = s.Commit("g", "t", 0, v)
	}

	// Replay must reflect the last write.
	s2, _ := OpenTopicOffsetStore(context.Background(), app, "")
	got, _ := s2.Lookup("g", "t", 0)
	if got != 99 {
		t.Fatalf("got %d, want 99", got)
	}
}
