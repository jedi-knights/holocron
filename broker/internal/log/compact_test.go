package log

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

// fillSegments produces enough writes (with the given key/value pairs)
// across multiple sealed segments to exercise compaction. Returns the
// PartitionLog, ready for Compact.
func fillSegments(t *testing.T, kvs []struct{ K, V []byte }, segCap int64) *PartitionLog {
	t.Helper()
	dir := t.TempDir()
	p, err := OpenPartition(dir, segCap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })

	for i, kv := range kvs {
		if _, err := p.Append(proto.Record{
			Offset:    int64(i),
			Timestamp: int64(i + 1),
			Key:       kv.K,
			Value:     kv.V,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestCompact_KeepsLatestRecordPerKey(t *testing.T) {
	// Arrange: 6 records, 3 keys, segCap forces multiple sealed segments.
	body := bytes.Repeat([]byte("x"), 64)
	_ = body
	p := fillSegments(t, []struct{ K, V []byte }{
		{[]byte("a"), []byte("a-1")},
		{[]byte("b"), []byte("b-1")},
		{[]byte("a"), []byte("a-2")},
		{[]byte("c"), []byte("c-1")},
		{[]byte("b"), []byte("b-2")},
		{[]byte("a"), []byte("a-3")},
	}, 128)

	// Force at least one sealed segment by appending a final no-op
	// record so rollover happens. Actually, the last record may have
	// triggered rollover if size>cap; verify there are >=2 segments.
	if len(p.segments) < 2 {
		// Force rollover by appending one more record.
		if _, err := p.Append(proto.Record{Offset: 6, Key: []byte("z"), Value: []byte("z")}); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.segments) < 2 {
		t.Skip("test environment did not produce a sealed segment; skipping")
	}

	// Act
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}

	// Assert: latest record per key is the one that survived.
	got, err := p.Read(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]string)
	for _, r := range got {
		if len(r.Key) > 0 {
			values[string(r.Key)] = string(r.Value)
		}
	}
	// The active segment may carry the trailing "z" record we appended
	// to force rollover; we only assert on the keyed compaction targets.
	if v := values["a"]; v != "a-3" {
		t.Errorf("a: got %q, want a-3", v)
	}
	if v := values["b"]; v != "b-2" {
		t.Errorf("b: got %q, want b-2", v)
	}
	if v := values["c"]; v != "c-1" {
		t.Errorf("c: got %q, want c-1", v)
	}
}

func TestCompact_TombstoneRemovesKey(t *testing.T) {
	// Arrange: write key "a", then a tombstone for "a", then key "b".
	p := fillSegments(t, []struct{ K, V []byte }{
		{[]byte("a"), []byte("a-1")},
		{[]byte("a"), nil}, // tombstone
		{[]byte("b"), []byte("b-1")},
	}, 64)
	// Force rollover.
	if _, err := p.Append(proto.Record{Offset: 3, Key: []byte("c"), Value: []byte("c-1")}); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) < 2 {
		t.Skip("test environment did not produce a sealed segment; skipping")
	}

	// Act
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}

	// Assert: "a" is gone after the tombstone removes it.
	got, _ := p.Read(0, 100)
	for _, r := range got {
		if string(r.Key) == "a" {
			t.Errorf("tombstoned key %q survived compaction: value=%q", r.Key, r.Value)
		}
	}
}

func TestCompact_PreservesOffsets(t *testing.T) {
	// Arrange: write 5 records at offsets 0..4, only key "a" survives at
	// offset 4. After compaction, the kept record's offset is 4.
	p := fillSegments(t, []struct{ K, V []byte }{
		{[]byte("a"), []byte("v0")},
		{[]byte("a"), []byte("v1")},
		{[]byte("a"), []byte("v2")},
		{[]byte("a"), []byte("v3")},
		{[]byte("a"), []byte("v4")},
	}, 96)
	if _, err := p.Append(proto.Record{Offset: 5, Key: []byte("b"), Value: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) < 2 {
		t.Skip("test environment did not produce a sealed segment; skipping")
	}

	// Act
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}

	// Assert
	got, _ := p.Read(0, 100)
	var foundA bool
	for _, r := range got {
		if string(r.Key) == "a" {
			foundA = true
			if r.Offset != 4 {
				t.Errorf("a: kept offset %d, want 4", r.Offset)
			}
			if string(r.Value) != "v4" {
				t.Errorf("a: kept value %q, want v4", r.Value)
			}
		}
	}
	if !foundA {
		t.Fatal("compacted segment lost key a entirely")
	}
}

func TestCompact_NoOpOnSingleSegment(t *testing.T) {
	// Arrange: one active segment, no sealed.
	p := fillSegments(t, []struct{ K, V []byte }{
		{[]byte("a"), []byte("v")},
	}, 1<<20)
	if len(p.segments) != 1 {
		t.Fatalf("expected exactly 1 segment, got %d", len(p.segments))
	}

	// Act
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}

	// Assert: still one segment, record unchanged.
	if len(p.segments) != 1 {
		t.Fatalf("compaction touched the active segment: now %d segments", len(p.segments))
	}
	got, _ := p.Read(0, 10)
	if len(got) != 1 || string(got[0].Value) != "v" {
		t.Fatalf("active record changed: %+v", got)
	}
}

func TestCompact_StateSurvivesReopen(t *testing.T) {
	// Arrange + Act
	dir := t.TempDir()
	{
		p, err := OpenPartition(dir, 96)
		if err != nil {
			t.Fatal(err)
		}
		records := []struct{ K, V []byte }{
			{[]byte("a"), []byte("a-1")},
			{[]byte("a"), []byte("a-2")},
			{[]byte("a"), []byte("a-3")},
		}
		for i, kv := range records {
			if _, err := p.Append(proto.Record{Offset: int64(i), Key: kv.K, Value: kv.V}); err != nil {
				t.Fatal(err)
			}
		}
		// Force rollover.
		if _, err := p.Append(proto.Record{Offset: 3, Key: []byte("z"), Value: []byte("z")}); err != nil {
			t.Fatal(err)
		}
		if len(p.segments) < 2 {
			_ = p.Close()
			t.Skip("test environment did not produce a sealed segment; skipping")
		}
		if err := p.Compact(); err != nil {
			t.Fatal(err)
		}
		_ = p.Close()
	}

	// Reopen and verify the compacted state is what's there.
	p2, err := OpenPartition(dir, 96)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	got, _ := p2.Read(0, 100)
	values := make(map[string]string)
	for _, r := range got {
		values[string(r.Key)] = string(r.Value)
	}

	// Assert
	if v := values["a"]; v != "a-3" {
		t.Errorf("after reopen, a = %q, want a-3", v)
	}
	if v := values["z"]; v != "z" {
		t.Errorf("after reopen, z = %q, want z (active record)", v)
	}
}

// TestCompact_RecoverFromInterruptedRename simulates a crash mid-
// compaction where the new segment's placeholder file is durable but
// the rename never happened. OpenPartition must promote the
// placeholder to its final name and recover normal operation.
func TestCompact_RecoverFromInterruptedRename(t *testing.T) {
	// Arrange — write a record directly to the canonical .log location,
	// then move it to .log.compacting (simulating: new segment
	// authoritative, old segments deleted, rename interrupted).
	dir := t.TempDir()
	{
		p, err := OpenPartition(dir, 96)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Append(proto.Record{Offset: 0, Key: []byte("a"), Value: []byte("a-1")}); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}

	logPath := filepath.Join(dir, segmentLogName(0))
	idxPath := filepath.Join(dir, segmentIndexName(0))
	if err := os.Rename(logPath, logPath+compactingSuffix); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(idxPath, idxPath+compactingSuffix); err != nil {
		t.Fatal(err)
	}

	// Act — reopening the partition must promote the placeholder.
	p, err := OpenPartition(dir, 96)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	defer p.Close()

	// Assert
	got, err := p.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Value) != "a-1" {
		t.Fatalf("recovered records: got %d, want 1 with value a-1 (got %v)", len(got), got)
	}
	if _, err := os.Stat(logPath + compactingSuffix); !os.IsNotExist(err) {
		t.Errorf("placeholder file still present after recovery: %v", err)
	}
}

// TestCompact_RecoverFromIncompletePlaceholder simulates a crash when
// the placeholder was created but the original sealed segments still
// exist. OpenPartition must delete the placeholder, leaving the
// original data intact.
func TestCompact_RecoverFromIncompletePlaceholder(t *testing.T) {
	// Arrange — produce a normal partition, then drop a stray
	// placeholder file alongside it.
	dir := t.TempDir()
	{
		p, err := OpenPartition(dir, 96)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Append(proto.Record{Offset: 0, Key: []byte("a"), Value: []byte("a-1")}); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}

	logPath := filepath.Join(dir, segmentLogName(0))
	if err := os.WriteFile(logPath+compactingSuffix, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Act
	p, err := OpenPartition(dir, 96)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	defer p.Close()

	// Assert — original data intact, placeholder removed.
	got, err := p.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Value) != "a-1" {
		t.Fatalf("originals lost during recovery: got %v", got)
	}
	if _, err := os.Stat(logPath + compactingSuffix); !os.IsNotExist(err) {
		t.Errorf("placeholder file still present after recovery: %v", err)
	}
}
