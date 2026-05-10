package cluster

import (
	"bytes"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestEncodeDecodeAppendRoundTrip(t *testing.T) {
	cmd := AppendCommand{
		Topic:     "events",
		Partition: 3,
		Offset:    42,
		Record: proto.Record{
			Timestamp: 1234,
			Key:       []byte("k"),
			Value:     []byte("payload"),
			Headers:   []proto.Header{{Key: "h", Value: []byte("v")}},
		},
	}
	enc := EncodeAppend(cmd)
	kind, body, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if kind != CmdAppend {
		t.Fatalf("kind: got 0x%02x, want CmdAppend", byte(kind))
	}
	got, err := DecodeAppend(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != cmd.Topic || got.Partition != cmd.Partition {
		t.Fatalf("metadata mismatch: %+v", got)
	}
	if got.Offset != cmd.Offset {
		t.Errorf("Offset round-trip: got %d, want %d", got.Offset, cmd.Offset)
	}
	if !bytes.Equal(got.Record.Key, cmd.Record.Key) || !bytes.Equal(got.Record.Value, cmd.Record.Value) {
		t.Fatalf("record mismatch: %+v", got.Record)
	}
}

// TestEncodeAppend_ZeroOffsetIsValid proves the Offset field's zero
// value round-trips cleanly. Until milestone 2 wires the leader-side
// stamping, every command encoded by the broker carries Offset=0
// and the FSM ignores it — so the zero value must not corrupt the
// rest of the layout.
func TestEncodeAppend_ZeroOffsetIsValid(t *testing.T) {
	cmd := AppendCommand{
		Topic:     "events",
		Partition: 0,
		Offset:    0,
		Record:    proto.Record{Value: []byte("v")},
	}
	enc := EncodeAppend(cmd)
	_, body, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeAppend(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.Offset != 0 {
		t.Errorf("Offset: got %d, want 0", got.Offset)
	}
	if !bytes.Equal(got.Record.Value, []byte("v")) {
		t.Errorf("record value: got %q, want v", got.Record.Value)
	}
}

func TestEncodeDecodeCreateTopicRoundTrip(t *testing.T) {
	cmd := CreateTopicCommand{
		Name:           "orders",
		PartitionCount: 8,
		RetentionMs:    3600000,
		SegmentBytes:   1 << 28,
	}
	enc := EncodeCreateTopic(cmd)
	kind, body, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if kind != CmdCreateTopic {
		t.Fatalf("kind: got 0x%02x, want CmdCreateTopic", byte(kind))
	}
	got, err := DecodeCreateTopic(body)
	if err != nil {
		t.Fatal(err)
	}
	if got != cmd {
		t.Fatalf("got %+v, want %+v", got, cmd)
	}
}

func TestDecodeRejectsEmpty(t *testing.T) {
	if _, _, err := Decode(nil); err == nil {
		t.Fatal("expected error on empty input")
	}
}
