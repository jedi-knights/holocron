// Package cluster wires hashicorp/raft into a holocron broker so that
// produce and topic-create operations are replicated across N nodes.
//
// One Raft cluster per broker (not per partition). Every accepted Append
// or CreateTopic becomes a Raft command; the FSM applies it to the local
// FileStore + Registry. Followers serve reads from their applied state
// (eventual consistency); writes must go through the leader.
//
// Per-partition Raft would let leadership spread across nodes for higher
// total throughput. Holocron picks one cluster-wide leader for simplicity;
// the limitation is documented in docs/stage-5.md.
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jedi-knights/holocron/proto"
)

// CommandKind tags each Raft log entry's payload type.
type CommandKind uint8

const (
	// CmdAppend records a single produce: append `Record` to (Topic, Partition).
	CmdAppend CommandKind = 1
	// CmdCreateTopic registers a topic with the given spec.
	CmdCreateTopic CommandKind = 2
	// CmdDeleteTopic removes a topic from the registry and tells the
	// store to drop every partition's segments. Replicated through
	// Raft so every node converges on the same topic set.
	CmdDeleteTopic CommandKind = 3
	// CmdUpdateTopicConfig changes a topic's retention and segment
	// settings. Partition count is immutable; only the soft knobs
	// are touched.
	CmdUpdateTopicConfig CommandKind = 4
)

// AppendCommand is the FSM payload for an append.
//
// Offset is the broker-assigned offset the leader stamps before
// submitting the command to Raft. Followers' FSMs read Offset
// (rather than letting their local store.Append assign one) so a
// fresh follower whose local store has been populated via the
// Stage 9 segment-sync path can dedup Raft entries it already
// holds — see docs/stage-9.md.
//
// Until milestone 2 wires leader-side stamping, every command
// carries Offset=0 and the FSM ignores it (current behavior:
// store.Append assigns the offset on every node). The wire field
// is reserved by milestone 1 so the format change is backwards-
// incompatible exactly once.
type AppendCommand struct {
	Topic     string
	Partition int32
	Offset    int64
	Record    proto.Record
}

// CreateTopicCommand is the FSM payload for a topic creation.
type CreateTopicCommand struct {
	Name           string
	PartitionCount int32
	RetentionMs    int64
	SegmentBytes   int64
}

// EncodeAppend serializes an AppendCommand. Layout:
//
//	[1] kind = CmdAppend
//	[4 + N] topic length + bytes
//	[4] partition (int32 BE)
//	[8] offset (int64 BE)
//	[record body — wire-format proto.Record minus its frame]
func EncodeAppend(c AppendCommand) []byte {
	buf := []byte{byte(CmdAppend)}
	buf = appendString(buf, c.Topic)
	buf = binary.BigEndian.AppendUint32(buf, uint32(c.Partition))
	buf = binary.BigEndian.AppendUint64(buf, uint64(c.Offset))
	rec := proto.ProduceRequest{Topic: c.Topic, Partition: c.Partition, Record: c.Record}.Encode()
	// ProduceRequest.Encode embeds topic+partition+record; for the FSM we
	// only need the record bytes, but reusing the encoder keeps the format
	// stable. Followers decode via DecodeProduceRequest.
	buf = append(buf, rec...)
	return buf
}

// EncodeCreateTopic serializes a CreateTopicCommand.
func EncodeCreateTopic(c CreateTopicCommand) []byte {
	buf := []byte{byte(CmdCreateTopic)}
	buf = appendString(buf, c.Name)
	buf = binary.BigEndian.AppendUint32(buf, uint32(c.PartitionCount))
	buf = binary.BigEndian.AppendUint64(buf, uint64(c.RetentionMs))
	buf = binary.BigEndian.AppendUint64(buf, uint64(c.SegmentBytes))
	return buf
}

// UpdateTopicConfigCommand is the FSM payload for a topic-config
// update. Retention/segment settings are mutable; partition count
// is not.
type UpdateTopicConfigCommand struct {
	Name         string
	RetentionMs  int64
	SegmentBytes int64
}

// EncodeUpdateTopicConfig serializes an UpdateTopicConfigCommand.
func EncodeUpdateTopicConfig(c UpdateTopicConfigCommand) []byte {
	buf := []byte{byte(CmdUpdateTopicConfig)}
	buf = appendString(buf, c.Name)
	buf = binary.BigEndian.AppendUint64(buf, uint64(c.RetentionMs))
	buf = binary.BigEndian.AppendUint64(buf, uint64(c.SegmentBytes))
	return buf
}

// DecodeUpdateTopicConfig parses an UpdateTopicConfigCommand body.
func DecodeUpdateTopicConfig(body []byte) (UpdateTopicConfigCommand, error) {
	name, n, err := readString(body)
	if err != nil {
		return UpdateTopicConfigCommand{}, fmt.Errorf("cluster: update-topic name: %w", err)
	}
	body = body[n:]
	if len(body) < 16 {
		return UpdateTopicConfigCommand{}, errors.New("cluster: short update-topic command")
	}
	return UpdateTopicConfigCommand{
		Name:         name,
		RetentionMs:  int64(binary.BigEndian.Uint64(body[0:8])),
		SegmentBytes: int64(binary.BigEndian.Uint64(body[8:16])),
	}, nil
}

// DeleteTopicCommand is the FSM payload for a topic deletion.
type DeleteTopicCommand struct {
	Name string
}

// EncodeDeleteTopic serializes a DeleteTopicCommand.
func EncodeDeleteTopic(c DeleteTopicCommand) []byte {
	buf := []byte{byte(CmdDeleteTopic)}
	return appendString(buf, c.Name)
}

// DecodeDeleteTopic parses a DeleteTopicCommand body.
func DecodeDeleteTopic(body []byte) (DeleteTopicCommand, error) {
	name, _, err := readString(body)
	if err != nil {
		return DeleteTopicCommand{}, fmt.Errorf("cluster: delete-topic name: %w", err)
	}
	return DeleteTopicCommand{Name: name}, nil
}

// Decode parses the leading kind byte and returns the kind plus the
// remaining body. The caller dispatches on kind.
func Decode(b []byte) (CommandKind, []byte, error) {
	if len(b) == 0 {
		return 0, nil, errors.New("cluster: empty command")
	}
	return CommandKind(b[0]), b[1:], nil
}

// DecodeAppend parses an AppendCommand body (kind byte already consumed).
func DecodeAppend(body []byte) (AppendCommand, error) {
	topic, n, err := readString(body)
	if err != nil {
		return AppendCommand{}, fmt.Errorf("cluster: append topic: %w", err)
	}
	body = body[n:]
	if len(body) < 12 {
		return AppendCommand{}, errors.New("cluster: short append command")
	}
	partition := int32(binary.BigEndian.Uint32(body[0:4]))
	offset := int64(binary.BigEndian.Uint64(body[4:12]))
	body = body[12:]
	req, err := proto.DecodeProduceRequest(body)
	if err != nil {
		return AppendCommand{}, fmt.Errorf("cluster: append record: %w", err)
	}
	return AppendCommand{Topic: topic, Partition: partition, Offset: offset, Record: req.Record}, nil
}

// DecodeCreateTopic parses a CreateTopicCommand body.
func DecodeCreateTopic(body []byte) (CreateTopicCommand, error) {
	name, n, err := readString(body)
	if err != nil {
		return CreateTopicCommand{}, fmt.Errorf("cluster: create-topic name: %w", err)
	}
	body = body[n:]
	if len(body) < 20 {
		return CreateTopicCommand{}, errors.New("cluster: short create-topic command")
	}
	return CreateTopicCommand{
		Name:           name,
		PartitionCount: int32(binary.BigEndian.Uint32(body[0:4])),
		RetentionMs:    int64(binary.BigEndian.Uint64(body[4:12])),
		SegmentBytes:   int64(binary.BigEndian.Uint64(body[12:20])),
	}, nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

func readString(b []byte) (string, int, error) {
	if len(b) < 4 {
		return "", 0, errors.New("cluster: short string")
	}
	n := binary.BigEndian.Uint32(b[:4])
	if uint32(len(b)-4) < n {
		return "", 0, errors.New("cluster: truncated string")
	}
	return string(b[4 : 4+n]), 4 + int(n), nil
}
