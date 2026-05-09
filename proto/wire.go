package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// wireCRCTable matches the polynomial used by broker/internal/log/frame.go.
// Disk and wire share the same CRC so a future sendfile(2) fast path
// can stream bytes directly from segment files to network connections
// without re-framing.
var wireCRCTable = crc32.MakeTable(crc32.Castagnoli)

// WireVersion is the on-wire protocol version. Bump whenever the layout
// changes; both broker and SDK refuse mismatched versions.
//
//	v1: initial release.
//	v2: adds the codec byte to ProduceBatchRequest for LZ4 compression.
//	v3: FetchResponse records carry the disk-format envelope
//	    (length + body + CRC32C) so future sendfile(2) implementations
//	    can stream segment bytes directly to the wire.
//	v4: HandshakeRequest carries an API key so brokers can authenticate
//	    SDK clients before any other RPC.
//	v5: FetchRequest carries an AcceptCodec byte and FetchResponse a
//	    Codec byte so brokers can ship records LZ4-compressed when the
//	    client signals support, symmetric with the produce-side codec
//	    introduced in v2.
//	v6: HeartbeatRequest carries a MaxWaitMs field so the broker can
//	    long-poll the heartbeat — holding the response open until a
//	    rebalance is needed or the deadline elapses, delivering the
//	    server-pushed rebalance signal that closes the
//	    duplicate-production window during rebalance.
//	v7: PartitionSnapshot replaced by chunked ListSegments +
//	    FetchSegmentChunk so large partitions don't OOM the broker
//	    on a single response, and the donor's active segment is
//	    safely included in the snapshot via byte-range reads bounded
//	    by the size captured at list time.
//	v8: ListGroupOffsets — operator-facing enumeration of a
//	    consumer group's committed offsets paired with each
//	    partition's high-water so an operator-side tool can compute
//	    lag in one round-trip.
//	v9: DeleteGroup — operator-driven removal of a group and
//	    every committed offset under it. Pairs with the cleanup
//	    workflow surfaced by `holocronctl group delete`.
const WireVersion uint8 = 9

// OpCode names a request type. Responses echo the same opcode.
type OpCode uint8

const (
	OpProduce      OpCode = 0x01
	OpFetch        OpCode = 0x02
	OpMetadata     OpCode = 0x03
	OpCreateTopic  OpCode = 0x04
	OpCommit       OpCode = 0x05
	OpHandshake    OpCode = 0x06
	OpJoinGroup    OpCode = 0x07
	OpHeartbeat    OpCode = 0x08
	OpLeaveGroup   OpCode = 0x09
	OpSync         OpCode = 0x0A
	OpProduceBatch    OpCode = 0x0B
	OpHighWater       OpCode = 0x0C
	OpClusterMembers  OpCode = 0x0D
	OpAddVoter        OpCode = 0x0E
	OpRemoveVoter     OpCode = 0x0F
	// OpListSegments returns metadata for every segment in a
	// partition — base offset and current (.log, .idx) sizes —
	// captured under the partition's mutex so subsequent
	// FetchSegmentChunk calls can read a self-consistent prefix even
	// while the active segment is being appended to.
	OpListSegments OpCode = 0x10
	// OpFetchSegmentChunk returns a byte range from a specific
	// segment file. Pairs with OpListSegments to ship segments to a
	// brand-new follower in bounded-memory chunks.
	OpFetchSegmentChunk OpCode = 0x11
	// OpDeleteTopic removes a topic and every partition's records.
	// Replicated through Raft on a clustered broker; on a single
	// broker the registry and storage are updated directly.
	OpDeleteTopic OpCode = 0x12
	// OpListTopics returns the broker's full topic registry —
	// every TopicConfig the broker currently knows about. Replaces
	// the probe-by-name workaround the CLI used pre-batch-23.
	OpListTopics OpCode = 0x13
	// OpListGroups returns a summary (name, generation, member
	// count, subscribed topics) of every consumer group registered
	// with the manager.
	OpListGroups OpCode = 0x14
	// OpDescribeGroup returns per-member partition assignments for
	// a single named group.
	OpDescribeGroup OpCode = 0x15
	// OpClusterStatus reports leader info — leader ID, leader's
	// Raft RPC address, and whether the responding broker is the
	// leader. Pairs with OpClusterMembers (which returns member
	// metadata only) so an operator can see the full picture in
	// one CLI invocation.
	OpClusterStatus OpCode = 0x16
	// OpUpdateTopicConfig changes the retention and segment-size
	// settings of an existing topic. Partition count is immutable
	// (changing it would break ordering invariants); only soft
	// configuration knobs are updatable.
	OpUpdateTopicConfig OpCode = 0x17
	// OpListGroupOffsets returns every (topic, partition, committed,
	// high-water) tuple a group has touched. The lag column is
	// computed by the caller (high-water - committed); the broker
	// emits the raw pair so the operator can see whether the high
	// water is what they expected.
	OpListGroupOffsets OpCode = 0x18
	// OpDeleteGroup drops a consumer group from the broker's
	// in-memory registry and clears every committed offset under
	// that group. Idempotent — a missing group surfaces as
	// StatusUnknownMember so an operator script can squelch it.
	OpDeleteGroup OpCode = 0x19
)

// Status is the first byte of every response body.
type Status uint8

const (
	StatusOK               Status = 0x00
	StatusUnknownTopic     Status = 0x10
	StatusInvalidPartition Status = 0x11
	StatusInvalidRequest   Status = 0x12
	StatusTopicExists      Status = 0x13
	StatusVersionMismatch  Status = 0x20
	StatusUnknownMember    Status = 0x30
	StatusRebalanceNeeded  Status = 0x31
	StatusNotLeader        Status = 0x40
	StatusUnauthorized     Status = 0x50
	StatusForbidden        Status = 0x51
	StatusRateLimited      Status = 0x60
	StatusInternal         Status = 0xFF
)

// ProtocolError is returned by client code when the broker reports a
// non-OK status. The error wraps both the status code and the broker's
// message.
type ProtocolError struct {
	Status  Status
	Message string
}

func (e *ProtocolError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("broker error: status 0x%02x", byte(e.Status))
	}
	return fmt.Sprintf("broker error 0x%02x: %s", byte(e.Status), e.Message)
}

// IsStatus reports whether err is a ProtocolError with the given status.
func IsStatus(err error, s Status) bool {
	var pe *ProtocolError
	return errors.As(err, &pe) && pe.Status == s
}

// Wire frame:
//
//	[4 bytes: payload length, BE uint32]
//	[1 byte:  opcode]
//	[payload]
//
// On responses the payload begins with a 1-byte status; status != OK means
// the rest of the payload is a length-prefixed UTF-8 error message.

// ReadFrame reads a single wire frame from r. It returns the opcode and
// the bytes that follow it (i.e., the request or response body).
func ReadFrame(r io.Reader) (OpCode, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return 0, nil, errors.New("proto: empty frame")
	}
	if n > maxFrameBytes {
		return 0, nil, fmt.Errorf("proto: frame too large (%d bytes)", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return OpCode(body[0]), body[1:], nil
}

// WriteFrame writes opcode + payload to w as a single wire frame.
func WriteFrame(w io.Writer, op OpCode, payload []byte) error {
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(1+len(payload)))
	hdr[4] = byte(op)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// WriteResponse writes a response frame with status + body.
func WriteResponse(w io.Writer, op OpCode, status Status, body []byte) error {
	payload := make([]byte, 0, 1+len(body))
	payload = append(payload, byte(status))
	payload = append(payload, body...)
	return WriteFrame(w, op, payload)
}

// WriteErrorResponse writes a non-OK response with a UTF-8 message.
func WriteErrorResponse(w io.Writer, op OpCode, status Status, msg string) error {
	body := encodeString(nil, msg)
	return WriteResponse(w, op, status, body)
}

// ReadResponse reads a response frame, expecting opcode op. It returns the
// body that follows the status byte; non-OK responses are returned as a
// *ProtocolError.
func ReadResponse(r io.Reader, op OpCode) ([]byte, error) {
	got, body, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	if got != op {
		return nil, fmt.Errorf("proto: expected opcode 0x%02x, got 0x%02x", byte(op), byte(got))
	}
	if len(body) == 0 {
		return nil, errors.New("proto: response missing status byte")
	}
	status := Status(body[0])
	rest := body[1:]
	if status == StatusOK {
		return rest, nil
	}
	msg, _, err := decodeString(rest)
	if err != nil {
		msg = ""
	}
	return nil, &ProtocolError{Status: status, Message: msg}
}

// ===== Request / Response payload types =====

// HandshakeRequest is the first message on every connection. Beyond
// the wire version (used for compatibility checking), it carries an
// optional API key — brokers configured with an allow-list of keys
// reject connections that don't present a matching one.
type HandshakeRequest struct {
	Version uint8
	APIKey  string
}

func (h HandshakeRequest) Encode() []byte {
	b := []byte{h.Version}
	b = encodeString(b, h.APIKey)
	return b
}

func DecodeHandshakeRequest(b []byte) (HandshakeRequest, error) {
	if len(b) < 1 {
		return HandshakeRequest{}, errors.New("proto: bad handshake")
	}
	version := b[0]
	rest := b[1:]
	if len(rest) == 0 {
		// v1–v3 compatibility — no API key field. Allowed only when
		// the local WireVersion check itself rejects the older client.
		return HandshakeRequest{Version: version}, nil
	}
	key, _, err := decodeString(rest)
	if err != nil {
		return HandshakeRequest{}, fmt.Errorf("proto: handshake api key: %w", err)
	}
	return HandshakeRequest{Version: version, APIKey: key}, nil
}

// ProduceRequest carries a single record to be appended.
type ProduceRequest struct {
	Topic     string
	Partition int32
	Record    Record // server assigns Offset; client should leave it 0
}

func (r ProduceRequest) Encode() []byte {
	b := encodeString(nil, r.Topic)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Partition))
	b = encodeRecord(b, r.Record)
	return b
}

func DecodeProduceRequest(b []byte) (ProduceRequest, error) {
	topic, n, err := decodeString(b)
	if err != nil {
		return ProduceRequest{}, err
	}
	b = b[n:]
	if len(b) < 4 {
		return ProduceRequest{}, errors.New("proto: short ProduceRequest")
	}
	part := int32(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	rec, _, err := decodeRecord(b)
	if err != nil {
		return ProduceRequest{}, fmt.Errorf("proto: ProduceRequest record: %w", err)
	}
	return ProduceRequest{Topic: topic, Partition: part, Record: rec}, nil
}

// ProduceResponse carries the offset assigned by the broker.
type ProduceResponse struct {
	Offset int64
}

func (r ProduceResponse) Encode() []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(r.Offset))
}

func DecodeProduceResponse(b []byte) (ProduceResponse, error) {
	if len(b) < 8 {
		return ProduceResponse{}, errors.New("proto: short ProduceResponse")
	}
	return ProduceResponse{Offset: int64(binary.BigEndian.Uint64(b[:8]))}, nil
}

// Codec names the compression applied to a batch's record payload. v2
// of the wire protocol adds a codec byte to ProduceBatchRequest so
// producers can opt into LZ4 compression.
type Codec uint8

const (
	// CodecNone leaves the records uncompressed on the wire.
	CodecNone Codec = 0
	// CodecLZ4 compresses the records portion with LZ4 block format.
	CodecLZ4 Codec = 1
)

// ProduceBatchRequest carries N records destined for the same partition.
// Records are appended in the order they appear; the response carries
// the offset assigned to the first record (subsequent records have
// contiguous offsets).
//
// Wire layout:
//
//	[topic][partition u32][codec u8]
//	if codec == CodecNone:
//	    [count u32][record][record]...
//	if codec == CodecLZ4:
//	    [count u32][uncompressedLen u32][lz4Bytes]
//	    where decompressing lz4Bytes yields the [record][record]... payload.
type ProduceBatchRequest struct {
	Topic     string
	Partition int32
	Codec     Codec
	Records   []Record
	// Level is a producer-side encoding hint, not part of the wire
	// format. 0 picks the fast LZ4 compressor (default); 1..9 use
	// LZ4-HC at that level for higher compression ratio at the
	// cost of CPU. The decoder is unaffected — both variants emit
	// the same on-the-wire format.
	Level uint8
}

func (r ProduceBatchRequest) Encode() []byte {
	b := encodeString(nil, r.Topic)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Partition))
	b = append(b, byte(r.Codec))
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Records)))

	var recordsBuf []byte
	for _, rec := range r.Records {
		recordsBuf = encodeRecord(recordsBuf, rec)
	}

	switch r.Codec {
	case CodecLZ4:
		compressed, err := lz4CompressLevel(recordsBuf, int(r.Level))
		if err != nil {
			// Fall back to uncompressed; document the failure in the
			// codec byte so the decoder doesn't try to decompress
			// nonsense.
			b[len(b)-5] = byte(CodecNone) // overwrite codec byte
			b = append(b, recordsBuf...)
			return b
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(recordsBuf)))
		b = append(b, compressed...)
	default:
		b = append(b, recordsBuf...)
	}
	return b
}

func DecodeProduceBatchRequest(b []byte) (ProduceBatchRequest, error) {
	topic, n, err := decodeString(b)
	if err != nil {
		return ProduceBatchRequest{}, err
	}
	b = b[n:]
	if len(b) < 5 {
		return ProduceBatchRequest{}, errors.New("proto: short ProduceBatchRequest")
	}
	partition := int32(binary.BigEndian.Uint32(b[:4]))
	codec := Codec(b[4])
	b = b[5:]
	if len(b) < 4 {
		return ProduceBatchRequest{}, errors.New("proto: short ProduceBatchRequest count")
	}
	count := binary.BigEndian.Uint32(b[:4])
	b = b[4:]

	var recordsBuf []byte
	switch codec {
	case CodecLZ4:
		if len(b) < 4 {
			return ProduceBatchRequest{}, errors.New("proto: short LZ4 length")
		}
		uncompressedLen := binary.BigEndian.Uint32(b[:4])
		b = b[4:]
		decoded, err := lz4Decompress(b, int(uncompressedLen))
		if err != nil {
			return ProduceBatchRequest{}, fmt.Errorf("proto: lz4 decompress: %w", err)
		}
		recordsBuf = decoded
	case CodecNone:
		recordsBuf = b
	default:
		return ProduceBatchRequest{}, fmt.Errorf("proto: unknown codec 0x%02x", byte(codec))
	}

	records := make([]Record, 0, count)
	for range count {
		rec, m, err := decodeRecord(recordsBuf)
		if err != nil {
			return ProduceBatchRequest{}, fmt.Errorf("proto: ProduceBatchRequest record: %w", err)
		}
		records = append(records, rec)
		recordsBuf = recordsBuf[m:]
	}
	return ProduceBatchRequest{Topic: topic, Partition: partition, Codec: codec, Records: records}, nil
}

// ProduceBatchResponse carries the broker-assigned offsets for each record.
type ProduceBatchResponse struct {
	BaseOffset int64
	Count      int32
}

func (r ProduceBatchResponse) Encode() []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(r.BaseOffset))
	b = binary.BigEndian.AppendUint32(b, uint32(r.Count))
	return b
}

func DecodeProduceBatchResponse(b []byte) (ProduceBatchResponse, error) {
	if len(b) < 12 {
		return ProduceBatchResponse{}, errors.New("proto: short ProduceBatchResponse")
	}
	return ProduceBatchResponse{
		BaseOffset: int64(binary.BigEndian.Uint64(b[:8])),
		Count:      int32(binary.BigEndian.Uint32(b[8:12])),
	}, nil
}

// FetchRequest is a long-poll read. AcceptCodec signals which response
// compression codec the client accepts; the server picks one or falls
// back to CodecNone.
type FetchRequest struct {
	Topic       string
	Partition   int32
	FromOffset  int64
	MaxRecords  int32
	MaxWaitMs   int32
	AcceptCodec Codec
}

func (r FetchRequest) Encode() []byte {
	b := encodeString(nil, r.Topic)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Partition))
	b = binary.BigEndian.AppendUint64(b, uint64(r.FromOffset))
	b = binary.BigEndian.AppendUint32(b, uint32(r.MaxRecords))
	b = binary.BigEndian.AppendUint32(b, uint32(r.MaxWaitMs))
	b = append(b, byte(r.AcceptCodec))
	return b
}

func DecodeFetchRequest(b []byte) (FetchRequest, error) {
	topic, n, err := decodeString(b)
	if err != nil {
		return FetchRequest{}, err
	}
	b = b[n:]
	if len(b) < 21 {
		return FetchRequest{}, errors.New("proto: short FetchRequest")
	}
	return FetchRequest{
		Topic:       topic,
		Partition:   int32(binary.BigEndian.Uint32(b[0:4])),
		FromOffset:  int64(binary.BigEndian.Uint64(b[4:12])),
		MaxRecords:  int32(binary.BigEndian.Uint32(b[12:16])),
		MaxWaitMs:   int32(binary.BigEndian.Uint32(b[16:20])),
		AcceptCodec: Codec(b[20]),
	}, nil
}

// FetchResponse carries zero or more records, each wrapped in the
// disk-format envelope (4-byte body length + body + 4-byte CRC32C).
// Wire-byte-equivalence with the on-disk segment format means a
// future sendfile(2) implementation can stream segment bytes directly
// to the wire.
//
// Wire layout:
//
//	[codec u8]
//	if codec == CodecNone:
//	  [count u32][bodyLen u32][body][crc u32] ...
//	if codec == CodecLZ4:
//	  [uncompressedLen u32][compressed bytes containing the codec=None payload]
type FetchResponse struct {
	Records []Record
	Codec   Codec
}

// fetchPayload encodes the count + framed records portion. Used both
// for the codec=None inline path and as the source bytes for codec=LZ4
// compression.
func (r FetchResponse) fetchPayload() []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(r.Records)))
	for _, rec := range r.Records {
		b = encodeFramedRecord(b, rec)
	}
	return b
}

func (r FetchResponse) Encode() []byte {
	if r.Codec != CodecLZ4 {
		// CodecNone (or any unknown codec we'd emit as None): inline.
		b := []byte{byte(CodecNone)}
		return append(b, r.fetchPayload()...)
	}
	payload := r.fetchPayload()
	compressed, err := lz4Compress(payload)
	if err != nil {
		// Compression refused (e.g., empty payload): fall back to
		// codec=None so the wire response is still well-formed.
		b := []byte{byte(CodecNone)}
		return append(b, payload...)
	}
	b := []byte{byte(CodecLZ4)}
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, compressed...)
}

func DecodeFetchResponse(b []byte) (FetchResponse, error) {
	if len(b) < 1 {
		return FetchResponse{}, errors.New("proto: short FetchResponse")
	}
	codec := Codec(b[0])
	b = b[1:]
	if codec == CodecLZ4 {
		if len(b) < 4 {
			return FetchResponse{}, errors.New("proto: short FetchResponse lz4 header")
		}
		uncompressedLen := int(binary.BigEndian.Uint32(b[:4]))
		decoded, err := lz4Decompress(b[4:], uncompressedLen)
		if err != nil {
			return FetchResponse{}, fmt.Errorf("proto: FetchResponse lz4: %w", err)
		}
		b = decoded
	}
	if len(b) < 4 {
		return FetchResponse{}, errors.New("proto: short FetchResponse count")
	}
	count := binary.BigEndian.Uint32(b[:4])
	b = b[4:]
	out := make([]Record, 0, count)
	for range count {
		rec, n, err := decodeFramedRecord(b)
		if err != nil {
			return FetchResponse{}, fmt.Errorf("proto: FetchResponse record: %w", err)
		}
		out = append(out, rec)
		b = b[n:]
	}
	return FetchResponse{Records: out, Codec: codec}, nil
}

// encodeFramedRecord writes a record in the disk-format envelope:
// 4-byte body length, body, 4-byte CRC32C (Castagnoli) of the body.
// Identical layout to broker/internal/log/frame.go encodeRecord — that
// equivalence is what enables sendfile-style zero-copy streaming.
func encodeFramedRecord(buf []byte, r Record) []byte {
	bodyStart := len(buf) + 4
	buf = append(buf, 0, 0, 0, 0)
	buf = encodeRecord(buf, r)
	body := buf[bodyStart:]
	binary.BigEndian.PutUint32(buf[bodyStart-4:bodyStart], uint32(len(body)))
	crc := crc32.Checksum(body, wireCRCTable)
	return binary.BigEndian.AppendUint32(buf, crc)
}

// decodeFramedRecord reads one disk-formatted record from b. Returns
// the record and the total bytes consumed (length prefix + body + CRC).
func decodeFramedRecord(b []byte) (Record, int, error) {
	if len(b) < 4 {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	bodyLen := binary.BigEndian.Uint32(b[:4])
	total := 4 + int(bodyLen) + 4
	if len(b) < total {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	body := b[4 : 4+bodyLen]
	gotCRC := binary.BigEndian.Uint32(b[4+bodyLen : total])
	if expected := crc32.Checksum(body, wireCRCTable); expected != gotCRC {
		return Record{}, 0, fmt.Errorf("proto: framed-record CRC mismatch: got %x want %x", gotCRC, expected)
	}
	rec, _, err := decodeRecord(body)
	if err != nil {
		return Record{}, 0, err
	}
	return rec, total, nil
}

// MetadataRequest asks for the partition count of a topic.
type MetadataRequest struct {
	Topic string
}

func (r MetadataRequest) Encode() []byte { return encodeString(nil, r.Topic) }

func DecodeMetadataRequest(b []byte) (MetadataRequest, error) {
	topic, _, err := decodeString(b)
	if err != nil {
		return MetadataRequest{}, err
	}
	return MetadataRequest{Topic: topic}, nil
}

// MetadataResponse returns partition count.
type MetadataResponse struct {
	PartitionCount int32
}

func (r MetadataResponse) Encode() []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(r.PartitionCount))
}

func DecodeMetadataResponse(b []byte) (MetadataResponse, error) {
	if len(b) < 4 {
		return MetadataResponse{}, errors.New("proto: short MetadataResponse")
	}
	return MetadataResponse{PartitionCount: int32(binary.BigEndian.Uint32(b[:4]))}, nil
}

// HighWaterRequest asks for the next-to-be-appended offset of a
// (topic, partition) pair.
type HighWaterRequest struct {
	Topic     string
	Partition int32
}

func (r HighWaterRequest) Encode() []byte {
	b := encodeString(nil, r.Topic)
	return binary.BigEndian.AppendUint32(b, uint32(r.Partition))
}

func DecodeHighWaterRequest(b []byte) (HighWaterRequest, error) {
	topic, off, err := decodeString(b)
	if err != nil {
		return HighWaterRequest{}, err
	}
	if len(b)-off < 4 {
		return HighWaterRequest{}, errors.New("proto: short HighWaterRequest")
	}
	return HighWaterRequest{
		Topic:     topic,
		Partition: int32(binary.BigEndian.Uint32(b[off : off+4])),
	}, nil
}

// HighWaterResponse returns the next-to-be-appended offset.
type HighWaterResponse struct {
	HighWater int64
}

func (r HighWaterResponse) Encode() []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(r.HighWater))
}

func DecodeHighWaterResponse(b []byte) (HighWaterResponse, error) {
	if len(b) < 8 {
		return HighWaterResponse{}, errors.New("proto: short HighWaterResponse")
	}
	return HighWaterResponse{HighWater: int64(binary.BigEndian.Uint64(b[:8]))}, nil
}

// SegmentKind tags which file of a segment is being addressed in
// FetchSegmentChunk: the .log (the records) or the .idx (the sparse
// index). Mapped to a single byte over the wire.
type SegmentKind uint8

const (
	// SegmentLog addresses the segment's .log file (record bytes).
	SegmentLog SegmentKind = 0
	// SegmentIdx addresses the segment's .idx file (sparse index).
	SegmentIdx SegmentKind = 1
)

// SegmentInfo describes a single segment in a partition: its base
// offset and the current sizes of the .log and .idx files. Sizes are
// captured under the partition's mutex when the broker assembles a
// ListSegmentsResponse so a follower can read a self-consistent
// prefix of every segment — including the active one — bounded by
// the listed sizes.
type SegmentInfo struct {
	Base    int64
	LogSize int64
	IdxSize int64
}

// ListSegmentsRequest asks the broker for the segment manifest of a
// partition.
type ListSegmentsRequest struct {
	Topic     string
	Partition int32
}

func (r ListSegmentsRequest) Encode() []byte {
	b := encodeString(nil, r.Topic)
	return binary.BigEndian.AppendUint32(b, uint32(r.Partition))
}

func DecodeListSegmentsRequest(b []byte) (ListSegmentsRequest, error) {
	topic, off, err := decodeString(b)
	if err != nil {
		return ListSegmentsRequest{}, err
	}
	if len(b)-off < 4 {
		return ListSegmentsRequest{}, errors.New("proto: short ListSegmentsRequest")
	}
	return ListSegmentsRequest{
		Topic:     topic,
		Partition: int32(binary.BigEndian.Uint32(b[off : off+4])),
	}, nil
}

// ListSegmentsResponse carries the segment manifest. Segments appear
// in ascending base-offset order; the highest-base segment is the
// donor's currently active segment.
type ListSegmentsResponse struct {
	Segments []SegmentInfo
}

func (r ListSegmentsResponse) Encode() []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(r.Segments)))
	for _, s := range r.Segments {
		b = binary.BigEndian.AppendUint64(b, uint64(s.Base))
		b = binary.BigEndian.AppendUint64(b, uint64(s.LogSize))
		b = binary.BigEndian.AppendUint64(b, uint64(s.IdxSize))
	}
	return b
}

func DecodeListSegmentsResponse(b []byte) (ListSegmentsResponse, error) {
	if len(b) < 4 {
		return ListSegmentsResponse{}, errors.New("proto: short ListSegmentsResponse")
	}
	count := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	if len(b) < count*24 {
		return ListSegmentsResponse{}, errors.New("proto: short ListSegmentsResponse entries")
	}
	out := make([]SegmentInfo, 0, count)
	for range count {
		out = append(out, SegmentInfo{
			Base:    int64(binary.BigEndian.Uint64(b[0:8])),
			LogSize: int64(binary.BigEndian.Uint64(b[8:16])),
			IdxSize: int64(binary.BigEndian.Uint64(b[16:24])),
		})
		b = b[24:]
	}
	return ListSegmentsResponse{Segments: out}, nil
}

// FetchSegmentChunkRequest reads a byte range from one segment file.
// Callers pair Base + Kind to address the file; (Offset, MaxBytes)
// bounds the read. The broker returns up to MaxBytes; fewer bytes
// signal end-of-file at the listed size.
type FetchSegmentChunkRequest struct {
	Topic     string
	Partition int32
	Base      int64
	Kind      SegmentKind
	Offset    int64
	MaxBytes  int32
}

func (r FetchSegmentChunkRequest) Encode() []byte {
	b := encodeString(nil, r.Topic)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Partition))
	b = binary.BigEndian.AppendUint64(b, uint64(r.Base))
	b = append(b, byte(r.Kind))
	b = binary.BigEndian.AppendUint64(b, uint64(r.Offset))
	return binary.BigEndian.AppendUint32(b, uint32(r.MaxBytes))
}

func DecodeFetchSegmentChunkRequest(b []byte) (FetchSegmentChunkRequest, error) {
	topic, off, err := decodeString(b)
	if err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	b = b[off:]
	if len(b) < 4+8+1+8+4 {
		return FetchSegmentChunkRequest{}, errors.New("proto: short FetchSegmentChunkRequest")
	}
	return FetchSegmentChunkRequest{
		Topic:     topic,
		Partition: int32(binary.BigEndian.Uint32(b[0:4])),
		Base:      int64(binary.BigEndian.Uint64(b[4:12])),
		Kind:      SegmentKind(b[12]),
		Offset:    int64(binary.BigEndian.Uint64(b[13:21])),
		MaxBytes:  int32(binary.BigEndian.Uint32(b[21:25])),
	}, nil
}

// FetchSegmentChunkResponse carries the requested byte range.
type FetchSegmentChunkResponse struct {
	Bytes []byte
}

func (r FetchSegmentChunkResponse) Encode() []byte {
	return encodeBytes(nil, r.Bytes)
}

func DecodeFetchSegmentChunkResponse(b []byte) (FetchSegmentChunkResponse, error) {
	bytes, _, err := decodeBytes(b)
	if err != nil {
		return FetchSegmentChunkResponse{}, err
	}
	return FetchSegmentChunkResponse{Bytes: bytes}, nil
}

// ClusterMember pairs a voter ID with its Raft RPC address.
type ClusterMember struct {
	ID   string
	Addr string
}

// ClusterMembersRequest takes no parameters; the response carries the
// current Raft configuration. Routed locally — any node can serve it.
type ClusterMembersRequest struct{}

func (ClusterMembersRequest) Encode() []byte                        { return nil }
func DecodeClusterMembersRequest(_ []byte) (ClusterMembersRequest, error) {
	return ClusterMembersRequest{}, nil
}

// ClusterMembersResponse carries the per-voter ID + address tuples.
type ClusterMembersResponse struct {
	Members []ClusterMember
}

func (r ClusterMembersResponse) Encode() []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(r.Members)))
	for _, m := range r.Members {
		b = encodeString(b, m.ID)
		b = encodeString(b, m.Addr)
	}
	return b
}

func DecodeClusterMembersResponse(b []byte) (ClusterMembersResponse, error) {
	if len(b) < 4 {
		return ClusterMembersResponse{}, errors.New("proto: short ClusterMembersResponse")
	}
	count := binary.BigEndian.Uint32(b[:4])
	b = b[4:]
	out := make([]ClusterMember, 0, count)
	for range count {
		id, n, err := decodeString(b)
		if err != nil {
			return ClusterMembersResponse{}, err
		}
		b = b[n:]
		addr, n2, err := decodeString(b)
		if err != nil {
			return ClusterMembersResponse{}, err
		}
		b = b[n2:]
		out = append(out, ClusterMember{ID: id, Addr: addr})
	}
	return ClusterMembersResponse{Members: out}, nil
}

// AddVoterRequest is a leader-only RPC. Followers respond with
// StatusNotLeader so the SDK can redirect.
type AddVoterRequest struct {
	ID   string
	Addr string
}

func (r AddVoterRequest) Encode() []byte {
	b := encodeString(nil, r.ID)
	return encodeString(b, r.Addr)
}

func DecodeAddVoterRequest(b []byte) (AddVoterRequest, error) {
	id, n, err := decodeString(b)
	if err != nil {
		return AddVoterRequest{}, err
	}
	addr, _, err := decodeString(b[n:])
	if err != nil {
		return AddVoterRequest{}, err
	}
	return AddVoterRequest{ID: id, Addr: addr}, nil
}

// RemoveVoterRequest is the leader-only counterpart.
type RemoveVoterRequest struct {
	ID string
}

func (r RemoveVoterRequest) Encode() []byte { return encodeString(nil, r.ID) }
func DecodeRemoveVoterRequest(b []byte) (RemoveVoterRequest, error) {
	id, _, err := decodeString(b)
	if err != nil {
		return RemoveVoterRequest{}, err
	}
	return RemoveVoterRequest{ID: id}, nil
}

// CreateTopicRequest registers a new topic.
type CreateTopicRequest struct {
	Name           string
	PartitionCount int32
	RetentionMs    int64
	SegmentBytes   int64
}

func (r CreateTopicRequest) Encode() []byte {
	b := encodeString(nil, r.Name)
	b = binary.BigEndian.AppendUint32(b, uint32(r.PartitionCount))
	b = binary.BigEndian.AppendUint64(b, uint64(r.RetentionMs))
	b = binary.BigEndian.AppendUint64(b, uint64(r.SegmentBytes))
	return b
}

func DecodeCreateTopicRequest(b []byte) (CreateTopicRequest, error) {
	name, n, err := decodeString(b)
	if err != nil {
		return CreateTopicRequest{}, err
	}
	b = b[n:]
	if len(b) < 20 {
		return CreateTopicRequest{}, errors.New("proto: short CreateTopicRequest")
	}
	return CreateTopicRequest{
		Name:           name,
		PartitionCount: int32(binary.BigEndian.Uint32(b[0:4])),
		RetentionMs:    int64(binary.BigEndian.Uint64(b[4:12])),
		SegmentBytes:   int64(binary.BigEndian.Uint64(b[12:20])),
	}, nil
}

// ClusterStatusRequest asks the broker to report its Raft leader
// info. Request body is empty.
type ClusterStatusRequest struct{}

func (r ClusterStatusRequest) Encode() []byte { return nil }

func DecodeClusterStatusRequest(_ []byte) (ClusterStatusRequest, error) {
	return ClusterStatusRequest{}, nil
}

// ClusterStatusResponse carries the responding broker's Raft
// leader view. NodeID is this broker's node ID; IsLeader reports
// whether it currently holds leadership; LeaderID/LeaderAddr name
// the leader the broker last observed (may be empty during an
// election or when this broker isn't part of a cluster).
type ClusterStatusResponse struct {
	NodeID     string
	IsLeader   bool
	LeaderID   string
	LeaderAddr string
}

func (r ClusterStatusResponse) Encode() []byte {
	b := encodeString(nil, r.NodeID)
	if r.IsLeader {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = encodeString(b, r.LeaderID)
	return encodeString(b, r.LeaderAddr)
}

func DecodeClusterStatusResponse(b []byte) (ClusterStatusResponse, error) {
	nodeID, n, err := decodeString(b)
	if err != nil {
		return ClusterStatusResponse{}, err
	}
	b = b[n:]
	if len(b) < 1 {
		return ClusterStatusResponse{}, errors.New("proto: short ClusterStatusResponse")
	}
	isLeader := b[0] != 0
	b = b[1:]
	leaderID, n, err := decodeString(b)
	if err != nil {
		return ClusterStatusResponse{}, err
	}
	b = b[n:]
	leaderAddr, _, err := decodeString(b)
	if err != nil {
		return ClusterStatusResponse{}, err
	}
	return ClusterStatusResponse{
		NodeID: nodeID, IsLeader: isLeader,
		LeaderID: leaderID, LeaderAddr: leaderAddr,
	}, nil
}

// ListGroupsRequest enumerates every consumer group the manager
// knows about. Request body is empty.
type ListGroupsRequest struct{}

func (r ListGroupsRequest) Encode() []byte { return nil }

func DecodeListGroupsRequest(_ []byte) (ListGroupsRequest, error) {
	return ListGroupsRequest{}, nil
}

// GroupSummary is one entry in a ListGroupsResponse.
type GroupSummary struct {
	Name        string
	Generation  int32
	MemberCount int32
	Topics      []string
}

// ListGroupsResponse carries a summary of every consumer group.
type ListGroupsResponse struct {
	Groups []GroupSummary
}

func (r ListGroupsResponse) Encode() []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(r.Groups)))
	for _, g := range r.Groups {
		b = encodeString(b, g.Name)
		b = binary.BigEndian.AppendUint32(b, uint32(g.Generation))
		b = binary.BigEndian.AppendUint32(b, uint32(g.MemberCount))
		b = binary.BigEndian.AppendUint32(b, uint32(len(g.Topics)))
		for _, t := range g.Topics {
			b = encodeString(b, t)
		}
	}
	return b
}

func DecodeListGroupsResponse(b []byte) (ListGroupsResponse, error) {
	if len(b) < 4 {
		return ListGroupsResponse{}, errors.New("proto: short ListGroupsResponse")
	}
	count := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	out := make([]GroupSummary, 0, count)
	for range count {
		name, n, err := decodeString(b)
		if err != nil {
			return ListGroupsResponse{}, err
		}
		b = b[n:]
		if len(b) < 12 {
			return ListGroupsResponse{}, errors.New("proto: short ListGroupsResponse entry")
		}
		gen := int32(binary.BigEndian.Uint32(b[0:4]))
		members := int32(binary.BigEndian.Uint32(b[4:8]))
		topicCount := int(binary.BigEndian.Uint32(b[8:12]))
		b = b[12:]
		topics := make([]string, 0, topicCount)
		for range topicCount {
			t, n2, err := decodeString(b)
			if err != nil {
				return ListGroupsResponse{}, err
			}
			topics = append(topics, t)
			b = b[n2:]
		}
		out = append(out, GroupSummary{
			Name: name, Generation: gen, MemberCount: members, Topics: topics,
		})
	}
	return ListGroupsResponse{Groups: out}, nil
}

// DescribeGroupRequest names the group to describe.
type DescribeGroupRequest struct {
	Group string
}

func (r DescribeGroupRequest) Encode() []byte {
	return encodeString(nil, r.Group)
}

func DecodeDescribeGroupRequest(b []byte) (DescribeGroupRequest, error) {
	g, _, err := decodeString(b)
	if err != nil {
		return DescribeGroupRequest{}, err
	}
	return DescribeGroupRequest{Group: g}, nil
}

// MemberAssignment is one member's partition list in a group.
type MemberAssignment struct {
	MemberID   string
	Partitions []PartitionRef
}

// DescribeGroupResponse carries per-member assignments for one
// group plus the group's current generation and subscribed topics.
type DescribeGroupResponse struct {
	Name       string
	Generation int32
	Topics     []string
	Members    []MemberAssignment
}

func (r DescribeGroupResponse) Encode() []byte {
	b := encodeString(nil, r.Name)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Generation))
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Topics)))
	for _, t := range r.Topics {
		b = encodeString(b, t)
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Members)))
	for _, m := range r.Members {
		b = encodeString(b, m.MemberID)
		b = binary.BigEndian.AppendUint32(b, uint32(len(m.Partitions)))
		for _, p := range m.Partitions {
			b = encodeString(b, p.Topic)
			b = binary.BigEndian.AppendUint32(b, uint32(p.Index))
		}
	}
	return b
}

func DecodeDescribeGroupResponse(b []byte) (DescribeGroupResponse, error) {
	name, n, err := decodeString(b)
	if err != nil {
		return DescribeGroupResponse{}, err
	}
	b = b[n:]
	if len(b) < 8 {
		return DescribeGroupResponse{}, errors.New("proto: short DescribeGroupResponse")
	}
	gen := int32(binary.BigEndian.Uint32(b[0:4]))
	topicCount := int(binary.BigEndian.Uint32(b[4:8]))
	b = b[8:]
	topics := make([]string, 0, topicCount)
	for range topicCount {
		t, n2, err := decodeString(b)
		if err != nil {
			return DescribeGroupResponse{}, err
		}
		topics = append(topics, t)
		b = b[n2:]
	}
	if len(b) < 4 {
		return DescribeGroupResponse{}, errors.New("proto: short DescribeGroupResponse members")
	}
	memberCount := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	members := make([]MemberAssignment, 0, memberCount)
	for range memberCount {
		id, n2, err := decodeString(b)
		if err != nil {
			return DescribeGroupResponse{}, err
		}
		b = b[n2:]
		if len(b) < 4 {
			return DescribeGroupResponse{}, errors.New("proto: short DescribeGroupResponse partition count")
		}
		pcount := int(binary.BigEndian.Uint32(b[:4]))
		b = b[4:]
		parts := make([]PartitionRef, 0, pcount)
		for range pcount {
			pt, n3, err := decodeString(b)
			if err != nil {
				return DescribeGroupResponse{}, err
			}
			b = b[n3:]
			if len(b) < 4 {
				return DescribeGroupResponse{}, errors.New("proto: short DescribeGroupResponse partition")
			}
			parts = append(parts, PartitionRef{
				Topic: pt,
				Index: int32(binary.BigEndian.Uint32(b[:4])),
			})
			b = b[4:]
		}
		members = append(members, MemberAssignment{MemberID: id, Partitions: parts})
	}
	return DescribeGroupResponse{
		Name: name, Generation: gen, Topics: topics, Members: members,
	}, nil
}

// ListTopicsRequest enumerates every topic the broker knows about.
// The request body is empty.
type ListTopicsRequest struct{}

func (r ListTopicsRequest) Encode() []byte { return nil }

func DecodeListTopicsRequest(_ []byte) (ListTopicsRequest, error) {
	return ListTopicsRequest{}, nil
}

// ListTopicsResponse carries every TopicConfig in the broker's
// registry. Order is unspecified.
type ListTopicsResponse struct {
	Topics []TopicConfig
}

func (r ListTopicsResponse) Encode() []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(r.Topics)))
	for _, t := range r.Topics {
		b = encodeString(b, t.Name)
		b = binary.BigEndian.AppendUint32(b, uint32(t.PartitionCount))
		b = binary.BigEndian.AppendUint64(b, uint64(t.RetentionMs))
		b = binary.BigEndian.AppendUint64(b, uint64(t.SegmentBytes))
	}
	return b
}

func DecodeListTopicsResponse(b []byte) (ListTopicsResponse, error) {
	if len(b) < 4 {
		return ListTopicsResponse{}, errors.New("proto: short ListTopicsResponse")
	}
	count := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	out := make([]TopicConfig, 0, count)
	for range count {
		name, n, err := decodeString(b)
		if err != nil {
			return ListTopicsResponse{}, err
		}
		b = b[n:]
		if len(b) < 20 {
			return ListTopicsResponse{}, errors.New("proto: short ListTopicsResponse entry")
		}
		out = append(out, TopicConfig{
			Name:           name,
			PartitionCount: int32(binary.BigEndian.Uint32(b[0:4])),
			RetentionMs:    int64(binary.BigEndian.Uint64(b[4:12])),
			SegmentBytes:   int64(binary.BigEndian.Uint64(b[12:20])),
		})
		b = b[20:]
	}
	return ListTopicsResponse{Topics: out}, nil
}

// UpdateTopicConfigRequest changes a topic's retention and
// segment-size settings without recreating it. PartitionCount is
// not modifiable — changing it would break per-partition ordering
// invariants for already-produced records.
type UpdateTopicConfigRequest struct {
	Name         string
	RetentionMs  int64
	SegmentBytes int64
}

func (r UpdateTopicConfigRequest) Encode() []byte {
	b := encodeString(nil, r.Name)
	b = binary.BigEndian.AppendUint64(b, uint64(r.RetentionMs))
	return binary.BigEndian.AppendUint64(b, uint64(r.SegmentBytes))
}

func DecodeUpdateTopicConfigRequest(b []byte) (UpdateTopicConfigRequest, error) {
	name, n, err := decodeString(b)
	if err != nil {
		return UpdateTopicConfigRequest{}, err
	}
	b = b[n:]
	if len(b) < 16 {
		return UpdateTopicConfigRequest{}, errors.New("proto: short UpdateTopicConfigRequest")
	}
	return UpdateTopicConfigRequest{
		Name:         name,
		RetentionMs:  int64(binary.BigEndian.Uint64(b[0:8])),
		SegmentBytes: int64(binary.BigEndian.Uint64(b[8:16])),
	}, nil
}

// DeleteTopicRequest removes a topic and every record on it. The
// response body is empty (status-only).
type DeleteTopicRequest struct {
	Name string
}

func (r DeleteTopicRequest) Encode() []byte {
	return encodeString(nil, r.Name)
}

func DecodeDeleteTopicRequest(b []byte) (DeleteTopicRequest, error) {
	name, _, err := decodeString(b)
	if err != nil {
		return DeleteTopicRequest{}, err
	}
	return DeleteTopicRequest{Name: name}, nil
}

// CommitRequest is a no-op through Stage 3.
type CommitRequest struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
}

func (r CommitRequest) Encode() []byte {
	b := encodeString(nil, r.Group)
	b = encodeString(b, r.Topic)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Partition))
	b = binary.BigEndian.AppendUint64(b, uint64(r.Offset))
	return b
}

func DecodeCommitRequest(b []byte) (CommitRequest, error) {
	group, n, err := decodeString(b)
	if err != nil {
		return CommitRequest{}, err
	}
	b = b[n:]
	topic, n, err := decodeString(b)
	if err != nil {
		return CommitRequest{}, err
	}
	b = b[n:]
	if len(b) < 12 {
		return CommitRequest{}, errors.New("proto: short CommitRequest")
	}
	return CommitRequest{
		Group:     group,
		Topic:     topic,
		Partition: int32(binary.BigEndian.Uint32(b[0:4])),
		Offset:    int64(binary.BigEndian.Uint64(b[4:12])),
	}, nil
}

// JoinGroupRequest signs the caller into a consumer group. An empty
// MemberID asks the broker to assign one.
type JoinGroupRequest struct {
	Group    string
	MemberID string
	Topics   []string
}

func (r JoinGroupRequest) Encode() []byte {
	b := encodeString(nil, r.Group)
	b = encodeString(b, r.MemberID)
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Topics)))
	for _, t := range r.Topics {
		b = encodeString(b, t)
	}
	return b
}

func DecodeJoinGroupRequest(b []byte) (JoinGroupRequest, error) {
	group, n, err := decodeString(b)
	if err != nil {
		return JoinGroupRequest{}, err
	}
	b = b[n:]
	member, n, err := decodeString(b)
	if err != nil {
		return JoinGroupRequest{}, err
	}
	b = b[n:]
	if len(b) < 4 {
		return JoinGroupRequest{}, errors.New("proto: short JoinGroupRequest")
	}
	count := binary.BigEndian.Uint32(b[:4])
	b = b[4:]
	topics := make([]string, 0, count)
	for range count {
		t, n, err := decodeString(b)
		if err != nil {
			return JoinGroupRequest{}, err
		}
		topics = append(topics, t)
		b = b[n:]
	}
	return JoinGroupRequest{Group: group, MemberID: member, Topics: topics}, nil
}

// AssignmentEntry pairs a partition with its committed offset (or -1).
type AssignmentEntry struct {
	Topic           string
	Partition       int32
	CommittedOffset int64
}

// JoinGroupResponse carries the broker-assigned member ID, the current
// generation, and the partitions this member should consume.
type JoinGroupResponse struct {
	MemberID    string
	Generation  int32
	Assignments []AssignmentEntry
}

func (r JoinGroupResponse) Encode() []byte {
	b := encodeString(nil, r.MemberID)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Generation))
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Assignments)))
	for _, a := range r.Assignments {
		b = encodeString(b, a.Topic)
		b = binary.BigEndian.AppendUint32(b, uint32(a.Partition))
		b = binary.BigEndian.AppendUint64(b, uint64(a.CommittedOffset))
	}
	return b
}

func DecodeJoinGroupResponse(b []byte) (JoinGroupResponse, error) {
	member, n, err := decodeString(b)
	if err != nil {
		return JoinGroupResponse{}, err
	}
	b = b[n:]
	if len(b) < 8 {
		return JoinGroupResponse{}, errors.New("proto: short JoinGroupResponse")
	}
	generation := int32(binary.BigEndian.Uint32(b[0:4]))
	count := binary.BigEndian.Uint32(b[4:8])
	b = b[8:]
	assignments := make([]AssignmentEntry, 0, count)
	for range count {
		topic, m, err := decodeString(b)
		if err != nil {
			return JoinGroupResponse{}, err
		}
		b = b[m:]
		if len(b) < 12 {
			return JoinGroupResponse{}, errors.New("proto: short assignment entry")
		}
		assignments = append(assignments, AssignmentEntry{
			Topic:           topic,
			Partition:       int32(binary.BigEndian.Uint32(b[0:4])),
			CommittedOffset: int64(binary.BigEndian.Uint64(b[4:12])),
		})
		b = b[12:]
	}
	return JoinGroupResponse{MemberID: member, Generation: generation, Assignments: assignments}, nil
}

// HeartbeatRequest reports liveness for memberID in groupName.
//
// MaxWaitMs > 0 turns the call into a long-poll: the broker holds
// the response open for up to MaxWaitMs milliseconds, returning
// immediately when a rebalance is needed. MaxWaitMs == 0 preserves
// the historical fire-and-forget semantic where the response is
// returned on receipt.
type HeartbeatRequest struct {
	Group      string
	MemberID   string
	Generation int32
	MaxWaitMs  int32
}

func (r HeartbeatRequest) Encode() []byte {
	b := encodeString(nil, r.Group)
	b = encodeString(b, r.MemberID)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Generation))
	b = binary.BigEndian.AppendUint32(b, uint32(r.MaxWaitMs))
	return b
}

func DecodeHeartbeatRequest(b []byte) (HeartbeatRequest, error) {
	group, n, err := decodeString(b)
	if err != nil {
		return HeartbeatRequest{}, err
	}
	b = b[n:]
	member, n, err := decodeString(b)
	if err != nil {
		return HeartbeatRequest{}, err
	}
	b = b[n:]
	if len(b) < 8 {
		return HeartbeatRequest{}, errors.New("proto: short HeartbeatRequest")
	}
	return HeartbeatRequest{
		Group:      group,
		MemberID:   member,
		Generation: int32(binary.BigEndian.Uint32(b[:4])),
		MaxWaitMs:  int32(binary.BigEndian.Uint32(b[4:8])),
	}, nil
}

// HeartbeatResponse signals whether the caller should rejoin.
type HeartbeatResponse struct {
	RebalanceNeeded bool
}

func (r HeartbeatResponse) Encode() []byte {
	if r.RebalanceNeeded {
		return []byte{1}
	}
	return []byte{0}
}

func DecodeHeartbeatResponse(b []byte) (HeartbeatResponse, error) {
	if len(b) < 1 {
		return HeartbeatResponse{}, errors.New("proto: short HeartbeatResponse")
	}
	return HeartbeatResponse{RebalanceNeeded: b[0] != 0}, nil
}

// SyncRequest asks the broker to fsync the addressed partition.
// Response body is empty (status-only).
type SyncRequest struct {
	Topic     string
	Partition int32
}

func (r SyncRequest) Encode() []byte {
	b := encodeString(nil, r.Topic)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Partition))
	return b
}

func DecodeSyncRequest(b []byte) (SyncRequest, error) {
	topic, n, err := decodeString(b)
	if err != nil {
		return SyncRequest{}, err
	}
	b = b[n:]
	if len(b) < 4 {
		return SyncRequest{}, errors.New("proto: short SyncRequest")
	}
	return SyncRequest{
		Topic:     topic,
		Partition: int32(binary.BigEndian.Uint32(b[:4])),
	}, nil
}

// DeleteGroupRequest names the group to delete. The response body
// is empty (status-only).
type DeleteGroupRequest struct {
	Group string
}

func (r DeleteGroupRequest) Encode() []byte {
	return encodeString(nil, r.Group)
}

func DecodeDeleteGroupRequest(b []byte) (DeleteGroupRequest, error) {
	g, _, err := decodeString(b)
	if err != nil {
		return DeleteGroupRequest{}, err
	}
	return DeleteGroupRequest{Group: g}, nil
}

// ListGroupOffsetsRequest names the group whose committed offsets
// should be enumerated.
type ListGroupOffsetsRequest struct {
	Group string
}

func (r ListGroupOffsetsRequest) Encode() []byte {
	return encodeString(nil, r.Group)
}

func DecodeListGroupOffsetsRequest(b []byte) (ListGroupOffsetsRequest, error) {
	g, _, err := decodeString(b)
	if err != nil {
		return ListGroupOffsetsRequest{}, err
	}
	return ListGroupOffsetsRequest{Group: g}, nil
}

// GroupOffsetEntry pairs a (topic, partition) with the group's
// committed offset and that partition's current high-water. The
// operator-side caller derives lag = HighWater - Committed; if
// Committed is NoOffset the partition has been assigned but never
// committed — typical for a fresh group that hasn't read anything yet.
type GroupOffsetEntry struct {
	Topic     string
	Partition int32
	Committed int64
	HighWater int64
}

// ListGroupOffsetsResponse carries one entry per (topic, partition)
// the group has touched. Order is deterministic (sorted by topic, then
// partition) so CLI output is stable.
type ListGroupOffsetsResponse struct {
	Entries []GroupOffsetEntry
}

func (r ListGroupOffsetsResponse) Encode() []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(r.Entries)))
	for _, e := range r.Entries {
		b = encodeString(b, e.Topic)
		b = binary.BigEndian.AppendUint32(b, uint32(e.Partition))
		b = binary.BigEndian.AppendUint64(b, uint64(e.Committed))
		b = binary.BigEndian.AppendUint64(b, uint64(e.HighWater))
	}
	return b
}

func DecodeListGroupOffsetsResponse(b []byte) (ListGroupOffsetsResponse, error) {
	if len(b) < 4 {
		return ListGroupOffsetsResponse{}, errors.New("proto: short ListGroupOffsetsResponse")
	}
	count := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	out := make([]GroupOffsetEntry, 0, count)
	for range count {
		topic, n, err := decodeString(b)
		if err != nil {
			return ListGroupOffsetsResponse{}, err
		}
		b = b[n:]
		if len(b) < 20 {
			return ListGroupOffsetsResponse{}, errors.New("proto: short ListGroupOffsetsResponse entry")
		}
		out = append(out, GroupOffsetEntry{
			Topic:     topic,
			Partition: int32(binary.BigEndian.Uint32(b[0:4])),
			Committed: int64(binary.BigEndian.Uint64(b[4:12])),
			HighWater: int64(binary.BigEndian.Uint64(b[12:20])),
		})
		b = b[20:]
	}
	return ListGroupOffsetsResponse{Entries: out}, nil
}

// LeaveGroupRequest deregisters a member from a group.
type LeaveGroupRequest struct {
	Group    string
	MemberID string
}

func (r LeaveGroupRequest) Encode() []byte {
	b := encodeString(nil, r.Group)
	b = encodeString(b, r.MemberID)
	return b
}

func DecodeLeaveGroupRequest(b []byte) (LeaveGroupRequest, error) {
	group, n, err := decodeString(b)
	if err != nil {
		return LeaveGroupRequest{}, err
	}
	b = b[n:]
	member, _, err := decodeString(b)
	if err != nil {
		return LeaveGroupRequest{}, err
	}
	return LeaveGroupRequest{Group: group, MemberID: member}, nil
}

// ===== Internal helpers =====

const maxFrameBytes = 64 * 1024 * 1024 // 64 MiB; bumps with config later.

// encodeString length-prefixes (uint32 BE) a UTF-8 string.
func encodeString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

func decodeString(b []byte) (string, int, error) {
	if len(b) < 4 {
		return "", 0, io.ErrUnexpectedEOF
	}
	n := binary.BigEndian.Uint32(b[:4])
	if uint32(len(b)-4) < n {
		return "", 0, io.ErrUnexpectedEOF
	}
	return string(b[4 : 4+n]), 4 + int(n), nil
}

// encodeRecord and decodeRecord use the same wire layout the disk log
// uses for record bodies, minus the disk frame's length prefix and CRC
// trailer (the wire frame already provides those).
func encodeRecord(buf []byte, r Record) []byte {
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Offset))
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Timestamp))
	buf = encodeBytes(buf, r.Key)
	buf = encodeBytes(buf, r.Value)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Headers)))
	for _, h := range r.Headers {
		buf = encodeString(buf, h.Key)
		buf = encodeBytes(buf, h.Value)
	}
	return buf
}

func decodeRecord(b []byte) (Record, int, error) {
	if len(b) < 16 {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	p := 0
	offset := int64(binary.BigEndian.Uint64(b[p : p+8]))
	p += 8
	ts := int64(binary.BigEndian.Uint64(b[p : p+8]))
	p += 8

	key, n, err := decodeBytes(b[p:])
	if err != nil {
		return Record{}, 0, err
	}
	p += n
	value, n, err := decodeBytes(b[p:])
	if err != nil {
		return Record{}, 0, err
	}
	p += n

	if len(b[p:]) < 4 {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	hdrCount := binary.BigEndian.Uint32(b[p : p+4])
	p += 4

	var headers []Header
	if hdrCount > 0 {
		headers = make([]Header, 0, hdrCount)
	}
	for range hdrCount {
		hk, n, err := decodeString(b[p:])
		if err != nil {
			return Record{}, 0, err
		}
		p += n
		hv, n, err := decodeBytes(b[p:])
		if err != nil {
			return Record{}, 0, err
		}
		p += n
		headers = append(headers, Header{Key: hk, Value: hv})
	}

	return Record{
		Offset:    offset,
		Timestamp: ts,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, p, nil
}

func encodeBytes(buf, b []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}

func decodeBytes(b []byte) ([]byte, int, error) {
	if len(b) < 4 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	n := binary.BigEndian.Uint32(b[:4])
	if uint32(len(b)-4) < n {
		return nil, 0, io.ErrUnexpectedEOF
	}
	if n == 0 {
		return nil, 4, nil
	}
	out := make([]byte, n)
	copy(out, b[4:4+n])
	return out, 4 + int(n), nil
}
