package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/jedi-knights/holocron/proto"
)

// crcTable uses Castagnoli (CRC32C) — the standard storage choice; cheap on
// modern CPUs that have an SSE4.2 instruction for it.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// FrameOverhead is the byte cost per record beyond the body itself: 4-byte
// length prefix + 4-byte trailing CRC.
const FrameOverhead = 8

// errTornFrame signals a record that did not pass CRC validation. Recovery
// treats this as a torn write and truncates back to the last good record.
var errTornFrame = errors.New("log: torn frame (CRC mismatch)")

// encodeRecord appends a framed record to buf and returns the new buffer.
//
//	[4 bytes: body length, BE uint32]
//	[body]
//	[4 bytes: CRC32C of body, BE uint32]
//
// Body layout:
//
//	[8] offset                     int64 BE
//	[8] timestamp                  int64 BE
//	[4] keyLen | bytes             int32 BE + bytes
//	[4] valueLen | bytes           int32 BE + bytes
//	[4] header count               uint32 BE
//	per header:
//	  [4] keyLen | bytes (UTF-8)   int32 BE + bytes
//	  [4] valueLen | bytes         int32 BE + bytes
func encodeRecord(buf []byte, r proto.Record) []byte {
	bodyStart := len(buf) + 4
	buf = append(buf, 0, 0, 0, 0)

	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Offset))
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Timestamp))
	buf = appendBytes(buf, r.Key)
	buf = appendBytes(buf, r.Value)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Headers)))
	for _, h := range r.Headers {
		buf = appendBytes(buf, []byte(h.Key))
		buf = appendBytes(buf, h.Value)
	}

	body := buf[bodyStart:]
	binary.BigEndian.PutUint32(buf[bodyStart-4:bodyStart], uint32(len(body)))

	crc := crc32.Checksum(body, crcTable)
	buf = binary.BigEndian.AppendUint32(buf, crc)
	return buf
}

func appendBytes(buf, b []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}

// readRecordFrom reads one framed record from r. It returns the parsed
// record and the number of bytes consumed from r (including the length
// prefix and trailing CRC). io.EOF is returned only when the reader is at
// a clean frame boundary; a partial frame returns io.ErrUnexpectedEOF.
func readRecordFrom(r io.Reader) (proto.Record, int, error) {
	var lenBuf [4]byte
	n, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return proto.Record{}, 0, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return proto.Record{}, n, io.ErrUnexpectedEOF
		}
		return proto.Record{}, n, err
	}
	bodyLen := binary.BigEndian.Uint32(lenBuf[:])

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			return proto.Record{}, 4, io.ErrUnexpectedEOF
		}
		return proto.Record{}, 4, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return proto.Record{}, 4 + int(bodyLen), io.ErrUnexpectedEOF
		}
		return proto.Record{}, 4 + int(bodyLen), err
	}
	expected := binary.BigEndian.Uint32(crcBuf[:])
	if got := crc32.Checksum(body, crcTable); got != expected {
		return proto.Record{}, 4 + int(bodyLen) + 4, errTornFrame
	}

	rec, err := decodeBody(body)
	if err != nil {
		return proto.Record{}, 4 + int(bodyLen) + 4, err
	}
	return rec, 4 + int(bodyLen) + 4, nil
}

func decodeBody(body []byte) (proto.Record, error) {
	if len(body) < 16 {
		return proto.Record{}, fmt.Errorf("log: short body (%d bytes)", len(body))
	}
	p := 0
	offset := int64(binary.BigEndian.Uint64(body[p : p+8]))
	p += 8
	ts := int64(binary.BigEndian.Uint64(body[p : p+8]))
	p += 8

	key, n, err := readBytes(body[p:])
	if err != nil {
		return proto.Record{}, fmt.Errorf("log: key: %w", err)
	}
	p += n

	value, n, err := readBytes(body[p:])
	if err != nil {
		return proto.Record{}, fmt.Errorf("log: value: %w", err)
	}
	p += n

	if len(body[p:]) < 4 {
		return proto.Record{}, fmt.Errorf("log: short header count")
	}
	hdrCount := binary.BigEndian.Uint32(body[p : p+4])
	p += 4

	var headers []proto.Header
	if hdrCount > 0 {
		headers = make([]proto.Header, 0, hdrCount)
	}
	for i := range hdrCount {
		hk, n, err := readBytes(body[p:])
		if err != nil {
			return proto.Record{}, fmt.Errorf("log: header %d key: %w", i, err)
		}
		p += n
		hv, n, err := readBytes(body[p:])
		if err != nil {
			return proto.Record{}, fmt.Errorf("log: header %d value: %w", i, err)
		}
		p += n
		headers = append(headers, proto.Header{Key: string(hk), Value: hv})
	}

	return proto.Record{
		Offset:    offset,
		Timestamp: ts,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, nil
}

func readBytes(b []byte) ([]byte, int, error) {
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
