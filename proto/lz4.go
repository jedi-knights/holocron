package proto

import (
	"errors"

	"github.com/pierrec/lz4/v4"
)

// lz4Compress compresses src using LZ4's block format. The destination
// is allocated to LZ4's worst-case bound; only the first n bytes of
// the returned slice are valid (CompressBlock reports the count).
func lz4Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	dst := make([]byte, lz4.CompressBlockBound(len(src)))
	var c lz4.Compressor
	n, err := c.CompressBlock(src, dst)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		// LZ4 reports n=0 when the data is incompressible — payload
		// would grow under compression. Caller should fall back to
		// CodecNone in that case.
		return nil, errors.New("proto: lz4: payload incompressible")
	}
	return dst[:n], nil
}

// lz4Decompress inflates src to a buffer of size uncompressedLen.
func lz4Decompress(src []byte, uncompressedLen int) ([]byte, error) {
	if uncompressedLen == 0 {
		return nil, nil
	}
	dst := make([]byte, uncompressedLen)
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return nil, err
	}
	if n != uncompressedLen {
		return nil, errors.New("proto: lz4: short decompress")
	}
	return dst, nil
}
