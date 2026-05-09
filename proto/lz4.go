package proto

import (
	"errors"

	"github.com/pierrec/lz4/v4"
)

// lz4Compress compresses src using LZ4's block format. The destination
// is allocated to LZ4's worst-case bound; only the first n bytes of
// the returned slice are valid (CompressBlock reports the count).
func lz4Compress(src []byte) ([]byte, error) {
	return lz4CompressLevel(src, 0)
}

// lz4CompressLevel compresses src at the given level. Level 0
// (or any value below 1) uses the fast LZ4 block compressor —
// matching the original lz4Compress. Levels 1..9 use the LZ4-HC
// (high-compression) variant at that level — better ratio at
// higher CPU cost. The wire format is identical for both
// variants; the decoder uses lz4Decompress unchanged.
func lz4CompressLevel(src []byte, level int) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	dst := make([]byte, lz4.CompressBlockBound(len(src)))
	var (
		n   int
		err error
	)
	if level <= 0 {
		var c lz4.Compressor
		n, err = c.CompressBlock(src, dst)
	} else {
		hc := lz4.CompressorHC{Level: hcLevel(level)}
		n, err = hc.CompressBlock(src, dst)
	}
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

// hcLevel maps the SDK's 1..9 level to the lz4 library's
// CompressionLevel enum. The library's levels are sparse named
// constants (Level1..Level9) so a direct numeric cast doesn't
// work — we walk the enum.
func hcLevel(n int) lz4.CompressionLevel {
	switch {
	case n <= 1:
		return lz4.Level1
	case n == 2:
		return lz4.Level2
	case n == 3:
		return lz4.Level3
	case n == 4:
		return lz4.Level4
	case n == 5:
		return lz4.Level5
	case n == 6:
		return lz4.Level6
	case n == 7:
		return lz4.Level7
	case n == 8:
		return lz4.Level8
	default:
		return lz4.Level9
	}
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
