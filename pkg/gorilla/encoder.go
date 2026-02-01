// Package gorilla implements the Gorilla float compression algorithm from
// Facebook's 2015 paper "Gorilla: A Fast, Scalable, In-Memory Time Series Database."
//
// Encoding overview:
//   - First (timestamp, value) pair is stored verbatim (64+64 bits).
//   - Subsequent timestamps use delta-of-delta encoding with variable-width brackets.
//   - Subsequent values are XORed with the previous value; the leading/trailing
//     zero structure of the XOR result determines the encoding case.
//
// Two subtleties from the paper that v1 of this code got wrong and v2 fixes:
//
//  1. The leading-zeros count is encoded in 5 bits, but LeadingZeros64 can return
//     0–63 for a non-zero XOR. Values ≥32 must be clamped to 31, otherwise the
//     low 5 bits are silently truncated and the decoder reconstructs garbage.
//
//  2. The meaningful-bits count covers the range [1, 64] but is encoded in 6 bits
//     (range [0, 63]). The Gorilla paper's convention is to map 64 → 0 explicitly.
//     v1 relied on integer truncation (writing 64 in 6 bits truncates to 0); the
//     decoder happened to invert this because of its sentinel handling, but the
//     pairing was undocumented and brittle.
package gorilla

import (
	"math"
	"math/bits"

	"github.com/vyshnavi-d-p-3/helios/pkg/bitstream"
)

// Encoder compresses a stream of (timestamp, value) pairs.
//
// Encoders are not safe for concurrent use. The intended pattern is one encoder
// per series per SSTable write — the encoder is constructed, fed, and finished
// from a single goroutine.
type Encoder struct {
	bw *bitstream.Writer

	// Timestamp delta-of-delta state.
	prevTime      int64
	prevTimeDelta int64
	timeFirst     bool

	// Value XOR state. prevLeadingZeros is initialised to a sentinel that
	// forces the first non-zero XOR to use the new-window encoding.
	prevValue         uint64
	prevLeadingZeros  int
	prevTrailingZeros int
	valueFirst        bool

	count int
}

// NewEncoder returns an empty encoder.
func NewEncoder() *Encoder {
	return &Encoder{
		bw:                bitstream.NewWriter(256),
		timeFirst:         true,
		valueFirst:        true,
		prevLeadingZeros:  -1,
		prevTrailingZeros: -1,
	}
}

// Encode appends a (timestamp, value) pair to the compressed stream.
//
// Timestamps must be monotonically non-decreasing — the encoder does not
// validate this, and out-of-order timestamps produce a stream that decodes
// to wrong values without raising any error.
func (e *Encoder) Encode(timestamp int64, value float64) {
	e.encodeTimestamp(timestamp)
	e.encodeValue(value)
	e.count++
}

func (e *Encoder) encodeTimestamp(t int64) {
	if e.timeFirst {
		e.bw.WriteBits(uint64(t), 64)
		e.prevTime = t
		e.timeFirst = false
		return
	}

	delta := t - e.prevTime
	dod := delta - e.prevTimeDelta

	switch {
	case dod == 0:
		e.bw.WriteBit(false)
	case dod >= -63 && dod <= 64:
		e.bw.WriteBits(0b10, 2)
		e.bw.WriteBits(uint64(dod+63), 7)
	case dod >= -255 && dod <= 256:
		e.bw.WriteBits(0b110, 3)
		e.bw.WriteBits(uint64(dod+255), 9)
	case dod >= -2047 && dod <= 2048:
		e.bw.WriteBits(0b1110, 4)
		e.bw.WriteBits(uint64(dod+2047), 12)
	default:
		e.bw.WriteBits(0b1111, 4)
		e.bw.WriteBits(uint64(dod), 32)
	}

	e.prevTimeDelta = delta
	e.prevTime = t
}

func (e *Encoder) encodeValue(v float64) {
	vBits := math.Float64bits(v)

	if e.valueFirst {
		e.bw.WriteBits(vBits, 64)
		e.prevValue = vBits
		e.valueFirst = false
		return
	}

	xor := vBits ^ e.prevValue
	if xor == 0 {
		e.bw.WriteBit(false)
		return
	}

	e.bw.WriteBit(true)

	leading := bits.LeadingZeros64(xor)
	trailing := bits.TrailingZeros64(xor)

	// FIX (v2): clamp leading zeros to fit in 5 bits.
	// Without this clamp, values with ≥32 leading zeros lose the high bit
	// of the leading-zeros count and the decoder reconstructs garbage.
	// This is the most common source of "Gorilla works on my benchmark
	// but fails on real data" bugs.
	if leading >= 32 {
		leading = 31
	}

	// Reuse the previous window if this XOR's meaningful region fits inside it.
	// We re-derive the meaningful region for the *previous* window so we can
	// compare apples to apples; note that prevLeadingZeros has already been
	// clamped on the path that wrote it.
	if e.prevLeadingZeros >= 0 &&
		leading >= e.prevLeadingZeros &&
		trailing >= e.prevTrailingZeros {
		e.bw.WriteBit(false)
		meaningful := 64 - e.prevLeadingZeros - e.prevTrailingZeros
		e.bw.WriteBits(xor>>uint(e.prevTrailingZeros), meaningful)
		e.prevValue = vBits
		return
	}

	// New window: encode (leading_zeros, meaningful_length, meaningful_bits).
	e.bw.WriteBit(true)
	meaningful := 64 - leading - trailing

	// FIX (v2): explicit 64 → 0 mapping for the 6-bit meaningful field.
	// The valid range is [1, 64]; we encode 64 as 0 by paper convention.
	// v1 relied on WriteBits truncation to make this work, which was correct
	// by accident and confusing on inspection.
	encodedMeaningful := meaningful
	if encodedMeaningful == 64 {
		encodedMeaningful = 0
	}

	e.bw.WriteBits(uint64(leading), 5)
	e.bw.WriteBits(uint64(encodedMeaningful), 6)
	e.bw.WriteBits(xor>>uint(trailing), meaningful)

	e.prevLeadingZeros = leading
	e.prevTrailingZeros = trailing
	e.prevValue = vBits
}

// Finish flushes pending bits and returns the compressed byte slice.
// The encoder must not be used after Finish.
func (e *Encoder) Finish() []byte {
	return e.bw.Bytes()
}

// Count is the number of samples encoded.
func (e *Encoder) Count() int { return e.count }

// BitsWritten is the total bit length of the compressed output.
// Useful for compression-ratio reporting in benchmarks.
func (e *Encoder) BitsWritten() uint64 { return e.bw.Len() }
