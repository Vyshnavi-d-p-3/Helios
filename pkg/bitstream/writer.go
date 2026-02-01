package bitstream

// Writer appends a stream of bits to a growable buffer. Bits are written
// MSB-first within each byte; the first bit of the stream is the MSB of buf[0].
type Writer struct {
	data []byte
	nb   uint // bits written
}

// NewWriter preallocates a buffer of cap bytes.
func NewWriter(cap int) *Writer {
	if cap < 0 {
		cap = 0
	}
	return &Writer{data: make([]byte, 0, cap)}
}

// WriteBit appends a single bit (true = 1).
func (w *Writer) WriteBit(b bool) {
	if w.nb/8 == uint(len(w.data)) {
		w.data = append(w.data, 0)
	}
	if b {
		byteIdx := w.nb / 8
		bitInByte := w.nb % 8
		w.data[byteIdx] |= 1 << (7 - bitInByte)
	}
	w.nb++
}

// WriteBits appends the low n bits of v, with the MSB of the field first (bit n-1 down to 0).
func (w *Writer) WriteBits(v uint64, n int) {
	if n <= 0 {
		return
	}
	if n > 64 {
		n = 64
	}
	for i := n - 1; i >= 0; i-- {
		w.WriteBit((v>>uint(i))&1 == 1)
	}
}

// Len is the number of bits written.
func (w *Writer) Len() uint64 { return uint64(w.nb) }

// Bytes returns the packed byte buffer (ceiling to whole bytes, zero-padded in the last byte).
func (w *Writer) Bytes() []byte {
	out := make([]byte, len(w.data))
	copy(out, w.data)
	return out
}
