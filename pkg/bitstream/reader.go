package bitstream

import (
	"fmt"
	"io"
)

// Reader reads a bit stream produced by Writer (same MSB-first byte layout).
type Reader struct {
	data []byte
	nb   uint // next bit index to read
}

// NewReader returns a reader over a full copy of the compressed buffer.
func NewReader(data []byte) *Reader {
	buf := make([]byte, len(data))
	copy(buf, data)
	return &Reader{data: buf}
}

// ReadBit returns the next bit, or an error if the stream is exhausted.
func (r *Reader) ReadBit() (bool, error) {
	if r.nb >= uint(len(r.data))*8 {
		return false, io.EOF
	}
	byteIdx := r.nb / 8
	bitInByte := r.nb % 8
	r.nb++
	return (r.data[byteIdx]>>(7-bitInByte))&1 == 1, nil
}

// ReadBits reads n bits as the low n bits of a uint64 (MSB of field first).
func (r *Reader) ReadBits(n int) (uint64, error) {
	if n <= 0 {
		return 0, nil
	}
	if n > 64 {
		return 0, fmt.Errorf("readBits: n=%d > 64", n)
	}
	var v uint64
	for i := 0; i < n; i++ {
		b, err := r.ReadBit()
		if err != nil {
			return 0, err
		}
		v <<= 1
		if b {
			v |= 1
		}
	}
	return v, nil
}

// BitsRead returns the number of bits consumed.
func (r *Reader) BitsRead() uint64 { return uint64(r.nb) }
