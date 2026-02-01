package gorilla

import (
	"fmt"
	"io"
	"math"

	"github.com/vyshnavi-d-p-3/helios/pkg/bitstream"
)

// Decoder decodes a Gorilla bitstream built by Encoder.
type Decoder struct {
	br *bitstream.Reader

	prevTime      int64
	prevTimeDelta int64
	timeFirst     bool

	prevValue         uint64
	prevLeadingZeros  int
	prevTrailingZeros int
	valueFirst        bool

	count int
}

// NewDecoder returns a decoder over compressed bytes.
func NewDecoder(b []byte) *Decoder {
	return &Decoder{
		br:                bitstream.NewReader(b),
		timeFirst:         true,
		valueFirst:        true,
		prevLeadingZeros:  -1,
		prevTrailingZeros: -1,
	}
}

// DecodeOne reads the next (timestamp, value) pair, or io.EOF when no bits remain
// to start a new sample (aligned end-of-stream for whole-sample multiples).
func (d *Decoder) DecodeOne() (timestamp int64, value float64, err error) {
	if err = d.decodeTimestamp(&timestamp); err != nil {
		return 0, 0, err
	}
	if err = d.decodeValue(&value); err != nil {
		return 0, 0, err
	}
	d.count++
	return timestamp, value, nil
}

func (d *Decoder) decodeTimestamp(t *int64) error {
	if d.timeFirst {
		v, err := d.br.ReadBits(64)
		if err != nil {
			return err
		}
		*t = int64(v)
		d.prevTime = *t
		d.timeFirst = false
		return nil
	}

	b0, err := d.br.ReadBit()
	if err != nil {
		return err
	}
	if !b0 {
		// dod == 0 ⇒ delta is unchanged
		delta := d.prevTimeDelta
		*t = d.prevTime + delta
		d.prevTime = *t
		return nil
	}

	b1, err := d.br.ReadBit()
	if err != nil {
		return err
	}
	if !b1 {
		// 10
		dod, err := d.br.ReadBits(7)
		if err != nil {
			return err
		}
		return d.applyDod(t, int64(dod)-63)
	}
	b2, err := d.br.ReadBit()
	if err != nil {
		return err
	}
	if !b2 {
		// 110
		dod, err := d.br.ReadBits(9)
		if err != nil {
			return err
		}
		return d.applyDod(t, int64(dod)-255)
	}
	b3, err := d.br.ReadBit()
	if err != nil {
		return err
	}
	if !b3 {
		// 1110
		dod, err := d.br.ReadBits(12)
		if err != nil {
			return err
		}
		return d.applyDod(t, int64(dod)-2047)
	}
	// 1111: raw 32 bits are two's-complement of dod
	dodRaw, err := d.br.ReadBits(32)
	if err != nil {
		return err
	}
	return d.applyDod(t, int64(int32(uint32(dodRaw))))
}

func (d *Decoder) applyDod(t *int64, dod int64) error {
	delta := d.prevTimeDelta + dod
	*t = d.prevTime + delta
	d.prevTimeDelta = delta
	d.prevTime = *t
	return nil
}

func (d *Decoder) decodeValue(v *float64) error {
	if d.valueFirst {
		bits, err := d.br.ReadBits(64)
		if err != nil {
			return err
		}
		d.prevValue = bits
		*v = math.Float64frombits(bits)
		d.valueFirst = false
		return nil
	}

	leadBit, err := d.br.ReadBit()
	if err != nil {
		return err
	}
	if !leadBit {
		*v = math.Float64frombits(d.prevValue)
		return nil
	}

	lead2, err := d.br.ReadBit()
	if err != nil {
		return err
	}
	if !lead2 {
		// Reuse previous window.
		meaningful := 64 - d.prevLeadingZeros - d.prevTrailingZeros
		chunk, err := d.br.ReadBits(meaningful)
		if err != nil {
			return err
		}
		xor := chunk << uint(d.prevTrailingZeros)
		valBits := d.prevValue ^ xor
		d.prevValue = valBits
		*v = math.Float64frombits(valBits)
		return nil
	}

	// New window: 5+6+meaningful
	leading, err := d.br.ReadBits(5)
	if err != nil {
		return err
	}
	encM, err := d.br.ReadBits(6)
	if err != nil {
		return err
	}
	meaningful := int(encM)
	if meaningful == 0 {
		meaningful = 64
	}
	trailing := 64 - int(leading) - meaningful
	if trailing < 0 {
		return fmt.Errorf("gorilla: invalid value window (leading=%d meaningful=%d)", leading, meaningful)
	}
	chunk, err := d.br.ReadBits(meaningful)
	if err != nil {
		return err
	}
	xor := chunk << uint(trailing)
	d.prevLeadingZeros = int(leading)
	d.prevTrailingZeros = 64 - int(leading) - meaningful
	d.prevValue ^= xor
	*v = math.Float64frombits(d.prevValue)
	return nil
}

// Count returns the number of samples successfully decoded.
func (d *Decoder) Count() int { return d.count }

// DecodeAll decodes the entire stream into ts and values; both slices are
// replaced. Use when the bitstream is an exact multiple of (ts,value) pairs.
func DecodeAll(b []byte) (ts []int64, values []float64, err error) {
	dec := NewDecoder(b)
	for {
		t, v, e := dec.DecodeOne()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, nil, e
		}
		ts = append(ts, t)
		values = append(values, v)
	}
	return ts, values, nil
}
