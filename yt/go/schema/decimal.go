package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"

	"go.ytsaurus.tech/yt/go/yson"
)

type decimalForm uint8

const (
	finiteDecimal = iota
	nanDecimal
	infDecimal
)

// DecimalValue stores raw encoded value of decimal
// to work with decimal value decode it to DecodedDecimalValue
type DecimalValue struct {
	encodedData []byte
}

// UnmarshalYSON takes a slice of bytes as input and stores a copy of it in the DecimalValue.
// This method is designed to prepare the data for subsequent decoding into a more structured form.
// The actual decoding process is not performed within this method due to the requirement of precision and scale values,
// which are necessary to accurately interpret the decimal value but are not available at this stage.
func (d *DecimalValue) UnmarshalYSON(input []byte) error {
	d.encodedData = make([]byte, len(input))
	copy(d.encodedData, input)
	return nil
}

func (d *DecimalValue) MarshalYSON(w *yson.Writer) error {
	output := make([]byte, len(d.encodedData))
	copy(output, d.encodedData)
	// TODO: how to serialize it properly? after marshal the value is mirrored
	w.Bytes(output)
	return w.Err()
}

// DecodedDecimalValue stores decoded decimal value
type DecodedDecimalValue struct {
	Precision int
	Scale     int
	Value     *big.Int
	form      decimalForm
}

func (d DecodedDecimalValue) IsNan() bool {
	return d.form == nanDecimal
}

func (d DecodedDecimalValue) IsInf() bool {
	return d.form == infDecimal
}

// IsZero returns true if Decimal contains zero value.
func (d DecodedDecimalValue) IsZero() bool {
	return d.form == finiteDecimal && d.Value.Cmp(big.NewInt(0)) == 0
}

func (d DecodedDecimalValue) Encode() ([]byte, error) {
	byteSize, err := getDecimalByteSize(d.Precision)
	if err != nil {
		return nil, err
	}

	// check if the decimal represents a special value (NaN, Inf, -Inf)
	if value, ok := d.encodeIfInfiniteValue(byteSize); ok {
		return value, nil
	}

	buf := make([]byte, byteSize)

	if d.Value.BitLen() > byteSize*8 {
		return nil, fmt.Errorf("value is too large for the expected byte size")
	}

	// Encode the value, taking into account the shift to restore it to its original position.
	adjustedValue := new(big.Int).Set(d.Value)
	adjustment := big.NewInt(1)
	adjustment.Lsh(adjustment, uint(8*byteSize-1))
	adjustedValue.Add(adjustedValue, adjustment)

	switch byteSize {
	case 4:
		binary.BigEndian.PutUint32(buf, uint32(adjustedValue.Int64()))
	case 8:
		binary.BigEndian.PutUint64(buf, adjustedValue.Uint64())
	case 16:
		lo := new(big.Int).And(adjustedValue, new(big.Int).SetUint64(0xffffffffffffffff))
		hi := new(big.Int).Rsh(adjustedValue, 64)
		binary.BigEndian.PutUint64(buf[:8], hi.Uint64())
		binary.BigEndian.PutUint64(buf[8:], lo.Uint64())
	default:
		return nil, fmt.Errorf("unexpected byte size: %d", byteSize)
	}

	return buf, nil
}

func (d DecodedDecimalValue) encodeIfInfiniteValue(byteSize int) ([]byte, bool) {
	var (
		inf = map[int][]byte{
			4:  {0xFF, 0xFF, 0xFF, 0xFE},
			8:  {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE},
			16: {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE},
		}

		infNeg = map[int][]byte{
			4:  {0x00, 0x00, 0x00, 0x02},
			8:  {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
			16: {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
		}

		nan = map[int][]byte{
			4:  {0xFF, 0xFF, 0xFF, 0xFF},
			8:  {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			16: {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		}
	)

	switch d.form {
	case infDecimal:
		if d.Value.Sign() < 0 {
			return infNeg[byteSize], true
		}
		return inf[byteSize], true
	case nanDecimal:
		return nan[byteSize], true
	default:
		return nil, false
	}
}

// NewDecimalValueFromBigInt creates already encoded decimal value from big.Int value
func NewDecimalValueFromBigInt(v *big.Int, precision, scale int) (DecimalValue, error) {
	d := NewDecodedDecimalValueFromBigInt(v, precision, scale)
	data, err := d.Encode()
	return DecimalValue{
		encodedData: data,
	}, err
}

// NewDecimalValueFromBigInt creates new decimal from big.Int value
func NewDecodedDecimalValueFromBigInt(v *big.Int, precision, scale int) DecodedDecimalValue {
	return DecodedDecimalValue{
		Precision: precision,
		Scale:     scale,
		Value:     v,
	}
}

// DecodeDecimal decodes a binary representation of a decimal number to *big.Int
func DecodeDecimalValue(d DecimalValue, precision, scale int) (DecodedDecimalValue, error) {
	expectedSize, err := getDecimalByteSize(precision)
	if err != nil {
		return DecodedDecimalValue{}, err
	}

	if len(d.encodedData) != expectedSize {
		return DecodedDecimalValue{}, fmt.Errorf("binary value has invalid length; expected: %d, got: %d", expectedSize, len(d.encodedData))
	}

	if value, ok := decodeIfInfiniteValue(d.encodedData); ok {
		return value, nil
	}

	// Decode integer value from binary
	var intval *big.Int
	switch len(d.encodedData) {
	case 4:
		intval = big.NewInt(int64(binary.BigEndian.Uint32(d.encodedData)))
	case 8:
		intval = big.NewInt(int64(binary.BigEndian.Uint64(d.encodedData)))
	case 16:
		hi := binary.BigEndian.Uint64(d.encodedData[:8])
		lo := binary.BigEndian.Uint64(d.encodedData[8:])
		intval = new(big.Int).SetUint64(hi)
		intval.Lsh(intval, 64).Or(intval, new(big.Int).SetUint64(lo))
	default:
		return DecodedDecimalValue{}, fmt.Errorf("unexpected binary value length: %d", len(d.encodedData))
	}

	// Adjust the integer based on the encoding scheme
	adjustment := big.NewInt(1)
	adjustment.Lsh(adjustment, uint(8*expectedSize-1))
	intval.Sub(intval, adjustment)

	return DecodedDecimalValue{
		Precision: precision,
		Scale:     scale,
		Value:     intval,
	}, nil
}

func decodeIfInfiniteValue(data []byte) (DecodedDecimalValue, bool) {
	inf := []byte{0xFF, 0xFF, 0xFF, 0xFE}
	infNeg := []byte{0x00, 0x00, 0x00, 0x02}
	nan := []byte{0xFF, 0xFF, 0xFF, 0xFF}

	var d DecodedDecimalValue

	switch len(data) {
	case 4, 8, 16:
		// For positive infinity
		if len(data) >= 4 && bytes.Equal(data[len(data)-4:], inf) {
			d.form = infDecimal
			return d, true
		}
		// For negative infinity
		if len(data) >= 4 && bytes.Equal(data[len(data)-4:], infNeg) {
			d.form = infDecimal
			return d, true
		}
		// For NaN
		if len(data) >= 4 && bytes.Equal(data[len(data)-4:], nan) {
			d.form = nanDecimal
			return d, true
		}
	}

	return DecodedDecimalValue{}, false
}

func getDecimalByteSize(precision int) (int, error) {
	switch {
	case precision < 0 || precision > 38:
		return 0, fmt.Errorf("bad precision: %d", precision)
	case precision <= 9:
		return 4, nil
	case precision <= 18:
		return 8, nil
	default:
		return 16, nil
	}
}
