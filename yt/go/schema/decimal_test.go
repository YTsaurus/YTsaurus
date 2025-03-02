package schema

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestDecimal(t *testing.T) {
	v, err := NewDecimalValueFromBigInt(big.NewInt(123456), 22, 9)
	require.NoError(t, err)

	ys, err := yson.Marshal(&v)
	require.NoError(t, err)

	var got DecimalValue
	require.NoError(t, yson.Unmarshal(ys, &got))
	assert.Equal(t, v, got)

	a, err := DecodeDecimalValue(got, 22, 9)
	require.NoError(t, err)
	assert.Equal(t, v, a)
}

func TestDecimalDecode(t *testing.T) {
	v, err := NewDecimalValueFromBigInt(big.NewInt(2821000000000000), 35, 12)
	require.NoError(t, err)

	got, err := DecodeDecimalValue(v, 35, 12)
	require.NoError(t, err)

	assert.Equal(t, NewDecodedDecimalValueFromBigInt(big.NewInt(2821000000000000), 35, 12), got)
}
