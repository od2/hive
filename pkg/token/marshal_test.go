package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var macSecret = [32]byte{
	0x23, 0xa9, 0x0e, 0x26, 0x0b, 0x98, 0x66, 0x0b,
	0x86, 0xbf, 0x83, 0xdb, 0xf5, 0x58, 0x91, 0xe6,
	0x2c, 0x87, 0xdb, 0x12, 0xd8, 0x98, 0xc5, 0xc1,
	0x17, 0xde, 0xe3, 0xe1, 0xdb, 0x92, 0x6f, 0x16,
}

var id1 = ID{
	0x28, 0xe6, 0xb8, 0x85,
	0x38, 0xc2, 0x3e, 0xaf,
	0x20, 0x21, 0x4a, 0xfe,
	0x88, 0x94, 0x77, 0x4f,
}

var validToken1 = "HCzSGK1WSf4OlRQzcxk0uFwYo5riFOMI-ryAhSv6IlHdP"

func TestMarshal(t *testing.T) {
	signer := NewSimpleSigner(&macSecret)
	signedPayload := signer.SignNoErr(id1)
	token := Marshal(&signedPayload)
	assert.Equal(t, validToken1, token)
}

func TestUnmarshal(t *testing.T) {
	t.Run("InvalidSize", func(t *testing.T) {
		assert.Nil(t, Unmarshal(""))
		assert.Nil(t, Unmarshal(validToken1+"a"))
		assert.Nil(t, Unmarshal(validToken1[:MarshalledSize-1]))
	})
	t.Run("Normal", func(t *testing.T) {
		signedPayload := Unmarshal(validToken1)
		require.NotNil(t, signedPayload)
		assert.Equal(t, [16]byte{
			0x34, 0x86, 0x2b, 0x55, 0x92, 0x7f, 0x83, 0xa5,
			0x45, 0x0c, 0xdc, 0xc6, 0x4d, 0x2e, 0x17, 0x06,
		}, signedPayload.Tag)
		assert.Equal(t, id1, signedPayload.ID)
	})
	t.Run("InvalidPrefix", func(t *testing.T) {
		assert.Nil(t, Unmarshal("aNqg74enY1hmZOrzkSxrtknlNhPI1Vug2SRQmQXYgbF4"))
		assert.Nil(t, Unmarshal("BD1g74enY1hmZOrzkSxrtknlNhPI1Vug2SRQmQXYgbF4"))
		assert.Nil(t, Unmarshal("bd2aNqg74enY1hmZOrzkSxrtknlNhPI1Vug2SRQmQXYgbF4"))
	})
	t.Run("Invalid", func(t *testing.T) {
		assert.Nil(t, Unmarshal("bd1bZZZbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
		assert.Nil(t, Unmarshal("bd1123"))
		assert.Nil(t, Unmarshal("bd1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb "))
	})
}
