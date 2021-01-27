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
}

var validToken1 = "HC7CsRB1tPUFI8jSZWvO7yhYo5riFOMI-ryAhSv4"

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
			0xb0, 0xac, 0x44, 0x1d, 0x6d, 0x3d, 0x41, 0x48,
			0xf2, 0x34, 0x99, 0x5a, 0xf3, 0xbb, 0xca, 0x16,
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
