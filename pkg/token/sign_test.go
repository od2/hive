package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var payload2 = Payload{
	Exp: 1,
	ID: [12]byte{
		0x01, 0x02, 0x03, 0x04,
		0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C,
	},
}

var serializedSP2 = []byte{
	0x0b,
	// Tag
	0x09, 0xf9, 0x70, 0x02, 0x94, 0x43, 0x68, 0x94, 0xfd, 0xab, 0x2e, 0x33, 0x5f, 0x32, 0x7f, 0xd5,
	// Payload.Exp
	0x00, 0x00, 0x00, 0x01,
	// Payload.XID
	0x01, 0x02, 0x03, 0x04,
	0x05, 0x06, 0x07, 0x08,
	0x09, 0x0A, 0x0B, 0x0C,
}

func TestSigner_Sign(t *testing.T) {
	signer := NewSimpleSigner(&[32]byte{0x03})
	sp := signer.SignNoErr(payload2)
	assert.Equal(t, serializedSP2, sp.Serialize())
}

func TestSigner_Verify(t *testing.T) {
	var sp2 SignedPayload
	assert.NoError(t, sp2.Deserialize(serializedSP2))
	assert.True(t, NewSimpleSigner(&[32]byte{0x03}).VerifyTag(&sp2))
	assert.False(t, NewSimpleSigner(&[32]byte{0x04}).VerifyTag(&sp2))
	sp3 := sp2
	sp3.Payload.ID[0] = 99
	assert.False(t, NewSimpleSigner(&[32]byte{0x03}).VerifyTag(&sp3))
}
