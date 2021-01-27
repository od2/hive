package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var id2 = [12]byte{
	0x01, 0x02, 0x03, 0x04,
	0x05, 0x06, 0x07, 0x08,
	0x09, 0x0A, 0x0B, 0x0C,
}

var serializedSP2 = []byte{
	0x0b,
	// Tag
	0x20, 0x17, 0xe5, 0x04, 0xa0, 0x58, 0xa4, 0x6b, 0x22, 0x38, 0x19, 0x88, 0x13, 0x65, 0xac, 0xf0,
	// Payload.XID
	0x01, 0x02, 0x03, 0x04,
	0x05, 0x06, 0x07, 0x08,
	0x09, 0x0A, 0x0B, 0x0C,
}

func TestSigner_Sign(t *testing.T) {
	signer := NewSimpleSigner(&[32]byte{0x03})
	sp := signer.SignNoErr(id2)
	assert.Equal(t, serializedSP2, sp.Serialize())
}

func TestSigner_Verify(t *testing.T) {
	var sp2 SignedPayload
	assert.NoError(t, sp2.Deserialize(serializedSP2))
	assert.True(t, NewSimpleSigner(&[32]byte{0x03}).VerifyTag(&sp2))
	assert.False(t, NewSimpleSigner(&[32]byte{0x04}).VerifyTag(&sp2))
	sp3 := sp2
	sp3.ID[0] = 99
	assert.False(t, NewSimpleSigner(&[32]byte{0x03}).VerifyTag(&sp3))
}
