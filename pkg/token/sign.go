package token

import (
	"crypto/subtle"
	"fmt"

	"golang.org/x/crypto/blake2b"
)

// Signer creates or checks the MAC tag on signed payloads.
type Signer interface {
	Sign(p Payload) (*SignedPayload, error)
	VerifyTag(sp *SignedPayload) bool
}

// SimpleSigner holds a secret used to sign payloads.
type SimpleSigner struct {
	secret *[32]byte
}

// NewSimpleSigner creates a signer from a secret.
func NewSimpleSigner(secret *[32]byte) SimpleSigner {
	return SimpleSigner{secret: secret}
}

// TagLen is the length of the MAC tag.
const TagLen = 16

// SignPayload computes a MAC tag.
func SignPayload(secret *[32]byte, p Payload) (o [16]byte) {
	h, err := blake2b.New(TagLen, secret[:])
	if err != nil {
		panic(err)
	}
	if _, err := h.Write(p.Serialize()); err != nil {
		panic(err)
	}
	copy(o[:], h.Sum(o[:0]))
	return
}

// SignNoErr computes a MAC tag and returns it with the payload.
func (s SimpleSigner) SignNoErr(p Payload) (out SignedPayload) {
	out.Payload = p
	out.Tag = SignPayload(s.secret, p)
	return
}

// Sign is SignErr, always returns nil as error.
// Required to implement Signer.
func (s SimpleSigner) Sign(p Payload) (*SignedPayload, error) {
	out := s.SignNoErr(p)
	return &out, nil
}

// VerifyTag checks the MAC. It doesn't check the expiration date.
func (s SimpleSigner) VerifyTag(sp *SignedPayload) bool {
	expTag := SignPayload(s.secret, sp.Payload)
	return subtle.ConstantTimeCompare(sp.Tag[:], expTag[:]) == 1
}

// SignedPayload is a payload with a tag.
type SignedPayload struct {
	Tag     [TagLen]byte // MAC signature
	Payload Payload
}

// SignedPayloadSize is the serialized size of SignedPayload.
const SignedPayloadSize = 1 + 16 + PayloadSize

// SignedPayloadPrefix is a single byte prefix.
const SignedPayloadPrefix = uint8(11)

// Serialize encodes a binary signed payload with a prefix.
func (sp *SignedPayload) Serialize() []byte {
	b := make([]byte, SignedPayloadSize)
	b[0] = SignedPayloadPrefix
	copy(b[1:17], sp.Tag[:])
	copy(b[17:33], sp.Payload.Serialize())
	return b
}

// Deserialize decodes a binary signed payload with a prefix.
// It does not verify the MAC nor expiration time.
func (sp *SignedPayload) Deserialize(b []byte) error {
	if len(b) != SignedPayloadSize {
		return fmt.Errorf("invalid length: %d", len(b))
	}
	if b[0] != SignedPayloadPrefix {
		return fmt.Errorf("invalid prefix: %x", b[0])
	}
	copy(sp.Tag[:], b[1:17])
	return sp.Payload.Deserialize(b[17:33])
}
