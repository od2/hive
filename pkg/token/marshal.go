package token

import "encoding/base64"

// Marshal returns a URL-safe serialization of a signed token.
func Marshal(sp *SignedPayload) string {
	return EncodedPrefix + base64.RawURLEncoding.EncodeToString(sp.Serialize())
}

// MarshalledSize is the length of the token, marshalled.
const MarshalledSize = len(EncodedPrefix) + 39

// EncodedPrefix is the hardcoded prefix of encoded signed tokens.
const EncodedPrefix = "H"

// Unmarshal deserializes an URL-safe string to a signed token.
// Returns nil if the token is invalid.
func Unmarshal(s string) *SignedPayload {
	if len(s) != MarshalledSize {
		return nil
	}
	if s[:len(EncodedPrefix)] != EncodedPrefix {
		return nil
	}
	buf, err := base64.RawURLEncoding.DecodeString(s[len(EncodedPrefix):])
	if err != nil || len(buf) != SignedPayloadSize {
		return nil
	}
	var sp SignedPayload
	if err := sp.Deserialize(buf[:]); err != nil {
		return nil
	}
	return &sp
}
