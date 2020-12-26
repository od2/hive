package token

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// Epoch is 2020-01-01 00:00:00 UTC.
var Epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// ErrInvalidTimestamp marks a timestamp that the token cannot describe.
var ErrInvalidTimestamp = errors.New("invalid timestamp")

// ID is the binary representation of an auth token ID.
type ID [12]byte

// Payload identifies a token.
type Payload struct {
	Exp uint32 // expiration timestamp, in seconds since epoch
	ID  ID
}

// PayloadSize is the serialized size of Payload.
const PayloadSize = 16

// Serialize packs the Payload to binary.
func (p *Payload) Serialize() []byte {
	b := make([]byte, PayloadSize)
	binary.BigEndian.PutUint32(b[:4], p.Exp)
	copy(b[4:16], p.ID[:])
	return b
}

// Deserialize unpacks binary to a payload.
func (p *Payload) Deserialize(b []byte) error {
	if len(b) != PayloadSize {
		return fmt.Errorf("invalid length: %d", len(b))
	}
	p.Exp = binary.BigEndian.Uint32(b[:4])
	copy(p.ID[:], b[4:16])
	return nil
}

// ExpToTime unpacks an expiration uint32 to a timestamp.
func ExpToTime(exp uint32) time.Time {
	return Epoch.Add(time.Duration(exp) * time.Second)
}

// TimeToExp tries to pack an expiration timestamp to uint32.
// Fails with expiration dates before year 2020 or after year 2155.
func TimeToExp(t time.Time) (uint32, error) {
	if t.Before(Epoch) {
		return 0, ErrInvalidTimestamp
	}
	secs := t.Sub(Epoch) / time.Second
	if secs > 0xFFFF_FFFF {
		return 0, ErrInvalidTimestamp
	}
	return uint32(secs), nil
}
