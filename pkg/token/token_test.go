package token

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPayload_Serialize(t *testing.T) {
	p := Payload{
		Exp: 1010110,
		ID: [12]byte{
			0x5c, 0x20, 0x92, 0x2b,
			0xcb, 0xe5, 0x44, 0x31,
			0x53, 0x9d, 0x48, 0xa1,
		},
	}
	assert.Equal(t, []byte{
		0x00, 0x0f, 0x69, 0xbe,
		0x5c, 0x20, 0x92, 0x2b,
		0xcb, 0xe5, 0x44, 0x31,
		0x53, 0x9d, 0x48, 0xa1,
	}, p.Serialize())
}

func TestPayload_Deserialize(t *testing.T) {
	var p Payload
	var err error
	err = p.Deserialize(nil)
	assert.EqualError(t, err, "invalid length: 0")
	err = p.Deserialize([]byte{
		0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF,
	})
	assert.Equal(t, Payload{
		Exp: 0xFFFFFFFF,
		ID: [12]byte{
			0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF,
		},
	}, p)
	assert.NoError(t, err)
}

func TestExpToTime(t *testing.T) {
	assert.Equal(t, Epoch, ExpToTime(0))
	assert.Equal(t, Epoch.Add(256*time.Second), ExpToTime(256))
	assert.Equal(t, Epoch.Add(0xFFFFFFFF*time.Second), ExpToTime(0xFFFFFFFF))
}

func TestTimeToExp(t *testing.T) {
	var exp uint32
	var err error
	exp, err = TimeToExp(Epoch)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), exp)
	exp, err = TimeToExp(Epoch.Add(-24 * time.Hour))
	assert.Equal(t, ErrInvalidTimestamp, err)
	exp, err = TimeToExp(Epoch.Add(0xFFFFFFFF * time.Second))
	assert.NoError(t, err)
	assert.Equal(t, uint32(0xFFFFFFFF), exp)
	exp, err = TimeToExp(Epoch.Add(0x100000000 * time.Second))
	assert.Equal(t, ErrInvalidTimestamp, err)
}
