package emmq

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Key represents a message key
type Key []byte

// NewKey returns a new key
func NewKey(prefix string, dueAt time.Time) (Key, error) {
	pb, err := encodePrefix(prefix)
	if err != nil {
		return nil, err
	}

	pl := len(pb)

	k := make(Key, pl+24)
	copy(k, pb)

	binary.BigEndian.PutUint64(k[pl:pl+8], uint64(dueAt.UnixNano()))
	u := uuid.New()
	copy(k[pl+8:], u[:])

	return k, nil
}

// Prefix returns the key prefix
func (k Key) Prefix() []byte {
	return k[:len(k)-24]
}

// DueAt returns the due at time for the key
func (k Key) DueAt() time.Time {
	ns := binary.BigEndian.Uint64(k[len(k)-24 : len(k)-16])
	return time.Unix(0, int64(ns)).UTC()
}

// UUID returns the uuid for the key
func (k Key) UUID() uuid.UUID {
	var u uuid.UUID
	copy(u[:], k[len(k)-16:])
	return u
}

// Delay returns a copy of the key with the specified due at delay
func (k Key) Delay(d time.Duration) Key {
	kc := make(Key, len(k))
	copy(kc, k)

	dueAt := k.DueAt().Add(d)
	binary.BigEndian.PutUint64(kc[len(kc)-24:len(kc)-16], uint64(dueAt.UnixNano()))

	return kc
}

func encodePrefix(prefix string) ([]byte, error) {
	pb := []byte(prefix)

	pl := len(pb)
	if pl < 1 || pl > 255 {
		return nil, errors.New("invalid prefix length")
	}

	eb := make([]byte, pl+1)
	eb[0] = byte(uint8(pl))
	copy(eb[1:], pb)

	return eb, nil
}
