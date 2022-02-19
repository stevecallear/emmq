package emmq

import (
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

type (
	// Key represents a key
	Key []byte

	// Status represents a message status
	Status uint8
)

const (
	// StatusReady indicates that the message is ready
	StatusReady Status = 1 << iota

	// StatusUnacked indicates that the message has been consumed, but not acked
	StatusUnacked

	// StatusNacked indicates that the message has been consumed and nacked
	StatusNacked
)

// NewKey returns a new key
func NewKey(topic string, s Status, dueAt time.Time) Key {
	tb := []byte(topic)
	tl := len(tb)

	k := make(Key, tl+25)
	k[0] = byte(s)
	copy(k[1:], []byte(topic))

	binary.BigEndian.PutUint64(k[tl+1:tl+9], uint64(dueAt.UnixNano()))
	u := uuid.New()
	copy(k[tl+9:], u[:])

	return k
}

// Status returns the status byte for the key
func (k Key) Status() Status {
	return Status(k[0])
}

// Topic returns the topic for the key
func (k Key) Topic() string {
	return string(k[1 : len(k)-24])
}

// DueAt returns the due at time for the key
func (k Key) DueAt() time.Time {
	ns := binary.BigEndian.Uint64(k[len(k)-24 : len(k)-8])
	return time.Unix(0, int64(ns)).UTC()
}

// UUID returns the uuid for the key
func (k Key) UUID() uuid.UUID {
	var u uuid.UUID
	copy(u[:], k[len(k)-16:])
	return u
}

func (k Key) withStatus(s Status) Key {
	kc := make([]byte, len(k))
	copy(kc, k)
	kc[0] = byte(s)
	return kc
}

func encodePrefix(topic string, s Status) []byte {
	tb := []byte(topic)
	tl := len(tb)
	b := make([]byte, tl+1)
	b[0] = byte(s)
	copy(b[1:], tb)
	return b
}

func hasStatus(s Status, cmp Status) bool {
	return s&cmp != 0
}
