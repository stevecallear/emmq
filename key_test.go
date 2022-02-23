package emmq_test

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stevecallear/emmq"
)

func TestNewKey(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		dueAt  time.Time
		err    bool
	}{
		{
			name:  "should return an error if the prefix is empty",
			dueAt: time.Now().UTC(),
			err:   true,
		},
		{
			name:   "should return an error if the prefix is too long",
			prefix: strings.Repeat("x", 256),
			dueAt:  time.Now().UTC(),
			err:    true,
		},
		{
			name:   "should return the key",
			prefix: "prefix",
			dueAt:  time.Now().UTC(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k, err := emmq.NewKey(tt.prefix, tt.dueAt)
			assertErrorExists(t, err, tt.err)
			if err != nil {
				return
			}

			expPre := append([]byte{byte(uint8(len(tt.prefix)))}, tt.prefix...)
			assertBytesEqual(t, k.Prefix(), expPre)

			if act, exp := k.DueAt(), tt.dueAt; act != exp {
				t.Errorf("got %v, expected %v", act, exp)
			}

			var zu uuid.UUID
			if act, nexp := k.UUID(), zu; act == nexp {
				t.Errorf("got %v, expected a uuid", act)
			}
		})
	}
}

func TestKey_Delay(t *testing.T) {
	t.Run("should return a delayed copy of the key", func(t *testing.T) {
		d := time.Now().UTC()
		k := newKey("prefix", d)

		dk := k.Delay(1 * time.Minute)

		if act, exp := dk.DueAt(), d.Add(1*time.Minute); act != exp {
			t.Errorf("got %v, expected %v", act, exp)
		}

		if act, exp := k.DueAt(), d; act != exp {
			t.Errorf("got %v, expected %v", act, exp)
		}
	})
}

func TestKey_Sorting(t *testing.T) {
	t.Run("keys should sort correctly", func(t *testing.T) {
		da := time.Now().UTC()

		k1 := newKey("prefix", da.Add(1*time.Minute))
		k2 := newKey("prefix", da)

		act := [][]byte{k1, k2}
		sort.Slice(act, func(i, j int) bool {
			return bytes.Compare(act[i], act[j]) < 0
		})

		if exp := [][]byte{k2, k1}; !reflect.DeepEqual(act, exp) {
			t.Errorf("got %v, expected %v", act, exp)
		}
	})
}

func newKey(prefix string, dueAt time.Time) emmq.Key {
	k, err := emmq.NewKey(prefix, dueAt)
	if err != nil {
		panic(err)
	}

	return k
}
