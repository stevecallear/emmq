package emmq_test

import (
	"bytes"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stevecallear/emmq"
)

func TestNewKey(t *testing.T) {
	t.Run("should return the key", func(t *testing.T) {
		da := time.Now().UTC()
		k := emmq.NewKey("topic", emmq.StatusReady, da)

		if act, exp := k.Topic(), "topic"; act != exp {
			t.Errorf("got %s, expected %s", act, exp)
		}

		if act, exp := k.Status(), emmq.StatusReady; act != exp {
			t.Errorf("got %d, expected %d", act, exp)
		}

		if act, exp := k.DueAt(), da; act != exp {
			t.Errorf("got %v, expected %v", act, exp)
		}

		var zu uuid.UUID
		if act, nexp := k.UUID(), zu; act == nexp {
			t.Errorf("got %v, expected a uuid", act)
		}
	})
}

func TestKey_Sorting(t *testing.T) {
	t.Run("keys should sort correctly", func(t *testing.T) {
		da := time.Now().UTC()

		k1 := emmq.NewKey("topic", emmq.StatusReady, da.Add(1*time.Minute))
		k2 := emmq.NewKey("topic", emmq.StatusReady, da)

		act := [][]byte{k1, k2}
		sort.Slice(act, func(i, j int) bool {
			return bytes.Compare(act[i], act[j]) < 0
		})

		if exp := [][]byte{k2, k1}; !reflect.DeepEqual(act, exp) {
			t.Errorf("got %v, expected %v", act, exp)
		}
	})
}
