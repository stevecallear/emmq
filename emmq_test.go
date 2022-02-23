package emmq_test

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stevecallear/emmq"
)

var (
	sut          *emmq.Exchange
	pollInterval = 100 * time.Millisecond
)

func TestMain(m *testing.M) {
	e, close := newExchange(emmq.WithPolling(pollInterval, 10))
	defer close()
	sut = e
	m.Run()
}

func newExchange(optFns ...func(*emmq.Options)) (*emmq.Exchange, func()) {
	p := "test-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	e, err := emmq.Open(p, optFns...)
	if err != nil {
		panic(err)
	}

	return e, func() {
		defer os.RemoveAll(p)
		if err := e.Close(); err != nil {
			panic(err)
		}
	}
}

func merge(cs ...<-chan emmq.Delivery) <-chan emmq.Delivery {
	mc := make(chan emmq.Delivery)
	for _, c := range cs {
		go func(c <-chan emmq.Delivery) {
			for d := range c {
				mc <- d
			}
		}(c)
	}

	return mc
}

func assertErrorExists(t *testing.T, act error, exp bool) {
	if act != nil && !exp {
		t.Fatalf("got %v, expected nil", act)
	}

	if act == nil && exp {
		t.Fatal("got nil, expected an error")
	}
}

func assertBytesEqual(t *testing.T, act, exp []byte) {
	if !bytes.Equal(act, exp) {
		t.Errorf("got %v, expected %v", act, exp)
	}
}

func assertDeliveries(t *testing.T, c <-chan emmq.Delivery, n int, fns ...func(emmq.Delivery)) {
	q := make(chan struct{})

	go func() {
		defer close(q)

		var dc int
		for {
			d := <-c
			dc++

			for _, fn := range fns {
				fn(d)
			}

			if dc >= n {
				return
			}
		}
	}()

	tt := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-q:
			return
		case <-tt.C:
			t.Error("expected a delivery, got none")
			return
		}
	}
}

func assertNoDeliveries(t *testing.T, c <-chan emmq.Delivery, wait time.Duration) {
	dt := time.NewTimer(wait)
	for {
		select {
		case <-c:
			t.Error("got delivery, expected none")
			return
		case <-dt.C:
			return
		}
	}
}
