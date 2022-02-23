package emmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stevecallear/emmq"
)

func TestQueue_Consume(t *testing.T) {
	t.Run("should return an error if the queue is already consumed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		q, err := sut.Declare(uuid.NewString())
		assertErrorExists(t, err, false)

		_, err = q.Consume(ctx)
		assertErrorExists(t, err, false)

		_, err = q.Consume(ctx)
		if err == nil {
			t.Error("got nil, expected an error")
		}
	})

	t.Run("should apply the poll batch size", func(t *testing.T) {
		e, close := newExchange(emmq.WithPolling(100*time.Millisecond, 2))
		defer close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		q, err := e.Declare(uuid.NewString())
		assertErrorExists(t, err, false)

		tn := uuid.NewString()
		q.Bind(tn)

		c, err := q.Consume(ctx)
		assertErrorExists(t, err, false)

		for _, v := range []string{"a", "b", "c"} {
			err = e.Publish(tn, []byte(v), emmq.WithWait())
			assertErrorExists(t, err, false)
		}

		assertDeliveries(t, c, 2)
		assertNoDeliveries(t, c, 90*time.Millisecond)
	})
}

func TestQueue_Bind(t *testing.T) {
	t.Run("should bind to multiple topics", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qn, t1, t2 := uuid.NewString(), uuid.NewString(), uuid.NewString()

		q, err := sut.Declare(qn)
		assertErrorExists(t, err, false)

		q.Bind(t1, t2)

		c, err := q.Consume(ctx)
		assertErrorExists(t, err, false)

		for _, tn := range []string{t1, t2} {
			err = sut.Publish(tn, []byte("value"))
			assertErrorExists(t, err, false)
		}

		assertDeliveries(t, c, 2)
	})

	t.Run("should bind to multiple queues", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tn, qn1, qn2 := uuid.NewString(), uuid.NewString(), uuid.NewString()

		cs := []<-chan emmq.Delivery{}
		for _, qn := range []string{qn1, qn2} {
			q, err := sut.Declare(qn)
			assertErrorExists(t, err, false)

			q.Bind(tn)

			c, err := q.Consume(ctx)
			assertErrorExists(t, err, false)

			cs = append(cs, c)
		}

		err := sut.Publish(tn, []byte("value"))
		assertErrorExists(t, err, false)

		c := merge(cs...)
		assertDeliveries(t, c, 2)
	})
}

func TestQueue_Purge(t *testing.T) {
	t.Run("should purge queue messages", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		q, err := sut.Declare(uuid.NewString())
		assertErrorExists(t, err, false)

		tn := uuid.NewString()
		q.Bind(tn)

		c, err := q.Consume(ctx)
		assertErrorExists(t, err, false)

		err = sut.Publish(tn, []byte{}, emmq.WithWait())
		assertErrorExists(t, err, false)

		err = q.Purge()
		assertErrorExists(t, err, false)

		assertNoDeliveries(t, c, 2*pollInterval)
	})
}
