package emmq_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/stevecallear/emmq"
)

func TestExchange_Declare(t *testing.T) {
	qn := uuid.NewString()

	tests := []struct {
		name  string
		setup func(*testing.T, *emmq.Exchange)
		queue string
	}{
		{
			name: "should return an error if the queue has already been declared",
			setup: func(t *testing.T, e *emmq.Exchange) {
				_, err := e.Declare(qn)
				assertErrorExists(t, err, false)
			},
			queue: qn,
		},
		{
			name:  "should return an error if the queue name is invalid",
			setup: func(t *testing.T, e *emmq.Exchange) {},
			queue: strings.Repeat("x", 256),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(t, sut)

			_, err := sut.Declare(tt.queue)
			assertErrorExists(t, err, true)
		})
	}
}

func TestExchange_Publish(t *testing.T) {
	exp := []byte("value")

	tests := []struct {
		name   string
		opts   func(*emmq.PublishOptions)
		pre    bool
		assert func(*testing.T, <-chan emmq.Delivery)
	}{
		{
			name: "should publish immediate messages",
			opts: func(o *emmq.PublishOptions) {},
			assert: func(t *testing.T, c <-chan emmq.Delivery) {
				assertDeliveries(t, c, 1, func(d emmq.Delivery) {
					assertBytesEqual(t, d.Value, exp)
				})
			},
		},
		{
			name: "should publish immediate messages pre consume",
			opts: func(o *emmq.PublishOptions) {},
			pre:  true,
			assert: func(t *testing.T, c <-chan emmq.Delivery) {
				assertDeliveries(t, c, 1, func(d emmq.Delivery) {
					assertBytesEqual(t, d.Value, exp)
				})
			},
		},
		{
			name: "should publish immediate wait messages",
			opts: emmq.WithWait(),
			assert: func(t *testing.T, c <-chan emmq.Delivery) {
				assertDeliveries(t, c, 1, func(d emmq.Delivery) {
					assertBytesEqual(t, d.Value, exp)
				})
			},
		},
		{
			name: "should publish delayed messages",
			opts: emmq.WithDelay(2 * pollInterval),
			assert: func(t *testing.T, c <-chan emmq.Delivery) {
				assertDeliveries(t, c, 1, func(d emmq.Delivery) {
					assertBytesEqual(t, d.Value, exp)
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qn, tn := uuid.NewString(), uuid.NewString()

			q, err := sut.Declare(qn)
			assertErrorExists(t, err, false)

			q.Bind(tn)

			if tt.pre {
				err = sut.Publish(tn, exp, tt.opts)
				assertErrorExists(t, err, false)
			}

			c, err := q.Consume(context.Background())
			assertErrorExists(t, err, false)

			if !tt.pre {
				err = sut.Publish(tn, exp, tt.opts)
				assertErrorExists(t, err, false)
			}

			tt.assert(t, c)
		})
	}
}

func TestExchange_PurgeAll(t *testing.T) {
	t.Run("should purge all messages", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		e, close := newExchange(emmq.WithPolling(pollInterval, 10))
		defer close()

		q, err := e.Declare(uuid.NewString())
		assertErrorExists(t, err, false)

		tn := uuid.NewString()
		q.Bind(tn)

		c, err := q.Consume(ctx)
		assertErrorExists(t, err, false)

		err = e.Publish(tn, []byte{}, emmq.WithWait())
		assertErrorExists(t, err, false)

		err = e.PurgeAll()
		assertErrorExists(t, err, false)

		assertNoDeliveries(t, c, 2*pollInterval)
	})
}
