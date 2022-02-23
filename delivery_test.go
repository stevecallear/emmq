package emmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stevecallear/emmq"
)

func TestDelivery_Delete(t *testing.T) {
	t.Run("should delete the message", func(t *testing.T) {
		e, close := newExchange(func(o *emmq.Options) {
			o.PollInterval = 50 * time.Millisecond
			o.VisibilityTimeout = 100 * time.Millisecond
		})
		defer close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qn, tn := uuid.NewString(), uuid.NewString()

		q, err := e.Declare(qn)
		assertErrorExists(t, err, false)

		q.Bind(tn)

		c, err := q.Consume(ctx)
		assertErrorExists(t, err, false)

		err = e.Publish(tn, []byte{})
		assertErrorExists(t, err, false)

		assertDeliveries(t, c, 1, func(d emmq.Delivery) {
			err := d.Delete()
			assertErrorExists(t, err, false)
		})

		assertNoDeliveries(t, c, 200*time.Millisecond)
	})
}
