package emmq

import "context"

// Queue represents a message queue
type Queue struct {
	name     string
	prefix   []byte
	exchange *Exchange
}

// Bind binds the queue to the specified topics
// A topic may be bound to multiple queues
func (q Queue) Bind(topics ...string) {
	for _, t := range topics {
		q.exchange.bind(t, q)
	}
}

// Consume consumes queue messages
func (q Queue) Consume(ctx context.Context) (<-chan Delivery, error) {
	return q.exchange.consume(ctx, q)
}

// Purge purges all queue messages
func (q Queue) Purge() error {
	return q.exchange.purge(q.prefix)
}
