package emmq

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type (
	// Exchange represents a message exchange
	Exchange struct {
		opts   Options
		db     *badger.DB
		queues map[string]chan Delivery
		topics map[string]map[string]struct{}
		quit   chan struct{}
		wg     *sync.WaitGroup
		mu     *sync.RWMutex
	}

	// Options represents exchange options
	Options struct {
		PollInterval      time.Duration
		PollBatchSize     int
		VisibilityTimeout time.Duration
		BadgerOptions     badger.Options
		OnError           func(error)
	}

	// PublishOptions represents message publish options
	PublishOptions struct {
		Wait  bool
		Delay time.Duration
	}
)

// WithDelay delays message consumption until the first poll interval
// after the specified duration
func WithDelay(d time.Duration) func(*PublishOptions) {
	return func(o *PublishOptions) {
		o.Delay = d
	}
}

// WithPolling configures the exchange poll interval and batch size
func WithPolling(interval time.Duration, batchSize int) func(*Options) {
	return func(o *Options) {
		o.PollInterval = interval
		o.PollBatchSize = batchSize
	}
}

// WithWait delays message consumption until the next poll interval
func WithWait() func(*PublishOptions) {
	return func(o *PublishOptions) {
		o.Wait = true
	}
}

// Open opens a new exchange with the specified options
func Open(path string, optFns ...func(*Options)) (*Exchange, error) {
	o := Options{
		PollInterval:      100 * time.Millisecond,
		PollBatchSize:     10,
		VisibilityTimeout: 30 * time.Second,
		BadgerOptions:     badger.DefaultOptions(path),
		OnError: func(err error) {
			log.Println(err)
		},
	}
	for _, fn := range optFns {
		fn(&o)
	}

	db, err := badger.Open(o.BadgerOptions)
	if err != nil {
		return nil, err
	}

	return &Exchange{
		opts:   o,
		db:     db,
		queues: map[string]chan Delivery{},
		topics: map[string]map[string]struct{}{},
		quit:   make(chan struct{}),
		wg:     new(sync.WaitGroup),
		mu:     new(sync.RWMutex),
	}, nil
}

// Declare declares a new queue with the specified name
func (e *Exchange) Declare(queue string) (Queue, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.queues[queue]; exists {
		return Queue{}, fmt.Errorf("queue already exists: %s", queue)
	}

	p, err := encodePrefix(queue)
	if err != nil {
		return Queue{}, err
	}

	e.queues[queue] = nil
	return Queue{
		name:     queue,
		prefix:   p,
		exchange: e,
	}, nil
}

// Publish publishes the specified message
// If no queues are bound to the topic then the message will be discarded
func (e *Exchange) Publish(topic string, value []byte, optFns ...func(*PublishOptions)) error {
	var o PublishOptions
	for _, fn := range optFns {
		fn(&o)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	wait := o.Wait || o.Delay > 0
	now := time.Now().UTC()

	wb := e.db.NewWriteBatch()
	defer wb.Cancel()

	dm := map[chan Delivery]Delivery{}

	for q := range e.topics[topic] {
		var due time.Time

		ch := e.queues[q]
		if wait || ch == nil {
			due = now.Add(o.Delay)
		} else {
			due = now.Add(e.opts.VisibilityTimeout)
		}

		k, err := NewKey(q, due)
		if err != nil {
			return err
		}

		if err = wb.Set(k, value); err != nil {
			return err
		}

		if ch != nil {
			dm[ch] = Delivery{
				Key:      k,
				Value:    value,
				exchange: e,
			}
		}
	}

	if err := wb.Flush(); err != nil {
		return err
	}

	if !wait {
		go func() {
			for ch, d := range dm {
				ch <- d
			}
		}()
	}

	return nil
}

// PurgeAll purges all messages in all queues
func (e *Exchange) PurgeAll() error {
	return e.db.DropAll()
}

// Close closes the exchange and the underlying database
func (e *Exchange) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	close(e.quit)
	e.wg.Wait()

	return e.db.Close()
}

func (e *Exchange) bind(topic string, q Queue) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if t, ok := e.topics[topic]; ok {
		t[q.name] = struct{}{}
	} else {
		e.topics[topic] = map[string]struct{}{
			q.name: {},
		}
	}
}

func (e *Exchange) consume(ctx context.Context, q Queue) (<-chan Delivery, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ch, exists := e.queues[q.name]; exists && ch != nil {
		return nil, fmt.Errorf("queue already consumed: %s", q.name)
	}

	ch := make(chan Delivery)
	e.queues[q.name] = ch

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		t := time.NewTicker(e.opts.PollInterval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-e.quit:
				return
			case <-t.C:
				ds, err := e.popBatch(q.prefix)
				if err != nil {
					e.opts.OnError(err)
					continue
				}

				for _, d := range ds {
					ch <- d
				}
			}
		}
	}()

	return ch, nil
}

func (e *Exchange) popBatch(prefix []byte) ([]Delivery, error) {
	var ds []Delivery

	err := e.db.Update(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var c int
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if !it.Valid() {
				break
			}

			i := it.Item()
			k := Key(i.KeyCopy(nil))

			if k.DueAt().After(time.Now().UTC()) {
				break
			}

			v, err := i.ValueCopy(nil)
			if err != nil {
				return err
			}

			dk := k.Delay(e.opts.VisibilityTimeout)
			if err := tx.Set(dk, v); err != nil {
				return err
			}

			if err := tx.Delete(k); err != nil {
				return err
			}

			ds = append(ds, Delivery{Key: dk, Value: v, exchange: e})

			c++
			if c >= e.opts.PollBatchSize {
				break
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return ds, nil
}

func (e *Exchange) delete(k Key) error {
	return e.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(k)
	})
}

func (e *Exchange) purge(prefix []byte) error {
	return e.db.DropPrefix(prefix)
}
