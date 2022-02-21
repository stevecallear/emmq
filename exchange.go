package emmq

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type (
	// Exchange represents a message exchange
	Exchange struct {
		opts  Options
		db    *badger.DB
		chans map[string]chan Delivery
		quit  chan struct{}
		wg    *sync.WaitGroup
		mu    *sync.RWMutex
	}

	// Options represents message exchange options
	Options struct {
		PollInterval  time.Duration
		PollBatchSize int
		BadgerOptions badger.Options
		OnError       func(error)
	}

	// PublishOptions represents message publish options
	PublishOptions struct {
		Wait  bool
		Delay time.Duration
	}

	// RedriveOptions represents message redrive options
	RedriveOptions struct {
		TopicPrefix string
		BatchSize   int
		Offset      time.Duration
	}

	// Delivery represents a message delivery
	Delivery struct {
		Key      Key
		Value    []byte
		exchange *Exchange
	}
)

// Open opens a new message exchange with the specified options
func Open(path string, optFns ...func(*Options)) (*Exchange, error) {
	o := Options{
		PollInterval:  100 * time.Millisecond,
		PollBatchSize: 10,
		BadgerOptions: badger.DefaultOptions(path),
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
		opts:  o,
		db:    db,
		chans: map[string]chan Delivery{},
		quit:  make(chan struct{}),
		wg:    new(sync.WaitGroup),
		mu:    new(sync.RWMutex),
	}, nil
}

// Publish publishes the specified message
func (e *Exchange) Publish(topic string, value []byte, optFns ...func(*PublishOptions)) error {
	var o PublishOptions
	for _, fn := range optFns {
		fn(&o)
	}

	if o.Delay > 0 || o.Wait {
		return e.publishDelayed(topic, value, o.Delay)
	}

	return e.publishImmediate(topic, value)
}

func (e *Exchange) publishImmediate(topic string, value []byte) error {
	n := time.Now().UTC()

	e.mu.RLock()
	defer e.mu.RUnlock()

	ch, ok := e.chans[topic]
	if !ok {
		k := NewKey(topic, StatusReady, n)
		return e.db.Update(func(tx *badger.Txn) error {
			return tx.Set(k, value)
		})
	}

	k := NewKey(topic, StatusUnacked, n)
	err := e.db.Update(func(tx *badger.Txn) error {
		return tx.Set(k, value)
	})
	if err != nil {
		return err
	}

	go func() {
		ch <- Delivery{
			Key:      k,
			Value:    value,
			exchange: e,
		}
	}()

	return nil
}

func (e *Exchange) publishDelayed(topic string, value []byte, delay time.Duration) error {
	k := NewKey(topic, StatusReady, time.Now().UTC().Add(delay))
	return e.db.Update(func(tx *badger.Txn) error {
		return tx.Set(k, value)
	})
}

// Consume returns a delivery channel for the specified topic prefix
func (e *Exchange) Consume(ctx context.Context, topicPrefix string) (<-chan Delivery, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.chans[topicPrefix]; exists {
		return nil, fmt.Errorf("topic %s already consumed", topicPrefix)
	}

	// ensure that the topic would not mask another channel
	for k := range e.chans {
		if strings.HasPrefix(k, topicPrefix) {
			return nil, fmt.Errorf("topic %s would mask %s", topicPrefix, k)
		}
		if strings.HasPrefix(topicPrefix, k) {
			return nil, fmt.Errorf("topic %s would be masked by %s", topicPrefix, k)
		}
	}

	ch := make(chan Delivery)
	e.chans[topicPrefix] = ch

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()

		defer func() {
			e.mu.Lock()
			defer e.mu.Unlock()
			delete(e.chans, topicPrefix)
		}()

		t := time.NewTicker(e.opts.PollInterval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-e.quit:
				return
			case <-t.C:
				ds, err := e.pop(topicPrefix, e.opts.PollBatchSize)
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

func (e *Exchange) pop(topic string, count int) ([]Delivery, error) {
	var res []Delivery
	pre := encodePrefix(topic, StatusReady)

	err := e.db.Update(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var c int
		for it.Seek(pre); it.ValidForPrefix(pre); it.Next() {
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

			uk := k.withStatus(StatusUnacked)
			if err := tx.Set(uk, v); err != nil {
				return err
			}

			if err := tx.Delete(k); err != nil {
				return err
			}

			res = append(res, Delivery{
				Key:      uk,
				Value:    v,
				exchange: e,
			})

			c++
			if c >= count {
				break
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Redrive redrives all messages of the specified status(es)
func (e *Exchange) Redrive(s Status, optFns ...func(*RedriveOptions)) error {
	o := RedriveOptions{BatchSize: e.opts.PollBatchSize}
	for _, fn := range optFns {
		fn(&o)
	}

	for _, cs := range []Status{StatusUnacked, StatusNacked} {
		if hasStatus(s, cs) {
			if err := e.redriveStatus(cs, o); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *Exchange) redriveStatus(s Status, o RedriveOptions) error {
	pre := encodePrefix(o.TopicPrefix, s)
	now := time.Now().UTC()

	for {
		var more bool
		err := e.db.Update(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			var c int
			for it.Seek(pre); it.ValidForPrefix(pre); it.Next() {
				if !it.Valid() {
					break
				}

				ii := it.Item()
				k := Key(ii.KeyCopy(nil))

				if o.Offset > 0 && k.DueAt().Add(o.Offset).After(now) {
					break
				}

				v, err := ii.ValueCopy(nil)
				if err != nil {
					return err
				}

				nk := k.withStatus(StatusReady)
				if err := tx.Set(nk, v); err != nil {
					return err
				}

				if err := tx.Delete(k); err != nil {
					return err
				}

				c++
				if c >= o.BatchSize {
					more = true
					break
				}
			}

			return nil
		})
		if err != nil {
			return err
		}

		if !more {
			break
		}
	}

	return nil
}

// Purge purges all messages of the specified status(es)
func (e *Exchange) Purge(s Status) error {
	ps := [][]byte{}
	for _, cs := range []Status{StatusReady, StatusUnacked, StatusNacked} {
		if hasStatus(s, cs) {
			ps = append(ps, []byte{byte(cs)})
		}
	}

	if len(ps) < 1 {
		return nil
	}

	return e.db.DropPrefix(ps...)
}

// Close closes the exchange and the underlying database
func (e *Exchange) Close() error {
	close(e.quit)
	e.wg.Wait()

	return e.db.Close()
}

func (e *Exchange) ack(k Key) error {
	return e.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(k)
	})
}

func (e *Exchange) nack(k Key) error {
	nk := k.withStatus(StatusNacked)
	return e.db.Update(func(tx *badger.Txn) error {
		i, err := tx.Get(k)
		if err != nil {
			return err
		}

		v, err := i.ValueCopy(nil)
		if err != nil {
			return err
		}

		if err = tx.Set(nk, v); err != nil {
			return err
		}

		return tx.Delete(k)
	})
}

// Ack acknowledges the delivery
// Acked messages cannot be redriven or purged
func (d *Delivery) Ack() error {
	return d.exchange.ack(d.Key)
}

// Nack marks the delivery as nacked
// Nacked messages can be redriven or purged
func (d *Delivery) Nack() error {
	return d.exchange.nack(d.Key)
}

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
