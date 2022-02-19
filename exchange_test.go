package emmq_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync/atomic"
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
	p := uniquePath()
	defer func() {
		if err := os.RemoveAll(p); err != nil {
			panic(err)
		}
	}()

	e, err := emmq.Open(p, emmq.WithPolling(100*time.Millisecond, 10))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := e.Close(); err != nil {
			panic(err)
		}
	}()

	sut = e
	m.Run()
}

func TestExchange_Publish(t *testing.T) {
	exp := []byte("value")

	tests := []struct {
		name        string
		opts        func(*emmq.PublishOptions)
		postConsume bool
		assert      func(*testing.T, string, <-chan emmq.Delivery)
	}{
		{
			name: "should publish immediate messages",
			opts: func(*emmq.PublishOptions) {},
			assert: func(t *testing.T, topic string, c <-chan emmq.Delivery) {
				assertSome(t, c, 1, func(d emmq.Delivery) {
					if act := d.Value; !bytes.Equal(act, exp) {
						t.Errorf("got %v, expected %v", act, exp)
					}
				})
			},
		},
		{
			name: "should publish immediate wait messages",
			opts: emmq.WithWait(),
			assert: func(t *testing.T, topic string, c <-chan emmq.Delivery) {
				assertSome(t, c, 1, func(d emmq.Delivery) {
					if act := d.Value; !bytes.Equal(act, exp) {
						t.Errorf("got %v, expected %v", act, exp)
					}
				})
			},
		},
		{
			name:        "should publish immediate messages with post consume",
			opts:        func(*emmq.PublishOptions) {},
			postConsume: true,
			assert: func(t *testing.T, topic string, c <-chan emmq.Delivery) {
				assertSome(t, c, 1, func(d emmq.Delivery) {
					if act := d.Value; !bytes.Equal(act, exp) {
						t.Errorf("got %v, expected %v", act, exp)
					}
				})
			},
		},
		{
			name: "should publish delayed messages",
			opts: emmq.WithDelay(2 * pollInterval),
			assert: func(t *testing.T, topic string, c <-chan emmq.Delivery) {
				assertSome(t, c, 1, func(d emmq.Delivery) {
					if act := d.Value; !bytes.Equal(act, exp) {
						t.Errorf("got %v, expected %v", act, exp)
					}
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			topic := uuid.NewString()

			var ch <-chan emmq.Delivery
			var err error

			if !tt.postConsume {
				ch, err = sut.Consume(ctx, topic)
				if err != nil {
					t.Fatal(err)
				}
			}

			if err = sut.Publish(topic, exp, tt.opts); err != nil {
				t.Fatal(err)
			}

			if tt.postConsume {
				ch, err = sut.Consume(ctx, topic)
				if err != nil {
					t.Fatal(err)
				}
			}

			tt.assert(t, topic, ch)
		})
	}
}

func TestExchange_Consume(t *testing.T) {
	tests := []struct {
		name   string
		topics func() (string, string)
	}{
		{
			name: "should return an error if the topic has already been consumed",
			topics: func() (string, string) {
				t := uuid.NewString()
				return t, t
			},
		},
		{
			name: "should return an error if the topic prefix would mask an existing channel",
			topics: func() (string, string) {
				p := "p1."
				t := uuid.NewString()
				return p + t, p
			},
		},

		{
			name: "should return an error if the topic prefix would be masked by an existing channel",
			topics: func() (string, string) {
				p := "p2."
				t := uuid.NewString()
				return p, p + t
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t1, t2 := tt.topics()

			_, err := sut.Consume(context.Background(), t1)
			if err != nil {
				t.Fatal(err)
			}

			_, err = sut.Consume(context.Background(), t2)
			if err == nil {
				t.Error("got nil, expected an error")
			}
		})
	}
}

func TestExchange_Redrive(t *testing.T) {
	const messageCount = 30

	tests := []struct {
		name   string
		status emmq.Status
		exp    int32
	}{
		{
			name:   "should not error on ready status",
			status: emmq.StatusReady,
		},
		{
			name:   "should redrive unacked status",
			status: emmq.StatusUnacked,
			exp:    messageCount / 2,
		},
		{
			name:   "should redrive nacked status",
			status: emmq.StatusNacked,
			exp:    messageCount / 2,
		},
		{
			name:   "should redrive unacked and nacked status",
			status: emmq.StatusUnacked | emmq.StatusNacked,
			exp:    messageCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			topic := uuid.NewString()

			ch, err := sut.Consume(ctx, topic)
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < messageCount; i++ {
				if err := sut.Publish(topic, []byte{}); err != nil {
					t.Fatal(err)
				}
			}

			assertSome(t, ch, messageCount/2)

			assertSome(t, ch, messageCount/2, func(d emmq.Delivery) {
				if err := d.Nack(); err != nil {
					t.Fatal(err)
				}
			})

			if err = sut.Redrive(tt.status); err != nil {
				t.Fatal(err)
			}

			if tt.exp < 1 {
				assertNone(t, ch, 2*pollInterval)
			} else {
				assertSome(t, ch, tt.exp, func(d emmq.Delivery) {
					if err := d.Ack(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestExchange_Purge(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*testing.T, <-chan emmq.Delivery, string)
		status emmq.Status
		assert func(*testing.T, <-chan emmq.Delivery, string)
	}{
		{
			name: "should do nothing on invalid status",
			setup: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				for i := 0; i < 15; i++ {
					err := sut.Publish(topic, []byte{}, emmq.WithDelay(2*pollInterval))
					if err != nil {
						t.Fatal(err)
					}
				}
			},
			status: emmq.Status(1 << 4),
			assert: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				assertSome(t, ch, 15)
			},
		},
		{
			name: "should purge ready status",
			setup: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				for i := 0; i < 15; i++ {
					err := sut.Publish(topic, []byte{}, emmq.WithDelay(2*pollInterval))
					if err != nil {
						t.Fatal(err)
					}
				}
			},
			status: emmq.StatusReady,
			assert: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				assertNone(t, ch, 4*pollInterval)
			},
		},
		{
			name: "should purge unacked status",
			setup: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				for i := 0; i < 15; i++ {
					if err := sut.Publish(topic, []byte{}); err != nil {
						t.Fatal(err)
					}
				}

				assertSome(t, ch, 15)
			},
			status: emmq.StatusUnacked,
			assert: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				if err := sut.Redrive(emmq.StatusUnacked); err != nil {
					t.Fatal(err)
				}

				assertNone(t, ch, 2*pollInterval)
			},
		},
		{
			name: "should purge nacked status",
			setup: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				for i := 0; i < 15; i++ {
					if err := sut.Publish(topic, []byte{}); err != nil {
						t.Fatal(err)
					}
				}

				assertSome(t, ch, 15, func(d emmq.Delivery) {
					if err := d.Nack(); err != nil {
						t.Fatal(err)
					}
				})
			},
			status: emmq.StatusNacked,
			assert: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				if err := sut.Redrive(emmq.StatusNacked); err != nil {
					t.Fatal(err)
				}

				assertNone(t, ch, 2*pollInterval)
			},
		},
		{
			name: "should purge unacked and nacked status",
			setup: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				for i := 0; i < 30; i++ {
					if err := sut.Publish(topic, []byte{}); err != nil {
						t.Fatal(err)
					}
				}

				assertSome(t, ch, 15)

				assertSome(t, ch, 15, func(d emmq.Delivery) {
					if err := d.Nack(); err != nil {
						t.Fatal(err)
					}
				})
			},
			status: emmq.StatusUnacked | emmq.StatusNacked,
			assert: func(t *testing.T, ch <-chan emmq.Delivery, topic string) {
				if err := sut.Redrive(emmq.StatusUnacked | emmq.StatusNacked); err != nil {
					t.Fatal(err)
				}

				assertNone(t, ch, 2*pollInterval)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			topic := uuid.NewString()

			ch, err := sut.Consume(ctx, topic)
			if err != nil {
				t.Fatal(err)
			}

			tt.setup(t, ch, topic)

			if err := sut.Purge(tt.status); err != nil {
				t.Fatal(err)
			}

			tt.assert(t, ch, topic)
		})
	}
}

func TestExchange_Close(t *testing.T) {
	t.Run("should close the exchange", func(t *testing.T) {
		p := uniquePath()
		defer func() {
			if err := os.RemoveAll(p); err != nil {
				t.Fatal(err)
			}
		}()

		e, err := emmq.Open(p)
		if err != nil {
			t.Fatal(err)
		}

		_, err = e.Consume(context.Background(), "topic")
		if err != nil {
			t.Fatal(err)
		}

		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWithPolling(t *testing.T) {
	t.Run("should configure polling", func(t *testing.T) {
		var o emmq.Options
		emmq.WithPolling(1*time.Second, 10)(&o)

		if act, exp := o.PollInterval, 1*time.Second; act != exp {
			t.Errorf("got %v, expected %v", act, exp)
		}

		if act, exp := o.PollBatchSize, 10; act != exp {
			t.Errorf("got %d, expected %d", act, exp)
		}
	})
}

func assertSome(t *testing.T, ch <-chan emmq.Delivery, n int32, fns ...func(emmq.Delivery)) {
	q := make(chan struct{})

	var c int32
	go func() {
		defer close(q)
		for {
			d := <-ch

			ct := atomic.AddInt32(&c, 1)
			for _, fn := range fns {
				fn(d)
			}

			if ct >= n {
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

func assertNone(t *testing.T, ch <-chan emmq.Delivery, wait time.Duration) {
	dt := time.NewTimer(wait)
	for {
		select {
		case <-ch:
			t.Error("got delivery, expected none")
			return
		case <-dt.C:
			return
		}
	}
}

func uniquePath() string {
	return "test-" + strings.ReplaceAll(uuid.NewString(), "-", "")
}
