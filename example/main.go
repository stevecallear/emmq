package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/stevecallear/emmq"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	e, err := emmq.Open("emmq", emmq.WithPolling(100*time.Millisecond, 100))
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	go consume(ctx, e, "topic")
	go publish(ctx, e, "topic")

	<-ctx.Done()
}

func consume(ctx context.Context, e *emmq.Exchange, topic string) {
	q, err := e.Declare("queue")
	if err != nil {
		log.Fatal(err)
	}

	q.Bind(topic)

	c, err := q.Consume(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case d := <-c:
			log.Printf("received: %s", d.Value)
			if err := d.Delete(); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func publish(ctx context.Context, e *emmq.Exchange, topic string) {
	var t *time.Timer
	msgs := []string{"foo", "bar", "baz"}

	for {
		ms := rand.Intn(500) + 50
		t = time.NewTimer(time.Duration(ms) * time.Millisecond)

		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m := msgs[rand.Intn(len(msgs))]
			if err := e.Publish(topic, []byte(m)); err != nil {
				log.Fatal(err)
			}
		}
	}
}
