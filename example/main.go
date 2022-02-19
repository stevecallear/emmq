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

const (
	topic  = "topic"
	dbPath = "messages"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ex, err := emmq.Open(dbPath, emmq.WithPolling(100*time.Millisecond, 100))
	if err != nil {
		log.Fatal(err)
	}
	defer ex.Close()

	// redrive all messages that have not been acked
	if err := ex.Redrive(emmq.StatusUnacked); err != nil {
		log.Fatal(err)
	}

	// purge all messages that have been explicitly nacked
	if err := ex.Purge(emmq.StatusNacked); err != nil {
		log.Fatal(err)
	}

	go consume(ctx, ex)
	go publish(ctx, ex)

	<-ctx.Done()
}

func consume(ctx context.Context, ex *emmq.Exchange) {
	ch, err := ex.Consume(ctx, topic)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case d := <-ch:
			log.Printf("received: %s", d.Value)
			if err := d.Ack(); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func publish(ctx context.Context, ex *emmq.Exchange) {
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
			if err := ex.Publish(topic, []byte(m)); err != nil {
				log.Fatal(err)
			}
		}
	}
}
