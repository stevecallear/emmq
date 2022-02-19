package main

import (
	"context"
	"log"

	"github.com/stevecallear/emmq"
)

func main() {
	ex, err := emmq.Open("db")
	if err != nil {
		log.Fatal(err)
	}
	defer ex.Close()

	ch1, err := ex.Consume(context.Background(), "package.")
	if err != nil {
		log.Fatal(err)
	}

	ch2, err := ex.Consume(context.Background(), "package.message")
	if err != nil {
		log.Fatal(err)
	}

	err = ex.Publish("package.message", []byte("hello"))
	if err != nil {
		log.Fatal(err)
	}

	d := <-ch1
	d = <-ch2
	log.Println(string(d.Value))
}
