# emmq
[![Build Status](https://github.com/stevecallear/emmq/actions/workflows/build.yml/badge.svg)](https://github.com/stevecallear/emmq/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/stevecallear/emmq/branch/master/graph/badge.svg)](https://codecov.io/gh/stevecallear/emmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/stevecallear/emmq)](https://goreportcard.com/report/github.com/stevecallear/emmq)

`emmq` is an embedded message queue backed by [BadgerDB](https://dgraph.io/docs/badger/). It focusses on durability of messages in favour of strict ordering, but will generally achieve FIFO behaviour with a couple of caveats noted below.

The package was built to provide a simple message queue for embedded applications and as a learning exercise for BadgerDB.

## Getting started
```
go get github.com/stevecallear/emmq@latest
```
```
e, err := emmq.Open("messages")
if err != nil {
    log.Fatal(err)
}
defer e.Close()

q, err := e.Declare("queue")
if err != nil {
    log.Fatal(err)
}

q.Bind("topic")

c, err := q.Consume(context.Background())
if err != nil {
    log.Fatal(err)
}

if err = e.Publish("topic", []byte("value")); err != nil {
    log.Fatal(err)
}

d := <-c
log.Print(string(d.Value))

if err = d.Delete(); err != nil {
    log.Fatal(err)
}
```

## Message delivery
Messages are published for a specific topic. Each topic can be bound to multiple queues and a single queue can be bound to multiple topics. If no queues are bound to a specific topic, then published messages will be discarded.

All published messages are persisted prior to sending to consuming channels. If bound queues have not been configured to consume messages, then they will be persisted for immediate consumption. If bound queues have been configured to consume messages then they will be persisted with the configured visibility timeout and immediately sent to the consuming channel.

Messages can be delayed by using `WithDelay` or can be forced to wait for the next poll interval using `WithWait`.

Delivered messages that have been processed should be deleted by calling `Delete`. If the message is not deleted then it will be delivered again once the visibility timeout has expired.

### Delivery order
Messages are generally sent to consumer channels in FIFO order to nanosecond precision. If multiple messages are published within the same nanosecond then consume order will be random for those messages only.