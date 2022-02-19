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
ex, err := emmq.Open("messages")
if err != nil {
    log.Fatal(err)
}
defer ex.Close()

ch, err := ex.Consume(context.Background(), "topic")
if err != nil {
    log.Fatal(err)
}

if err = ex.Publish("topic", []byte("value")); err != nil {
    log.Fatal(err)
}

d := <- ch
log.Print(string(d.Value))

if err = d.Ack(); err != nil {
    log.Fatal(err)
}
```

## Message delivery
All published messages are persisted prior to sending to consuming channels. If no channel has been configured to consume a specific topic prefix, then the message will be persisted in ready state. If a channel has been configured and the message is not delayed then it will be persisted in an unacked state and immediately sent to the consuming channel.

Publish options can be specified per-message using `WithDelay` or can be forced to wait for the next poll interval using `WithWait`.

Poll interval and batch size can be configured when opening the exchange using `WithPolling`.

Published messages can be consumed using a topic prefix by calling `Consume`. If a delivery is successfully processed, then `Ack` should be called to remove it. If a failure occurs then `Nack` should be called.

### Topic prefixes
Due to the underlying use of key prefixes to allow topics with BadgerDB, consume channels use a topic prefix. This means that if a channel consumes `package.` then it will receive messages for any topic with that prefix, for example `package.message`. In some scenarios this will result in unexpected behaviour. To avoid bugs, an error will be returned on calls to consume if the specified topic prefix would mask an existing channel.

Attempting to consume the same topic prefix more than once will also return an error. The rationale for this is that supporting competing consumers at a topic level without more complex routing would undermine the durability guarantees.

### Consume ordering
Messages are generally sent to consumer channels in FIFO order to nanosecond precision. If multiple messages are published within the same nanosecond then consume order will be random for those messages only. It is also possible for immediate published messages to be consumed prior to delayed messages in some scenarios, although this can be prevented by using `WithWait`.

## Failed messages
Messages are considered failed if they are either not acked, or are explicitly nacked, falling into 'unacked' and 'nacked' statuses respectively.

Failed messages are persisted indefinitely unless explicitly redriven or purged. Redriven messages will be made ready for consumption ahead of all other messages at the first poll interval. Purged messages are deleted.
```
if err := ex.Purge(emmq.StatusNacked); err != nil {
    log.Fatal(err)
}

if err := ex.Redrive(emmq.StatusUnacked); err != nil {
    log.Fatal(err)
}
```