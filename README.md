# flow
`flow` is a small message streaming framework for the Go programming language. It leverages a pluggable Pub/Sub system to distribute messages and to intercommunicate with other nodes. Furthermore, all published messages can be persisted using a custom storage engine.

## Architecture Overview
Each node in the system is represented by a [`Broker`](https://godoc.org/github.com/tsne/flow#Broker), which can publish messages to and subscribe messages from the underlying Pub/Sub system. A broker is assigned to an individual group and processes subscribed messages within a particular partition. If no group is assigned manually, the broker will be automatically assigned to a global default group.

All brokers within a single group are arranged in a ring and communicate with each other. Whenever a broker joins or leaves the group, a message will be published to inform the other group members about the event. With this information each broker is able to build its own local view of the ring to route messages to the responsible broker. So, once the ring structure is established, each broker always processes messages within the same partition key range using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). The broker's partition key range is determined by its assigned node key. If no node key is assigned manually, a random node key will be generated for this broker. If a broker dies unexpectedly, the other group members will get timeouts during the communication and suspect this broker to be faulty. Therefore, dead brokers will eventually be removed from the ring.


## Persisting Messages
It is possible for a broker to persist messages before publishing them to the Pub/Sub system. This can be achieved by a pluggable storage engine using the [`Storage`](https://godoc.org/github.com/tsne/flow#Storage) option. By default, messages from all streams will be persisted. It is also possible to decide which streams should be persisted using the [`StorageFilter`](https://godoc.org/github.com/tsne/flow#StorageFilter) option.

Be aware, that the persistence and the publishing of a message is not transaction safe. If the message was persisted successfully and an error occurs during the publishing step, there will be no rollback. In case of a retry, the storage engine is therefore requested to persist the same message again which could result in duplicates or unique violation errors. To avoid this scenario the storage engine could, for example, ignore duplicate messages. On the other hand, if the storage engine returns an error persisting a message, the error will immediatedly be reported by the broker without publishing this message.

## Delivery Guarantees
The delivery guarantee of `flow` depends heavily on the delivery guarantee of the underlying pub/sub system. This plays a major role for message delivery between groups. For group internal message routing the message delivery is at-least-once, since the routed messages have to be acknowledged by the target broker.

## Pub/Sub Requirements
The underlying pub/sub needs to support queue grouping, i.e. a message is delivered to only one subscriber in a queue group.

## Example
```golang
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/tsne/flow"
	"github.com/tsne/flow/nats"
)

func main() {
	const stream = "mystream"

	pubsub, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatalf("error connecting to pubsub: %v", err)
	}
	defer pubsub.Close()

	b, err := flow.NewBroker(pubsub,
		flow.Group("<group-name>"),
		flow.NodeKey(flow.StringKey("<node-name>")),
	)
	if err != nil {
		log.Fatal("error creating broker: %v", err)
	}
	defer b.Close()

	// subscribe messages
	err = b.Subscribe(stream, func(msg *flow.Message) {
		tm := msg.Time.Format("2006-01-02 15:04:05 MST")
		fmt.Printf("message@%s: %s\n", tm, msg.Data)
	})
	if err != nil {
		log.Printf("subscribe error: %v", err)
	}

	// publish messages
	var count int
	for tm := range time.Tick(time.Second) {
		count++
		err := b.Publish(flow.Message{
			Stream: stream,
			Time:   tm,
			Data:   []byte(fmt.Sprintf("hello flow %d", count)),
		})
		if err != nil {
			log.Printf("publish error: %v", err)
		}
	}
}
```
