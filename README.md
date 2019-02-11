# flow
`flow` is a small message streaming framework for the Go programming language. It leverages a pluggable Pub/Sub system to distribute messages and to intercommunicate with other nodes.

## Architecture Overview
Each node in the system is represented by a [`Broker`](https://godoc.org/github.com/tsne/flow#Broker), which can publish messages to and subscribe to a message stream from the underlying Pub/Sub system. A broker is assigned to an individual clique and processes messages for particular partitions within this clique. A clique is a group of related nodes. In case a broker is not assigned to a clique manually, the broker will be automatically assigned to a global default clique.

All brokers within a single clique are arranged in a ring structure and communicate with each other. Whenever a broker joins or leaves the clique, a message will be published to inform the other clique members about the event. Furthermore, each broker performs a regular stabilization step, where known nodes are verified to be alive and clique information is gathered. With all this information each broker is able to build its own local view of the ring to forward messages to the responsible broker. So, once the ring structure is established, each broker always processes messages within the same partitions using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). The broker's partitions is determined by its node key, which can be assigned via the [`WithPartition`](https://godoc.org/github.com/tsne/flow#WithPartition) option. By default a random node key will be generated for each broker. In case a broker dies unexpectedly, the other clique members will get timeouts during the communication and suspect this broker to be faulty. Therefore, dead brokers will eventually be removed from the ring.

## Message Encoding
In order to be interoperable with existing message streams, a broker needs to know how to encode messages to and decode messages from binary data. Therefore, the [`MessageCodec`](https://godoc.org/github.com/tsne/flow#MessageCodec) option can be used to introduce a custom codec. This is useful to ensure compatibility with existing stream publishers and subscribers, that use different binary message formats. On the other hand, `flow` provides an internal codec, which is implemented by the [`DefaultCodec`](https://godoc.org/github.com/tsne/flow#DefaultCodec), and is set by default. If there are no compatibility issues, this internal codec should suffice.

## Delivery Guarantees
The delivery guarantee of `flow` depends heavily on the delivery guarantee of the underlying pub/sub system. This plays a major role for message delivery between cliques. For clique internal message routing the message delivery is at-least-once, since the routed messages have to be acknowledged by the target broker.

## Pub/Sub Requirements
The underlying pub/sub system needs to support queue grouping, i.e. a message is delivered to only one subscriber in a queue group.

## Example
```golang
package main

import (
	"context"
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

	ctx := context.Background()
	b, err := flow.NewBroker(ctx, pubsub,
		flow.WithPartition("<clique-name>", flow.StringKey("<node-name>")),
		flow.WithMessageHandler(stream, func(_ context.Context, msg flow.Message) {
			tm := time.Now().Format("2006-01-02 15:04:05 MST")
			fmt.Printf("message@%s: %s\n", tm, msg.Data)
		}),
	)
	if err != nil {
		log.Fatal("error creating broker: %v", err)
	}
	defer b.Close()

	// publish messages
	var count int
	for range time.Tick(time.Second) {
		count++
		err := b.Publish(ctx, flow.Message{
			Stream: stream,
			Data:   []byte(fmt.Sprintf("hello flow %d", count)),
		})
		if err != nil {
			log.Printf("publish error: %v", err)
		}
	}
}
```
