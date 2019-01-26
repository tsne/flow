# flow
`flow` is a small message streaming framework for the Go programming language. It leverages a pluggable Pub/Sub system to distribute messages and to intercommunicate with other nodes.

## Architecture Overview
Each node in the system is represented by a [`Broker`](https://godoc.org/github.com/tsne/flow#Broker), which can publish messages to and subscribe messages from the underlying Pub/Sub system. A broker is assigned to an individual group and processes subscribed messages within a particular partition within this group. If no group is assigned manually, the broker will be automatically assigned to a global default group.

All brokers within a single group are arranged in a ring and communicate with each other. Whenever a broker joins or leaves the group, a message will be published to inform the other group members about the event. With this information each broker is able to build its own local view of the ring to forward messages to the responsible broker. So, once the ring structure is established, each broker always processes messages within the same partition key range using [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing). The broker's partition key range is determined by its node key, which can be assigned via the [`NodeKey`](https://godoc.org/github.com/tsne/flow#NodeKey) option. By default a random node key will be generated for each broker. In case a broker dies unexpectedly, the other group members will get timeouts during the communication and suspect this broker to be faulty. Therefore, dead brokers will eventually be removed from the ring.

## Message Encoding
In order to be interoperable with existing message streams, a broker needs to know how to encode messages to and decode messages from binary data. Therefore, the [`MessageCodec`](https://godoc.org/github.com/tsne/flow#MessageCodec) option can be used to introduce a custom codec. This is useful to ensure compatibility with existing stream publishers and subscribers, that use different binary message formats. On the other hand, `flow` provides an internal codec, which is implemented by the [`DefaultCodec`](https://godoc.org/github.com/tsne/flow#DefaultCodec), and is set by default. If there are no compatibility issues, this internal codec should suffice.

## Delivery Guarantees
The delivery guarantee of `flow` depends heavily on the delivery guarantee of the underlying pub/sub system. This plays a major role for message delivery between groups. For group internal message routing the message delivery is at-least-once, since the routed messages have to be acknowledged by the target broker.

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
		flow.Group("<group-name>"),
		flow.NodeKey(flow.StringKey("<node-name>")),
	)
	if err != nil {
		log.Fatal("error creating broker: %v", err)
	}
	defer b.Close()

	// subscribe messages
	err = b.Subscribe(ctx, stream, func(_ context.Context, msg flow.Message) {
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
		err := b.Publish(ctx, flow.Message{
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
