package flow

import "sync"

// PubSubHandler represents a handler for the plugged-in pub/sub system.
type PubSubHandler func(stream string, data []byte)

// PubSub defines an interface for the pluggable pub/sub system.
//
// The Publish method describes the publishing side of the pub/sub
// system and is used to publish binary data to a specific stream
// (also known as topic).
//
// The Subscribe and Unsubscribe methods define the subscribing side
// of the pub/sub system and are used to subscribe to and unsubscribe
// from a stream respectively. The subscription has to support queue
// grouping, where a message is delivered to only one subscriber in
// a group.
type PubSub interface {
	Publish(stream string, data []byte) error
	Subscribe(stream, group string, h PubSubHandler) error
	Unsubscribe(stream string) error
}

//
// The subscriber has to support grouping, where only one
// subscribed handler is called within a single group.
type Subscriber interface {
}

type pubsub struct {
	PubSub
	groupStream string
	nodeStream  string

	subsMtx sync.Mutex
	subs    map[string]struct{}
}

func newPubSub(ps PubSub, opts options) pubsub {
	return pubsub{
		PubSub:      ps,
		groupStream: opts.groupName,
		nodeStream:  opts.groupName + "." + opts.nodeKey.String(),
		subs:        make(map[string]struct{}),
	}
}

func (ps *pubsub) sendToGroup(msg message) error {
	if err := ps.PubSub.Publish(ps.groupStream, msg); err != nil {
		logf("group publish error: %v", err)
		return err
	}
	return nil
}

func (ps *pubsub) sendToNode(target key, msg message) error {
	stream := ps.groupStream + "." + target.String()
	if err := ps.PubSub.Publish(stream, msg); err != nil {
		logf("node send error: %v", err)
		return err
	}
	return nil
}

func (ps *pubsub) publish(stream string, msg message) error {
	return ps.PubSub.Publish(stream, msg)
}

func (ps *pubsub) subscribeGroup(h PubSubHandler) error {
	if err := ps.PubSub.Subscribe(ps.groupStream, "", h); err != nil {
		return err
	}
	if err := ps.PubSub.Subscribe(ps.nodeStream, "", h); err != nil {
		ps.PubSub.Unsubscribe(ps.groupStream)
		return err
	}
	return nil
}

func (ps *pubsub) subscribe(stream string, h PubSubHandler) error {
	if err := ps.PubSub.Subscribe(stream, ps.groupStream, h); err != nil {
		return err
	}
	ps.subsMtx.Lock()
	ps.subs[stream] = struct{}{}
	ps.subsMtx.Unlock()
	return nil
}

func (ps *pubsub) shutdown() {
	ps.subsMtx.Lock()
	for stream := range ps.subs {
		ps.PubSub.Unsubscribe(stream)
	}
	ps.subsMtx.Unlock()

	ps.PubSub.Unsubscribe(ps.nodeStream)
	ps.PubSub.Unsubscribe(ps.groupStream)
}
