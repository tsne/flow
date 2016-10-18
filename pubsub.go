package flow

import "sync"

// PubSubHandler represents a handler for the pub/sub system.
type PubSubHandler func(stream string, data []byte)

// Publisher defines an interface for publishing binary data.
// It is used as the publishing side of the pub/sub system.
type Publisher interface {
	Publish(stream string, data []byte) error
}

// Subscriber defines an interface for subscribing to and
// unsubscribing from a stream. It is used as the subscribing
// side of the pub/sub system.
//
// The subscriber has to support grouping, where only one
// subscribed handler is called within a single group.
type Subscriber interface {
	Subscribe(stream, group string, h PubSubHandler) error
	Unsubscribe(stream string) error
}

type pubsub struct {
	pub         Publisher
	sub         Subscriber
	groupStream string
	nodeStream  string

	subsMtx sync.Mutex
	subs    map[string]struct{}
}

func newPubSub(pub Publisher, sub Subscriber, opts options) pubsub {
	return pubsub{
		pub:         pub,
		sub:         sub,
		groupStream: opts.groupName,
		nodeStream:  opts.groupName + "." + opts.nodeKey.String(),
		subs:        make(map[string]struct{}),
	}
}

func (ps *pubsub) sendToGroup(msg message) error {
	if err := ps.pub.Publish(ps.groupStream, msg); err != nil {
		logf("group publish error: %v", err)
		return err
	}
	return nil
}

func (ps *pubsub) sendToNode(target key, msg message) error {
	stream := ps.groupStream + "." + target.String()
	if err := ps.pub.Publish(stream, msg); err != nil {
		logf("node send error: %v", err)
		return err
	}
	return nil
}

func (ps *pubsub) publish(stream string, msg message) error {
	return ps.pub.Publish(stream, msg)
}

func (ps *pubsub) subscribeGroup(h PubSubHandler) error {
	if err := ps.sub.Subscribe(ps.groupStream, "", h); err != nil {
		return err
	}
	if err := ps.sub.Subscribe(ps.nodeStream, "", h); err != nil {
		ps.sub.Unsubscribe(ps.nodeStream)
		return err
	}
	return nil
}

func (ps *pubsub) subscribe(stream string, h PubSubHandler) error {
	if err := ps.sub.Subscribe(stream, ps.groupStream, h); err != nil {
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
		ps.sub.Unsubscribe(stream)
	}
	ps.subsMtx.Unlock()

	ps.sub.Unsubscribe(ps.nodeStream)
	ps.sub.Unsubscribe(ps.groupStream)
}
