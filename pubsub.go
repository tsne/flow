package flow

import (
	"encoding/hex"
	"sync"
)

// PubSubHandler represents a handler for the plugged-in pub/sub system.
// The data can be assumed as valid only during the function call.
type PubSubHandler func(stream string, data []byte)

// Subscription defines an interface for an interest in a given stream.
type Subscription interface {
	Unsubscribe() error
}

// PubSub defines an interface for the pluggable pub/sub system.
//
// The Publish method describes the publishing side of the pub/sub
// system and is used to publish binary data to a specific stream
// (also known as topic).
// The passed bytes are only valid until the method returns. It is
// the responsibility of the pub/sub implementation to copy the data
// that should be reused after publishing.
//
// The Subscribe method describes the subscribing side of the pub/sub
// system and is used to subscribe to a given stream. The subscription
// has to support queue grouping, where a message is delivered to only
// one subscriber in a group. If the group parameter is empty, the
// messages should be sent to every subscriber.
// The bytes passed to the handler are only valid until the handler
// returns. It is the subscriber's responsibility to copy the data that
// should be reused.
type PubSub interface {
	Publish(stream string, data []byte) error
	Subscribe(stream, group string, h PubSubHandler) (Subscription, error)
}

type pubsub struct {
	PubSub
	onError   func(error)
	groupName string
	nodeName  string

	subsMtx sync.Mutex
	subs    map[string]Subscription
}

func newPubSub(ps PubSub, opts options) pubsub {
	return pubsub{
		PubSub:    ps,
		onError:   opts.errorHandler,
		groupName: opts.groupName,
		nodeName:  nodeName(opts.groupName, opts.nodeKey),
		subs:      make(map[string]Subscription),
	}
}

func (ps *pubsub) sendToGroup(frame frame) error {
	return ps.PubSub.Publish(ps.groupName, frame)
}

func (ps *pubsub) sendToNode(target key, frame frame) error {
	stream := nodeName(ps.groupName, target)
	return ps.PubSub.Publish(stream, frame)
}

func (ps *pubsub) publish(stream string, frame frame) error {
	return ps.PubSub.Publish(stream, frame)
}

func (ps *pubsub) subscribeGroup(h PubSubHandler) error {
	ps.subsMtx.Lock()
	defer ps.subsMtx.Unlock()

	_, hasGroupSub := ps.subs[ps.groupName]
	_, hasNodeSub := ps.subs[ps.nodeName]
	if hasGroupSub || hasNodeSub {
		return errorf("already subscribed to group '%s'", ps.groupName)
	}

	groupSub, err := ps.PubSub.Subscribe(ps.groupName, "", h)
	if err != nil {
		return err
	}

	nodeSub, err := ps.PubSub.Subscribe(ps.nodeName, "", h)
	if err != nil {
		ps.unsubscribe(ps.groupName, groupSub)
		return err
	}

	ps.subs[ps.groupName] = groupSub
	ps.subs[ps.nodeName] = nodeSub
	return nil
}

func (ps *pubsub) subscribe(stream string, h PubSubHandler) error {
	sub, err := ps.PubSub.Subscribe(stream, ps.groupName, h)
	if err != nil {
		return err
	}

	ps.subsMtx.Lock()
	_, has := ps.subs[stream]
	if !has {
		ps.subs[stream] = sub
	}
	ps.subsMtx.Unlock()

	if has {
		ps.unsubscribe(stream, sub)
		return errorf("already subscribed to '%s'", stream)
	}
	return nil
}

func (ps *pubsub) unsubscribe(stream string, sub Subscription) {
	if err := sub.Unsubscribe(); err != nil {
		ps.onError(errorf("error unsubscribing from '%s'", stream))
	}
}

func (ps *pubsub) shutdown() {
	ps.subsMtx.Lock()
	defer ps.subsMtx.Unlock()

	for stream, sub := range ps.subs {
		ps.unsubscribe(stream, sub)
	}
}

func nodeName(groupName string, nodeKey key) string {
	buf := alloc(len(groupName)+1+2*KeySize, nil)
	n := copy(buf, groupName)
	buf[n] = '.'
	hex.Encode(buf[n+1:], nodeKey)
	return string(buf)
}
