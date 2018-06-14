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
	ps          PubSub
	onError     func(error)
	groupStream string
	nodeStream  string

	subsMtx sync.Mutex
	subs    map[string]Subscription
}

func newPubSub(ps PubSub, opts options) pubsub {
	return pubsub{
		ps:          ps,
		onError:     opts.errorHandler,
		groupStream: opts.groupName,
		nodeStream:  nodeStream(opts.groupName, opts.nodeKey),
		subs:        make(map[string]Subscription),
	}
}

func (ps *pubsub) sendToGroup(data []byte) error {
	return ps.ps.Publish(ps.groupStream, data)
}

func (ps *pubsub) sendToNode(target key, data []byte) error {
	stream := nodeStream(ps.groupStream, target)
	return ps.ps.Publish(stream, data)
}

func (ps *pubsub) publish(stream string, data []byte) error {
	return ps.ps.Publish(stream, data)
}

func (ps *pubsub) subscribeGroup(h PubSubHandler) error {
	ps.subsMtx.Lock()
	defer ps.subsMtx.Unlock()

	_, hasGroupSub := ps.subs[ps.groupStream]
	_, hasNodeSub := ps.subs[ps.nodeStream]
	if hasGroupSub || hasNodeSub {
		return errorf("already subscribed to group '%s'", ps.groupStream)
	}

	groupSub, err := ps.ps.Subscribe(ps.groupStream, "", h)
	if err != nil {
		return err
	}

	nodeSub, err := ps.ps.Subscribe(ps.nodeStream, "", h)
	if err != nil {
		ps.unsubscribe(ps.groupStream, groupSub)
		return err
	}

	ps.subs[ps.groupStream] = groupSub
	ps.subs[ps.nodeStream] = nodeSub
	return nil
}

func (ps *pubsub) subscribe(stream string, h PubSubHandler) error {
	ps.subsMtx.Lock()
	defer ps.subsMtx.Unlock()

	if _, has := ps.subs[stream]; has {
		return errorf("already subscribed to '%s'", stream)
	}

	sub, err := ps.ps.Subscribe(stream, ps.groupStream, h)
	if err != nil {
		return err
	}

	ps.subs[stream] = sub
	return nil
}

func (ps *pubsub) unsubscribe(stream string, sub Subscription) {
	if err := sub.Unsubscribe(); err != nil {
		ps.onError(errorf("error unsubscribing from '%s'", stream))
	}
}

func (ps *pubsub) unsubscribeAll() {
	ps.subsMtx.Lock()
	defer ps.subsMtx.Unlock()

	for stream, sub := range ps.subs {
		ps.unsubscribe(stream, sub)
	}
}

func nodeStream(groupName string, nodeKey key) string {
	buf := alloc(len(groupName)+1+2*KeySize, nil)
	n := copy(buf, groupName)
	buf[n] = '.'
	hex.Encode(buf[n+1:], nodeKey)
	return string(buf)
}
