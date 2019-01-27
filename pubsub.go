package flow

import (
	"context"
	"encoding/hex"
)

// PubSubHandler represents a handler for the plugged-in pub/sub system.
// The data can be assumed as valid only during the function call.
type PubSubHandler func(ctx context.Context, stream string, data []byte)

// Subscription defines an interface for an interest in a given stream.
type Subscription interface {
	Unsubscribe(ctx context.Context) error
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
	Publish(ctx context.Context, stream string, data []byte) error
	Subscribe(ctx context.Context, stream, group string, h PubSubHandler) (Subscription, error)
}

type pubsub struct {
	ps           PubSub
	onError      func(error)
	cliqueStream string
	nodeStream   string
	subs         map[string]Subscription
}

func newPubSub(ps PubSub, opts options) pubsub {
	return pubsub{
		ps:           ps,
		onError:      opts.errorHandler,
		cliqueStream: opts.cliqueName,
		nodeStream:   nodeStream(opts.cliqueName, opts.nodeKey),
		subs:         make(map[string]Subscription),
	}
}

func (ps *pubsub) sendToClique(ctx context.Context, data []byte) error {
	return ps.ps.Publish(ctx, ps.cliqueStream, data)
}

func (ps *pubsub) sendToNode(ctx context.Context, target key, data []byte) error {
	stream := nodeStream(ps.cliqueStream, target)
	return ps.ps.Publish(ctx, stream, data)
}

func (ps *pubsub) publish(ctx context.Context, stream string, data []byte) error {
	return ps.ps.Publish(ctx, stream, data)
}

func (ps *pubsub) subscribeClique(ctx context.Context, h PubSubHandler) error {
	_, hasCliqueSub := ps.subs[ps.cliqueStream]
	_, hasNodeSub := ps.subs[ps.nodeStream]
	if hasCliqueSub || hasNodeSub {
		return errorf("already subscribed to clique '%s'", ps.cliqueStream)
	}

	cliqueSub, err := ps.ps.Subscribe(ctx, ps.cliqueStream, "", h)
	if err != nil {
		return err
	}

	nodeSub, err := ps.ps.Subscribe(ctx, ps.nodeStream, "", h)
	if err != nil {
		ps.unsubscribe(ctx, ps.cliqueStream, cliqueSub)
		return err
	}

	ps.subs[ps.cliqueStream] = cliqueSub
	ps.subs[ps.nodeStream] = nodeSub
	return nil
}

func (ps *pubsub) subscribe(ctx context.Context, stream string, h PubSubHandler) error {
	if _, has := ps.subs[stream]; has {
		return errorf("already subscribed to '%s'", stream)
	}

	sub, err := ps.ps.Subscribe(ctx, stream, ps.cliqueStream, h)
	if err != nil {
		return err
	}

	ps.subs[stream] = sub
	return nil
}

func (ps *pubsub) unsubscribe(ctx context.Context, stream string, sub Subscription) {
	if err := sub.Unsubscribe(ctx); err != nil {
		ps.onError(errorf("error unsubscribing from '%s'", stream))
	}
}

func (ps *pubsub) unsubscribeAll(ctx context.Context) {
	for stream, sub := range ps.subs {
		ps.unsubscribe(ctx, stream, sub)
	}
}

func nodeStream(cliqueName string, nodeKey key) string {
	buf := alloc(len(cliqueName)+1+2*KeySize, nil)
	n := copy(buf, cliqueName)
	buf[n] = '.'
	hex.Encode(buf[n+1:], nodeKey)
	return string(buf)
}
