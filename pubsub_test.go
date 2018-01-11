package flow

import (
	"bytes"
	"sync"
	"testing"
)

func TestPubSubNewPubSub(t *testing.T) {
	ps := newPubSub(newPubsubRecorder(), options{
		groupName: "group",
		nodeKey:   intKey(7),
	})

	switch {
	case ps.PubSub == nil:
		t.Fatal("no pub/sub")
	case ps.groupName != "group":
		t.Fatalf("unexpected group stream: %s", ps.groupName)
	case ps.nodeName != "group.0000000000000000000000000000000000000007":
		t.Fatalf("unexpected node stream: %s", ps.nodeName)
	case len(ps.subs) != 0:
		t.Fatalf("unexpected number of subscriptions: %d", len(ps.subs))
	}
}

func TestPubSubSendToGroup(t *testing.T) {
	msg := message("group message")
	rec := newPubsubRecorder()
	ps := pubsub{
		PubSub:    rec,
		groupName: "group",
	}

	var sendErr error
	go func() { sendErr = ps.sendToGroup(msg) }()
	sentMsg := <-rec.pubchan("group")

	switch {
	case sendErr != nil:
		t.Fatalf("unexpected error: %v", sendErr)
	case !bytes.Equal(sentMsg, msg):
		t.Fatalf("unexpected published message: %s", sentMsg)
	}
}

func TestPubSubSendToNode(t *testing.T) {
	msg := message("node message")
	rec := newPubsubRecorder()
	ps := pubsub{
		PubSub:    rec,
		groupName: "group",
	}

	var sendErr error
	go func() { sendErr = ps.sendToNode(intKey(7), msg) }()
	sentMsg := <-rec.pubchan("group.0000000000000000000000000000000000000007")

	switch {
	case sendErr != nil:
		t.Fatalf("unexpected error: %v", sendErr)
	case !bytes.Equal(sentMsg, msg):
		t.Fatalf("unexpected published message: %s", sentMsg)
	}
}

func TestPubSubPublish(t *testing.T) {
	msg := message("message")
	rec := newPubsubRecorder()
	ps := pubsub{PubSub: rec}

	var sendErr error
	go func() { sendErr = ps.publish("stream", msg) }()
	sentMsg := <-rec.pubchan("stream")

	switch {
	case sendErr != nil:
		t.Fatalf("unexpected error: %v", sendErr)
	case !bytes.Equal(sentMsg, msg):
		t.Fatalf("unexpected published message: %s", sentMsg)
	}
}

func TestPubSubSubscribeGroup(t *testing.T) {
	rec := newPubsubRecorder()
	ps := pubsub{
		PubSub:    rec,
		groupName: "group",
		nodeName:  "node",
		subs:      make(map[string]Subscription),
	}

	err := ps.subscribeGroup(func(stream string, data []byte) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := rec.sub("group")
	switch {
	case s == nil:
		t.Fatal("no group subscriptions")
	case s.group != "":
		t.Fatalf("unexpected subscription group: %s", s.group)
	}

	s = rec.sub("node")
	switch {
	case s == nil:
		t.Fatal("no node subscriptions")
	case s.group != "":
		t.Fatalf("unexpected subscription group: %s", s.group)
	}
}

func TestPubSubSubscribe(t *testing.T) {
	rec := newPubsubRecorder()
	ps := pubsub{
		PubSub:    rec,
		groupName: "group",
		subs:      make(map[string]Subscription),
	}

	err := ps.subscribe("stream", func(stream string, data []byte) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, has := ps.subs["stream"]; !has {
		t.Fatal("missing tracked subscription")
	}

	s := rec.sub("stream")
	switch {
	case s == nil:
		t.Fatal("missing subscription")
	case s.group != "group":
		t.Fatalf("unexpected subscription group: %s", s.group)
	}
}

func TestPubSubShutdown(t *testing.T) {
	rec := newPubsubRecorder()
	ps := pubsub{
		PubSub: rec,
		subs:   make(map[string]Subscription),
	}
	ps.subscribe("stream one", nil)
	ps.subscribe("stream two", nil)

	ps.shutdown()
	if nsubs := rec.countSubs(); nsubs != 0 {
		t.Fatalf("unexpected number of subscriptions: %d", nsubs)
	}
}

// pubsub stub

type subscription struct {
	group       string
	handler     PubSubHandler
	unsubscribe func() error
}

func (s *subscription) Unsubscribe() error {
	return s.unsubscribe()
}

type pubsubRecorder struct {
	pubMtx sync.Mutex
	pubs   map[string]chan message // stream => message channel

	subMtx sync.Mutex
	subs   map[string]*subscription // stream => subscription

}

func newPubsubRecorder() *pubsubRecorder {
	return &pubsubRecorder{
		pubs: make(map[string]chan message),
		subs: make(map[string]*subscription),
	}
}

func (r *pubsubRecorder) Publish(stream string, data []byte) error {
	r.pubchan(stream) <- data
	return nil
}

func (r *pubsubRecorder) Subscribe(stream, group string, h PubSubHandler) (Subscription, error) {
	r.subMtx.Lock()
	defer r.subMtx.Unlock()

	if _, has := r.subs[stream]; has {
		return nil, errorString("already subscribed")
	}
	sub := &subscription{
		group:       group,
		handler:     h,
		unsubscribe: func() error { return r.unsubscribe(stream) },
	}
	r.subs[stream] = sub
	return sub, nil
}

func (r *pubsubRecorder) pubchan(stream string) chan message {
	r.pubMtx.Lock()
	defer r.pubMtx.Unlock()

	ch, has := r.pubs[stream]
	if !has {
		ch = make(chan message)
		r.pubs[stream] = ch
	}
	return ch
}

func (r *pubsubRecorder) unsubscribe(stream string) error {
	r.subMtx.Lock()
	defer r.subMtx.Unlock()

	if _, has := r.subs[stream]; !has {
		return errorString("not subscribed")
	}
	delete(r.subs, stream)
	return nil
}

func (r *pubsubRecorder) sub(stream string) *subscription {
	r.subMtx.Lock()
	defer r.subMtx.Unlock()
	return r.subs[stream]
}

func (r *pubsubRecorder) countSubs() int {
	r.subMtx.Lock()
	defer r.subMtx.Unlock()
	return len(r.subs)
}

func (r *pubsubRecorder) clear() {
	r.pubMtx.Lock()
	r.pubs = map[string]chan message{}
	r.pubMtx.Unlock()

	r.subMtx.Lock()
	r.subs = map[string]*subscription{}
	r.subMtx.Unlock()
}
