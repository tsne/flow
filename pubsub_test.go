package flow

import (
	"bytes"
	"sync"
	"testing"
)

func TestPubSubNewPubSub(t *testing.T) {
	ps := newPubSub(newPublishRecorder(), newSubscriptionRecorder(), options{
		groupName: "group",
		nodeKey:   intKey(7),
	})

	switch {
	case ps.pub == nil:
		t.Fatal("no publisher")
	case ps.sub == nil:
		t.Fatal("no subscriber")
	case ps.groupStream != "group":
		t.Fatalf("unexpected group stream: %s", ps.groupStream)
	case ps.nodeStream != "group.0000000000000000000000000000000000000007":
		t.Fatalf("unexpected node stream: %s", ps.nodeStream)
	case len(ps.subs) != 0:
		t.Fatalf("unexpected number of subscriptions: %d", len(ps.subs))
	}
}

func TestPubSubSendToGroup(t *testing.T) {
	msg := message("group message")
	pub := newPublishRecorder()
	ps := pubsub{
		pub:         pub,
		groupStream: "group",
	}

	var sendErr error
	go func() { sendErr = ps.sendToGroup(msg) }()
	sentMsg := <-pub.channel("group")

	switch {
	case sendErr != nil:
		t.Fatalf("unexpected error: %v", sendErr)
	case !bytes.Equal(sentMsg, msg):
		t.Fatalf("unexpected published message: %s", sentMsg)
	}
}

func TestPubSubSendToNode(t *testing.T) {
	msg := message("node message")
	pub := newPublishRecorder()
	ps := pubsub{
		pub:         pub,
		groupStream: "group",
	}

	var sendErr error
	go func() { sendErr = ps.sendToNode(intKey(7), msg) }()
	sentMsg := <-pub.channel("group.0000000000000000000000000000000000000007")

	switch {
	case sendErr != nil:
		t.Fatalf("unexpected error: %v", sendErr)
	case !bytes.Equal(sentMsg, msg):
		t.Fatalf("unexpected published message: %s", sentMsg)
	}
}

func TestPubSubPublish(t *testing.T) {
	msg := message("message")
	pub := newPublishRecorder()
	ps := pubsub{pub: pub}

	var sendErr error
	go func() { sendErr = ps.publish("stream", msg) }()
	sentMsg := <-pub.channel("stream")

	switch {
	case sendErr != nil:
		t.Fatalf("unexpected error: %v", sendErr)
	case !bytes.Equal(sentMsg, msg):
		t.Fatalf("unexpected published message: %s", sentMsg)
	}
}

func TestPubSubSubscribeGroup(t *testing.T) {
	sub := newSubscriptionRecorder()
	ps := pubsub{
		sub:         sub,
		groupStream: "group",
		nodeStream:  "node",
	}

	err := ps.subscribeGroup(func(stream string, data []byte) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := sub.get("group")
	switch {
	case s == nil:
		t.Fatal("no group subscriptions")
	case s.group != "":
		t.Fatalf("unexpected subscription group: %s", s.group)
	}

	s = sub.get("node")
	switch {
	case s == nil:
		t.Fatal("no node subscriptions")
	case s.group != "":
		t.Fatalf("unexpected subscription group: %s", s.group)
	}
}

func TestPubSubSubscribe(t *testing.T) {
	sub := newSubscriptionRecorder()
	ps := pubsub{
		sub:         sub,
		groupStream: "group",
		subs:        make(map[string]struct{}),
	}

	err := ps.subscribe("stream", func(stream string, data []byte) {})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, has := ps.subs["stream"]; !has {
		t.Fatal("missing tracked subscription")
	}

	s := sub.get("stream")
	switch {
	case s == nil:
		t.Fatal("missing subscription")
	case s.group != "group":
		t.Fatalf("unexpected subscription group: %s", s.group)
	}
}

func TestPubSubShutdown(t *testing.T) {
	sub := newSubscriptionRecorder()
	sub.Subscribe("group", "", nil)
	sub.Subscribe("node", "", nil)
	sub.Subscribe("stream one", "", nil)
	sub.Subscribe("stream two", "", nil)

	ps := pubsub{
		sub:         sub,
		groupStream: "group",
		nodeStream:  "node",
		subs:        make(map[string]struct{}),
	}
	ps.subs["stream one"] = struct{}{}
	ps.subs["stream two"] = struct{}{}

	ps.shutdown()
	if nsubs := sub.countSubs(); nsubs != 0 {
		t.Fatalf("unexpected number of subscriptions: %d", nsubs)
	}
}

type publishRecorder struct {
	mtx   sync.Mutex
	chans map[string]chan message // stream => message channel
}

func newPublishRecorder() *publishRecorder {
	return &publishRecorder{
		chans: make(map[string]chan message),
	}
}

func (r *publishRecorder) Publish(stream string, data []byte) error {
	ch := r.channel(stream)
	ch <- data
	return nil
}

func (r *publishRecorder) channel(stream string) chan message {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	ch, has := r.chans[stream]
	if !has {
		ch = make(chan message)
		r.chans[stream] = ch
	}
	return ch
}

func (r *publishRecorder) clear() {
	r.mtx.Lock()
	r.chans = map[string]chan message{}
	r.mtx.Unlock()
}

type subscription struct {
	group   string
	handler PubSubHandler
}

type subscriptionRecorder struct {
	mtx  sync.Mutex
	subs map[string]*subscription // stream => subscription
}

func newSubscriptionRecorder() *subscriptionRecorder {
	return &subscriptionRecorder{
		subs: make(map[string]*subscription),
	}
}

func (r *subscriptionRecorder) Subscribe(stream, group string, h PubSubHandler) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, has := r.subs[stream]; has {
		return errorString("already subscribed")
	}
	r.subs[stream] = &subscription{
		group:   group,
		handler: h,
	}
	return nil
}

func (r *subscriptionRecorder) Unsubscribe(stream string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, has := r.subs[stream]; !has {
		return errorString("not subscribed")
	}
	delete(r.subs, stream)
	return nil
}

func (r *subscriptionRecorder) get(stream string) *subscription {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.subs[stream]
}

func (r *subscriptionRecorder) countSubs() int {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return len(r.subs)
}
