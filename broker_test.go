package flow

import (
	"bytes"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNewBroker(t *testing.T) {
	rec := newPubsubRecorder()

	var (
		b     *Broker
		err   error
		wg    sync.WaitGroup
		frame frame
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b, err = NewBroker(rec, Group("group"))
	}()
	go func() {
		defer wg.Done()
		frame = <-rec.pubchan("group")
	}()
	wg.Wait()

	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case b.ackTimeout <= 0:
		t.Fatalf("unexpected ack timeout: %v", b.ackTimeout)
	case b.respID != 0:
		t.Fatalf("unexpected frame id: %d", b.respID)
	case b.closing == nil:
		t.Fatal("no closing channel")
	case b.pendingResps == nil:
		t.Fatal("no pending response map")
	case len(b.pendingResps) != 0:
		t.Fatalf("unexpected number of pending responses: %d", len(b.pendingResps))
	case b.handlers == nil:
		t.Fatal("no handler map")
	case len(b.handlers) != 0:
		t.Fatalf("unexpected number of handlers: %d", len(b.handlers))
	case len(frame) == 0:
		t.Fatalf("unexpected frame length: %d", len(frame))
	case frame.typ() != frameTypeJoin:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = b.Close()
	}()
	frame = <-rec.pubchan("group")
	wg.Wait()

	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case frame.typ() != frameTypeLeave:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	}
}

func TestBrokerClose(t *testing.T) {
	errch := make(chan error, 1)
	rec := newPubsubRecorder()
	b := &Broker{
		pubsub: newPubSub(rec, defaultOptions()),
		pendingResps: map[uint64]pendingResp{
			1: {
				timer: time.AfterFunc(time.Second, func() { t.Fatalf("response timeout") }),
				errch: errch,
			},
		},
		closing: make(chan struct{}),
	}

	var (
		wg        sync.WaitGroup
		closeErr  error
		published frame
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		closeErr = b.Close()
	}()
	go func() {
		defer wg.Done()
		published = <-rec.pubchan(b.pubsub.groupName)
	}()
	wg.Wait()

	if closeErr != nil {
		t.Fatalf("unexpected close error: %v", closeErr)
	}
	if published.typ() != frameTypeLeave {
		t.Fatalf("unexpected frame type: %s", published.typ())
	}
	if err := <-errch; err != errClosing {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBrokerPublish(t *testing.T) {
	rec := newPubsubRecorder()
	store := newStoreRecorder()

	opts := defaultOptions()
	opts.store = store

	b := &Broker{
		codec:   DefaultCodec{},
		pubsub:  newPubSub(rec, opts),
		storage: newStorage(opts),
	}

	msg := Message{
		Stream:       "stream",
		Source:       []byte("source id"),
		Time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
		PartitionKey: []byte("partition key"),
		Data:         []byte("data"),
	}

	var (
		wg        sync.WaitGroup
		err       error
		published frame
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		err = b.Publish(msg)
	}()
	go func() {
		defer wg.Done()
		published = <-rec.pubchan(msg.Stream)
	}()
	wg.Wait()

	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case store.countMessages() != 1:
		t.Fatalf("unexpected number of store messages: %d", store.countMessages())
	case !reflect.DeepEqual(*store.message(0), msg):
		t.Fatalf("unexpected store message: %+v", store.message(0))
	}

	decoded, err := b.codec.DecodeMessage("stream", published)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !equalMessage(decoded, msg) {
		t.Fatalf("unexpected decoded message: %+v", decoded)
	}
}

func TestBrokerPublishWithValidationErrors(t *testing.T) {
	messages := []Message{
		{ // missing stream
		},
	}

	b := &Broker{}
	for _, msg := range messages {
		if b.Publish(msg) == nil {
			t.Fatalf("error expected for message %+v", msg)
		}
	}
}

func TestBrokerSubscribe(t *testing.T) {
	rec := newPubsubRecorder()
	b := &Broker{
		pubsub:   newPubSub(rec, defaultOptions()),
		handlers: make(map[string][]Handler),
	}

	err := b.Subscribe("stream", func(Message) {})
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(b.handlers) != 1:
		t.Fatalf("unexpected number of subscribed streams: %d", len(b.handlers))
	case len(b.handlers["stream"]) != 1:
		t.Fatalf("unexpected number of subscribed handlers: %d", len(b.handlers["stream"]))
	}

	s := rec.sub("stream")
	switch {
	case s == nil:
		t.Fatal("no subscription")
	case s.group != b.pubsub.groupName:
		t.Fatalf("unexpected subscription group: %s", s.group)
	}

	// subscribe to the same stream again
	err = b.Subscribe("stream", func(Message) {})
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(b.handlers) != 1:
		t.Fatalf("unexpected number of subscribed streams: %d", len(b.handlers))
	case len(b.handlers["stream"]) != 2:
		t.Fatalf("unexpected number of subscribed handlers: %d", len(b.handlers["stream"]))
	}

	s = rec.sub("stream")
	switch {
	case s == nil:
		t.Fatal("no subscription")
	case s.group != b.pubsub.groupName:
		t.Fatalf("unexpected subscription group: %s", s.group)
	}
}

func TestBrokerHandleJoin(t *testing.T) {
	keys := intKeys(1, 2)
	opts := defaultOptions()
	opts.nodeKey = keys.at(0)
	opts.groupName = "group"

	rec := newPubsubRecorder()
	b := &Broker{
		routing: newRoutingTable(opts),
		pubsub:  newPubSub(rec, opts),
	}

	join := join{
		sender: keys.at(1),
	}

	var (
		wg    sync.WaitGroup
		frame frame
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", marshalJoin(join, nil))
	}()
	go func() {
		defer wg.Done()
		frame = <-rec.pubchan("group.0000000000000000000000000000000000000002")
	}()
	wg.Wait()

	switch {
	case b.routing.keys.length() != 1:
		t.Fatalf("unexpected number of keys: %d", b.routing.keys.length())
	case !b.routing.keys.at(0).equal(join.sender):
		t.Fatalf("unexpected keys: %v", printableKeys(b.routing.keys))
	case frame.typ() != frameTypeInfo:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	}

	info, err := unmarshalInfo(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if info.neighbors.length() == 0 {
		t.Fatalf("unexpected number of neighbors: %d", info.neighbors.length())
	}
}

func TestBrokerHandleLeave(t *testing.T) {
	keys := intKeys(1, 2)
	opts := defaultOptions()
	opts.nodeKey = keys.at(0)

	b := &Broker{
		routing: newRoutingTable(opts),
	}
	b.routing.register(keys)

	b.processGroupSubs("stream", marshalLeave(leave{
		node: keys.at(1),
	}, nil))

	if b.routing.keys.length() != 0 {
		t.Fatalf("unexpected number of keys: %d", b.routing.keys.length())
	}
}

func TestBrokerHandleInfo(t *testing.T) {
	keys := intKeys(1, 2)
	opts := defaultOptions()
	opts.nodeKey = keys.at(0)

	b := &Broker{
		routing: newRoutingTable(opts),
		pendingResps: map[uint64]pendingResp{
			1: {
				timer: time.AfterFunc(time.Second, func() { t.Fatalf("response timeout") }),
				errch: make(chan error, 1),
			},
		},
	}

	b.processGroupSubs("stream", marshalInfo(info{
		id:        1,
		neighbors: keys,
	}, nil))

	switch {
	case len(b.pendingResps) != 0:
		t.Fatalf("unexpected number of pending responses: %d", len(b.pendingResps))
	case b.routing.keys.length() != 1:
		t.Fatalf("unexpected number of keys: %d", b.routing.keys.length())
	case !b.routing.keys.at(0).equal(keys.at(1)):
		t.Fatalf("unexpected keys: %d", printableKeys(b.routing.keys))
	}
}

func TestBrokerHandlePing(t *testing.T) {
	keys := intKeys(1, 2)
	opts := defaultOptions()
	opts.nodeKey = keys.at(0)
	opts.groupName = "group"

	rec := newPubsubRecorder()
	b := &Broker{
		pubsub: newPubSub(rec, opts),
	}

	ping := ping{
		id:     1,
		sender: keys.at(1),
	}

	var (
		wg    sync.WaitGroup
		frame frame
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", marshalPing(ping, nil))
	}()
	go func() {
		defer wg.Done()
		frame = <-rec.pubchan("group.0000000000000000000000000000000000000002")
	}()
	wg.Wait()

	if frame.typ() != frameTypeInfo {
		t.Fatalf("unexpected frame type: %s", frame.typ())
	}

	info, err := unmarshalInfo(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if info.neighbors.length() == 0 {
		t.Fatalf("unexpected number of neighbors: %d", info.neighbors.length())
	}
}

func TestBrokerHandleFwd(t *testing.T) {
	keys := intKeys(1, 2, 3)
	opts := defaultOptions()
	opts.nodeKey = keys.at(0)
	opts.groupName = "group"

	fwd := fwd{
		id:     11,
		origin: keys.at(1),
		msg: Message{
			Stream:       "fwdstream",
			Source:       []byte("source id"),
			Time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
			PartitionKey: keys.at(2),
			Data:         []byte("payload"),
		},
	}

	handlerCalled := false

	rec := newPubsubRecorder()
	b := &Broker{
		routing: newRoutingTable(opts),
		pubsub:  newPubSub(rec, opts),
		handlers: map[string][]Handler{
			fwd.msg.Stream: {
				func(msg Message) {
					if !equalMessage(msg, fwd.msg) {
						t.Fatalf("unexpected message: %s", msg)
					}
					handlerCalled = true
				},
			},
		},
	}

	var (
		wg    sync.WaitGroup
		frame frame
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", marshalFwd(fwd, nil))
	}()
	go func() {
		defer wg.Done()
		frame = <-rec.pubchan("group.0000000000000000000000000000000000000002")
	}()
	wg.Wait()

	// local dispatch
	switch {
	case !handlerCalled:
		t.Fatal("expected handler to be called")
	case frame.typ() != frameTypeAck:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	}

	ack, err := unmarshalAck(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if ack.id != fwd.id {
		t.Fatalf("unexpected ack id: %d", ack.id)
	} else if ack.err != nil {
		t.Fatalf("unexpected ack error: %v", ack.err)
	}

	// forward again
	rec.clear()
	handlerCalled = false
	fwdframe := marshalFwd(fwd, nil)
	b.routing.register(keys)

	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", fwdframe)
	}()
	go func() {
		defer wg.Done()
		frame = <-rec.pubchan("group.0000000000000000000000000000000000000003")
	}()
	wg.Wait()

	switch {
	case handlerCalled:
		t.Fatal("unexpected handler call")
	case !bytes.Equal(frame, fwdframe):
		t.Fatalf("unexpected fwd frame: %v", frame)
	}
}

func TestBrokerHandleAck(t *testing.T) {
	errch := make(chan error)
	b := &Broker{
		pendingResps: map[uint64]pendingResp{
			7: {
				timer: time.AfterFunc(time.Second, func() { t.Fatalf("response timeout") }),
				errch: errch,
			},
		},
	}

	ack := ack{
		id:  7,
		err: ackError("error"),
	}

	var (
		wg  sync.WaitGroup
		err error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", marshalAck(ack, nil))
	}()
	go func() {
		defer wg.Done()
		err = <-errch
	}()
	wg.Wait()

	switch {
	case !reflect.DeepEqual(err, ack.err):
		t.Fatalf("unexpected ack error: %v", err)
	case len(b.pendingResps) != 0:
		t.Fatalf("unexpected number of pending responses: %d", len(b.pendingResps))
	}
}

func TestBrokerProcessSub(t *testing.T) {
	handlerCalled := false
	publishedMsg := Message{
		Stream: "stream",
		Source: []byte("source id"),
		Time:   time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
		Data:   []byte("payload"),
	}
	pkey := StringKey("partition key")
	local := pkey
	local[0]++

	opts := defaultOptions()
	opts.nodeKey = local[:]
	opts.groupName = "group"

	rec := newPubsubRecorder()
	b := &Broker{
		codec:   DefaultCodec{},
		routing: newRoutingTable(opts),
		pubsub:  newPubSub(rec, opts),
		handlers: map[string][]Handler{
			"stream": {
				func(msg Message) {
					if !equalMessage(msg, publishedMsg) {
						t.Fatalf("unexpected message: %+v", msg)
					}
					handlerCalled = true
				},
			},
		},
		pendingResps: map[uint64]pendingResp{},
	}

	// local dispatch without partition key
	b.processSub("stream", b.codec.EncodeMessage(publishedMsg))
	if !handlerCalled {
		t.Fatal("expected handler to be called")
	}

	// local dispatch with partition key
	handlerCalled = false
	publishedMsg.PartitionKey = []byte("partition key")

	b.processSub("stream", b.codec.EncodeMessage(publishedMsg))
	if !handlerCalled {
		t.Fatal("expected handler to be called")
	}

	// forward with ack
	handlerCalled = false
	b.ackTimeout = time.Second
	b.routing.register(pkey[:])

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processSub("stream", b.codec.EncodeMessage(publishedMsg))
	}()
	go func() {
		defer wg.Done()
		frame := <-rec.pubchan("group." + pkey.String())
		if frame.typ() != frameTypeFwd {
			t.Fatalf("unexpected frame type: %s", frame.typ())
		}

		fwd, err := unmarshalFwd(frame)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		} else if fwd.id == 0 || !fwd.origin.equal(b.routing.local) || !fwd.key.equal(pkey[:]) || !equalMessage(fwd.msg, publishedMsg) {
			t.Fatalf("unexpected fwd frame: %+v", fwd)
		}

		b.processGroupSubs("stream", marshalAck(ack{id: fwd.id}, nil))
	}()
	wg.Wait()

	if handlerCalled {
		t.Fatal("unexpected handler call")
	}

	// forward with ack timeout
	handlerCalled = false
	b.ackTimeout = time.Microsecond

	var frame frame

	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processSub("stream", b.codec.EncodeMessage(publishedMsg))
	}()
	go func() {
		defer wg.Done()
		frame = <-rec.pubchan("group." + pkey.String())
	}()
	wg.Wait()

	switch {
	case !handlerCalled:
		t.Fatal("expected handler to be called")
	case frame.typ() != frameTypeFwd:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case b.routing.keys.length() != 0:
		t.Fatalf("unexpected number of routing keys: %d", b.routing.keys.length())
	}

	fwd, err := unmarshalFwd(frame)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if fwd.id == 0 || !fwd.origin.equal(b.routing.local) || !fwd.key.equal(pkey[:]) || !equalMessage(fwd.msg, publishedMsg) {
		t.Fatalf("unexpected fwd frame: %+v", fwd)
	}
}

func TestBrokerStabilize(t *testing.T) {
	keys := intKeys(1, 2)
	opts := defaultOptions()
	opts.groupName = "group"
	opts.nodeKey = keys.at(0)

	rec := newPubsubRecorder()
	b := &Broker{
		ackTimeout:   time.Second,
		routing:      newRoutingTable(opts),
		pubsub:       newPubSub(rec, opts),
		closing:      make(chan struct{}),
		pendingResps: make(map[uint64]pendingResp),
	}
	b.routing.register(keys)
	b.wg.Add(1)

	var (
		wg     sync.WaitGroup
		frames [2]frame
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.stabilize(time.Millisecond)
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < len(frames); i++ {
			frames[i] = <-rec.pubchan("group.0000000000000000000000000000000000000002")
			// The generated response ids are just incremented, starting with 1.
			b.notifyResp(uint64(i+1), nil)
		}
		close(b.closing)
	}()
	wg.Wait()

	for i, frame := range frames {
		if frame.typ() != frameTypePing {
			t.Fatalf("unexpected frame type at %d: %s", i, frame.typ())
		}

		ping, err := unmarshalPing(frame)
		if err != nil {
			t.Errorf("unexpected error at %d: %v", i, err)
		} else if ping.id == 0 || !ping.sender.equal(b.routing.local) {
			t.Errorf("unexpected ping frame at %d: %+v", i, ping)
		}
	}
}
