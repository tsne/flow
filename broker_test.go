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
		b   *Broker
		err error
		wg  sync.WaitGroup
		msg message
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b, err = NewBroker(rec, Group("group"))
	}()
	go func() {
		defer wg.Done()
		msg = <-rec.pubchan("group")
	}()
	wg.Wait()

	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case b.ackTimeout <= 0:
		t.Fatalf("unexpected ack timeout: %v", b.ackTimeout)
	case b.respID != 0:
		t.Fatalf("unexpected message id: %d", b.respID)
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
	case len(msg) == 0:
		t.Fatalf("unexpected message length: %d", len(msg))
	case msg.typ() != msgTypeJoin:
		t.Fatalf("unexpected message type: %s", msg.typ())
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = b.Close()
	}()
	msg = <-rec.pubchan("group")
	wg.Wait()

	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case msg.typ() != msgTypeLeave:
		t.Fatalf("unexpected message type: %s", msg.typ())
	}
}

func TestBrokerPublish(t *testing.T) {
	rec := newPubsubRecorder()
	store := newStoreRecorder()

	opts := defaultOptions()
	opts.store = store

	b := &Broker{
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
		published message
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
	case published.typ() != msgTypePub:
		t.Fatalf("unexpected message type: %s", published.typ())
	}

	pub, err := unmarshalPub(published)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(pub.source, msg.Source) || !pub.time.Equal(msg.Time) || !bytes.Equal(pub.partitionKey, msg.PartitionKey) || !bytes.Equal(pub.payload, msg.Data) {
		t.Fatalf("unexpected pub message: %+v", pub)
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
		wg  sync.WaitGroup
		msg message
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", marshalJoin(join, nil))
	}()
	go func() {
		defer wg.Done()
		msg = <-rec.pubchan("group.0000000000000000000000000000000000000002")
	}()
	wg.Wait()

	switch {
	case b.routing.keys.length() != 1:
		t.Fatalf("unexpected number of keys: %d", b.routing.keys.length())
	case !b.routing.keys.at(0).equal(join.sender):
		t.Fatalf("unexpected keys: %v", printableKeys(b.routing.keys))
	case msg.typ() != msgTypeInfo:
		t.Fatalf("unexpected message type: %s", msg.typ())
	}

	info, err := unmarshalInfo(msg)
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
			1: {timer: time.AfterFunc(time.Second, func() { t.Fatalf("response timeout") })},
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
		wg  sync.WaitGroup
		msg message
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", marshalPing(ping, nil))
	}()
	go func() {
		defer wg.Done()
		msg = <-rec.pubchan("group.0000000000000000000000000000000000000002")
	}()
	wg.Wait()

	if msg.typ() != msgTypeInfo {
		t.Fatalf("unexpected message type: %s", msg.typ())
	}

	info, err := unmarshalInfo(msg)
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
		stream: "fwdstream",
		pub: pub{
			source:       []byte("source id"),
			time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
			partitionKey: keys.at(2),
			payload:      []byte("payload"),
		},
	}

	handlerCalled := false

	rec := newPubsubRecorder()
	b := &Broker{
		routing: newRoutingTable(opts),
		pubsub:  newPubSub(rec, opts),
		handlers: map[string][]Handler{
			fwd.stream: {
				func(msg Message) {
					switch {
					case msg.Stream != fwd.stream:
						t.Fatalf("unexpected stream: %s", msg.Stream)
					case !bytes.Equal(msg.Source, fwd.source):
						t.Fatalf("unexpected source: %s", msg.Source)
					case !msg.Time.Equal(fwd.time):
						t.Fatalf("unexpected time: %s", msg.Time)
					case !bytes.Equal(msg.PartitionKey, fwd.partitionKey):
						t.Fatalf("unexpected partition key: %s", msg.PartitionKey)
					case !bytes.Equal(msg.Data, fwd.payload):
						t.Fatalf("unexpected payload: %s", msg.Data)
					}
					handlerCalled = true
				},
			},
		},
	}

	var (
		wg  sync.WaitGroup
		msg message
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", marshalFwd(fwd, nil))
	}()
	go func() {
		defer wg.Done()
		msg = <-rec.pubchan("group.0000000000000000000000000000000000000002")
	}()
	wg.Wait()

	// local dispatch
	switch {
	case !handlerCalled:
		t.Fatal("expected handler to be called")
	case msg.typ() != msgTypeAck:
		t.Fatalf("unexpected message type: %s", msg.typ())
	}

	ack, err := unmarshalAck(msg)
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
	fwdmsg := marshalFwd(fwd, nil)
	b.routing.register(keys)

	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processGroupSubs("stream", fwdmsg)
	}()
	go func() {
		defer wg.Done()
		msg = <-rec.pubchan("group.0000000000000000000000000000000000000003")
	}()
	wg.Wait()

	switch {
	case handlerCalled:
		t.Fatal("unexpected handler call")
	case !bytes.Equal(msg, fwdmsg):
		t.Fatalf("unexpected fwd message: %v", msg)
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
	case err != ack.err:
		t.Fatalf("unexpected ack error: %v", err)
	case len(b.pendingResps) != 0:
		t.Fatalf("unexpected number of pending responses: %d", len(b.pendingResps))
	}
}

func TestBrokerProcessSub(t *testing.T) {
	handlerCalled := false
	pub := pub{
		source:  []byte("source id"),
		time:    time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
		payload: []byte("payload"),
	}
	pkey := StringKey("partition key")
	local := pkey
	local[0]++

	opts := defaultOptions()
	opts.nodeKey = local[:]
	opts.groupName = "group"

	rec := newPubsubRecorder()
	b := &Broker{
		routing: newRoutingTable(opts),
		pubsub:  newPubSub(rec, opts),
		handlers: map[string][]Handler{
			"stream": {
				func(msg Message) {
					switch {
					case msg.Stream != "stream":
						t.Fatalf("unexpected stream: %s", msg.Stream)
					case !bytes.Equal(msg.Source, pub.source):
						t.Fatalf("unexpected source: %s", msg.Source)
					case !msg.Time.Equal(pub.time):
						t.Fatalf("unexpected time: %s", msg.Time)
					case !bytes.Equal(msg.PartitionKey, pub.partitionKey):
						t.Fatalf("unexpected partition key: %s", msg.PartitionKey)
					case !bytes.Equal(msg.Data, pub.payload):
						t.Fatalf("unexpected payload: %s", msg.Data)
					}
					handlerCalled = true
				},
			},
		},
		pendingResps: map[uint64]pendingResp{},
	}

	// local dispatch without partition key
	b.processSub("stream", marshalPub(pub, nil))
	if !handlerCalled {
		t.Fatal("expected handler to be called")
	}

	// local dispatch with partition key
	handlerCalled = false
	pub.partitionKey = []byte("partition key")

	b.processSub("stream", marshalPub(pub, nil))
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
		b.processSub("stream", marshalPub(pub, nil))
	}()
	go func() {
		defer wg.Done()
		msg := <-rec.pubchan("group." + pkey.String())
		if msg.typ() != msgTypeFwd {
			t.Fatalf("unexpected message type: %s", msg.typ())
		}

		fwd, err := unmarshalFwd(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		} else if fwd.id == 0 || !fwd.origin.equal(b.routing.local) || fwd.stream != "stream" || !bytes.Equal(fwd.partitionKey, pub.partitionKey) || !bytes.Equal(fwd.payload, pub.payload) {
			t.Fatalf("unexpected fwd message: %+v", fwd)
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

	var msg message

	wg.Add(2)
	go func() {
		defer wg.Done()
		b.processSub("stream", marshalPub(pub, nil))
	}()
	go func() {
		defer wg.Done()
		msg = <-rec.pubchan("group." + pkey.String())
	}()
	wg.Wait()

	switch {
	case !handlerCalled:
		t.Fatal("expected handler to be called")
	case msg.typ() != msgTypeFwd:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case b.routing.keys.length() != 0:
		t.Fatalf("unexpected number of routing keys: %d", b.routing.keys.length())
	}

	fwd, err := unmarshalFwd(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if fwd.id == 0 || !fwd.origin.equal(b.routing.local) || fwd.stream != "stream" || !bytes.Equal(fwd.partitionKey, pub.partitionKey) || !bytes.Equal(fwd.payload, pub.payload) {
		t.Fatalf("unexpected fwd message: %+v", fwd)
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
		wg   sync.WaitGroup
		msgs [2]message
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		b.stabilize(time.Millisecond)
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < len(msgs); i++ {
			msgs[i] = <-rec.pubchan("group.0000000000000000000000000000000000000002")
		}
		close(b.closing)
	}()
	wg.Wait()

	for i, msg := range msgs {
		if msg.typ() != msgTypePing {
			t.Fatalf("unexpected message type at %d: %s", i, msg.typ())
		}

		ping, err := unmarshalPing(msg)
		if err != nil {
			t.Fatalf("unexpected error at %d: %v", i, err)
		} else if ping.id == 0 || !ping.sender.equal(b.routing.local) {
			t.Fatalf("unexpected ping message at %d: %+v", i, ping)
		}
	}
}
