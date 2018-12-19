package flow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerPublish(t *testing.T) {
	ctx := context.Background()
	pubsub := newPubsubRecorder()
	now := time.Date(1988, time.September, 26, 13, 14, 15, 0, time.UTC)

	b, err := NewBroker(ctx, pubsub,
		StabilizationInterval(time.Hour), // disable stabilization
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = b.Publish(ctx, Message{
		Stream:       "store-stream",
		Time:         now,
		PartitionKey: []byte("partition one"),
		Data:         []byte("message data"),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBrokerSubscription(t *testing.T) {
	const groupName = "testgroup"

	ctx := context.Background()
	now := time.Date(1988, time.September, 26, 13, 14, 15, 0, time.UTC)
	msg := Message{
		Stream:       "message-stream",
		Time:         now,
		PartitionKey: []byte("partition one"),
		Data:         []byte("message data"),
	}

	local := KeyFromBytes(msg.PartitionKey)
	localStream := nodeStream(groupName, local[:])

	pubsub := newPubsubRecorder()
	b, err := NewBroker(ctx, pubsub,
		Group(groupName),
		NodeKey(local),
		AckTimeout(time.Hour),            // disable ack timeouts
		StabilizationInterval(time.Hour), // disable stabilization
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	messageChan := make(chan Message, 1)
	err = b.Subscribe(ctx, msg.Stream, func(_ context.Context, msg Message) { messageChan <- msg })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("dispatch", func(t *testing.T) {
		err := b.Publish(ctx, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		receivedMsg := <-messageChan
		if !equalMessage(receivedMsg, msg) {
			t.Fatalf("unexpected message: %+v", receivedMsg)
		}
	})

	t.Run("forward", func(t *testing.T) {
		partitionKey := msg.PartitionKey
		msg.PartitionKey = []byte("partition two")
		defer func() { msg.PartitionKey = partitionKey }()

		remote := KeyFromBytes(msg.PartitionKey)

		b.routing.register(keys(remote[:]))
		defer b.routing.unregister(remote[:])

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(groupName, remote[:]))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case fwd.id != fwdID || !fwd.ack.equal(local[:]) || !fwd.pkey.equal(remote[:]) || !equalMessage(fwd.msg, msg):
				t.Fatalf("unexpected fwd: %+v", fwd)
			}

			pubsub.Publish(ctx, localStream, marshalAck(ack{
				id: fwdID,
			}, nil))

			if hasPendingAck(b, fwdID) {
				t.Fatalf("unexpected pending ack: %d", fwdID)
			}
		}()

		err := b.Publish(ctx, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("forward-timeout", func(t *testing.T) {
		ackTimeout := b.ackTimeout
		b.ackTimeout = time.Millisecond
		defer func() { b.ackTimeout = ackTimeout }()

		partitionKey := msg.PartitionKey
		msg.PartitionKey = []byte("partition two")
		defer func() { msg.PartitionKey = partitionKey }()

		remote := KeyFromBytes(msg.PartitionKey)

		b.routing.register(keys(remote[:]))
		defer b.routing.unregister(remote[:])

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(groupName, remote[:]))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case fwd.id != fwdID || !fwd.ack.equal(local[:]) || !fwd.pkey.equal(remote[:]) || !equalMessage(fwd.msg, msg):
				t.Fatalf("unexpected fwd: %+v", fwd)
			}

			// dispatch locally
			receivedMsg := <-messageChan
			switch {
			case !equalMessage(receivedMsg, msg):
				t.Fatalf("unexpected message: %+v", receivedMsg)
			case hasPendingAck(b, fwdID):
				t.Fatalf("unexpected pending ack: %d", fwdID)
			case containsKey(keys(b.routing.keys), remote[:]):
				t.Fatalf("unexpected routing keys: %v", b.routing.keys)
			}
		}()

		err := b.Publish(ctx, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestBrokerGroupProtocol(t *testing.T) {
	const groupName = "testgroup"

	ctx := context.Background()
	now := time.Date(1988, time.September, 26, 13, 14, 15, 0, time.UTC)

	local := intKey(1)
	localStream := nodeStream(groupName, local)

	remote := intKey(2)
	remoteStream := nodeStream(groupName, remote)

	pubsub := newPubsubRecorder()
	groupChan, groupSub := pubsub.SubscribeChan(ctx, groupName)
	defer groupSub.Unsubscribe(ctx)

	remoteChan, remoteSub := pubsub.SubscribeChan(ctx, remoteStream)
	defer remoteSub.Unsubscribe(ctx)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		frame := <-groupChan
		if frame.typ() != frameTypeJoin {
			t.Fatalf("unexpected frame type: %s", frame.typ())
		}

		join, err := unmarshalJoin(frame)
		switch {
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		case !join.sender.equal(local):
			t.Fatalf("unexpected join: %+v", join)
		}

		pubsub.Publish(ctx, localStream, marshalInfo(info{
			id:        13,
			neighbors: intKeys(101, 102, 103),
		}, nil))
	}()

	b, err := NewBroker(ctx, pubsub,
		NodeKey(local.array()),
		Group(groupName),
		AckTimeout(time.Hour),            // disable ack timeouts
		StabilizationInterval(time.Hour), // disable stabilization
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wg.Wait()

	if !equalKeys(keys(b.routing.keys), intKeys(101, 102, 103)) {
		t.Fatalf("unexpected routing keys: %v", b.routing.keys)
	}

	b.routing.unregister(intKey(101))
	b.routing.unregister(intKey(102))
	b.routing.unregister(intKey(103))

	if b.routing.keys.length() != 0 {
		t.Fatalf("unexpected routing keys: %v", b.routing.keys)
	}

	// --- join -->
	// <-- info ---
	t.Run("join", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		go func() {
			defer wg.Done()

			_ = <-groupChan // consume JOIN
			frame := <-remoteChan
			if frame.typ() != frameTypeInfo {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			info, err := unmarshalInfo(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case info.id != 0 || !equalKeys(info.neighbors, keys(b.routing.local)):
				t.Fatalf("unexpected info: %+v", info)
			}
		}()

		pubsub.Publish(ctx, groupName, marshalJoin(join{sender: remote}, nil))

		if !containsKey(keys(b.routing.keys), remote) {
			t.Fatalf("unexpected routing keys: %v", b.routing.keys)
		}
	})

	// --- ping -->
	// <-- info ---
	t.Run("ping", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		id := b.nextID()
		expectedNeighbors := b.routing.neighbors(nil)

		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeInfo {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			info, err := unmarshalInfo(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case info.id != id || !equalKeys(info.neighbors, expectedNeighbors):
				t.Fatalf("unexpected info: %+v", info)
			}
		}()

		pubsub.Publish(ctx, localStream, marshalPing(ping{
			id:     id,
			sender: remote,
		}, nil))
	})

	// --- fwd -->
	//              dispatch
	// <-- ack ---
	t.Run("dispatch", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		id := b.nextID()
		msg := Message{
			Stream:       "mystream",
			Time:         now,
			PartitionKey: nil,
			Data:         []byte("message data"),
		}

		incoming := make(chan Message, 1)
		err := b.Subscribe(ctx, msg.Stream, func(_ context.Context, m Message) {
			incoming <- m
		})
		if err != nil {
			t.Fatalf("unexpexted error: %v", err)
		}
		defer clearSubscriptions(b, msg.Stream)

		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeAck {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			ack, err := unmarshalAck(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case ack.id != id:
				t.Fatalf("unexpected ack: %+v", ack)
			}
		}()

		pubsub.Publish(ctx, localStream, marshalFwd(fwd{
			id:   id,
			ack:  remote,
			pkey: local,
			msg:  msg,
		}, nil))

		if in := <-incoming; !equalMessage(in, msg) {
			t.Fatalf("unexpected incoming message: %+v", in)
		}
	})

	// --- fwd -->
	// <-- ack ---
	//              --- fwd -->
	//              <-- ack ---
	t.Run("forward", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		id := b.nextID()
		msg := Message{
			Stream:       "mystream",
			Time:         now,
			PartitionKey: nil,
			Data:         []byte("message data"),
		}

		go func() {
			defer wg.Done()

			// ack
			frame := <-remoteChan
			if frame.typ() != frameTypeAck {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			sentAck, err := unmarshalAck(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case sentAck.id != id:
				t.Fatalf("unexpected ack: %+v", sentAck)
			}

			// fwd
			frame = <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case fwd.id != fwdID || !fwd.ack.equal(local) || !fwd.pkey.equal(remote) || !equalMessage(fwd.msg, msg):
				t.Fatalf("unexpected fwd: %+v", fwd)
			}

			pubsub.Publish(ctx, localStream, marshalAck(ack{
				id: fwdID,
			}, nil))

			if hasPendingAck(b, fwdID) {
				t.Fatalf("unexpected pending ack: %d", fwdID)
			}
		}()

		pubsub.Publish(ctx, localStream, marshalFwd(fwd{
			id:   id,
			ack:  remote,
			pkey: remote,
			msg:  msg,
		}, nil))

	})

	// --- fwd -->
	// <-- ack ---
	//              --- fwd -->
	//                timeout
	//                dispatch
	t.Run("forward-timeout", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		id := b.nextID()
		msg := Message{
			Stream:       "mystream",
			Time:         now,
			PartitionKey: nil,
			Data:         []byte("message data"),
		}

		go func() {
			defer wg.Done()

			ackTimeout := b.ackTimeout
			b.ackTimeout = time.Millisecond
			defer func() { b.ackTimeout = ackTimeout }()

			dispatched := make(chan Message, 1)
			b.Subscribe(ctx, msg.Stream, func(_ context.Context, msg Message) {
				dispatched <- msg
			})

			// ack
			frame := <-remoteChan
			if frame.typ() != frameTypeAck {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			sentAck, err := unmarshalAck(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case sentAck.id != id:
				t.Fatalf("unexpected ack: %+v", sentAck)
			}

			// fwd
			frame = <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case fwd.id != fwdID || !fwd.ack.equal(local) || !fwd.pkey.equal(remote) || !equalMessage(fwd.msg, msg):
				t.Fatalf("unexpected fwd: %+v", fwd)
			}

			// dispatch
			dispatchedMsg := <-dispatched
			switch {
			case !equalMessage(dispatchedMsg, msg):
				t.Fatalf("unexpected dispatched message: %+v", dispatchedMsg)
			case containsKey(keys(b.routing.keys), remote):
				t.Fatalf("unexpected routing keys: %v", b.routing.keys)
			}

			// add remote again for following tests
			b.routing.register(keys(remote))
		}()

		pubsub.Publish(ctx, localStream, marshalFwd(fwd{
			id:   id,
			ack:  remote,
			pkey: remote,
			msg:  msg,
		}, nil))
	})

	// leave
	t.Run("leave", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		go func() {
			defer wg.Done()

			_ = <-groupChan // consume LEAV
		}()

		pubsub.Publish(ctx, groupName, marshalLeave(leave{node: remote}, nil))

		if containsKey(keys(b.routing.keys), remote) {
			t.Fatalf("unexpected routing keys: %v", b.routing.keys)
		}
	})
}

func TestBrokerRequest(t *testing.T) {
	const groupName = "testgroup"

	ctx := context.Background()
	now := time.Date(1988, time.September, 26, 13, 14, 15, 0, time.UTC)

	local := intKey(1)
	localStream := nodeStream(groupName, local)

	pubsub := newPubsubRecorder()
	b, err := NewBroker(ctx, pubsub,
		NodeKey(local.array()),
		Group(groupName),
		AckTimeout(time.Hour),            // disable ack timeouts
		StabilizationInterval(time.Hour), // disable stabilization
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	request := Message{
		Stream:       "myrequests",
		PartitionKey: []byte("partition one"),
		Time:         now,
		Data:         []byte("request data"),
	}

	response := Message{
		Stream:       "myresponses",
		Time:         now,
		PartitionKey: []byte("partition two"),
		Data:         []byte("response data"),
	}

	t.Run("dispatch", func(t *testing.T) {
		err = b.SubscribeRequest(ctx, request.Stream, func(_ context.Context, msg Message) Message {
			if !equalMessage(msg, request) {
				t.Fatalf("unexpected request: %+v", msg)
			}
			return response
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer clearSubscriptions(b, request.Stream)

		msg, err := b.Request(ctx, request)
		switch {
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		case !equalMessage(msg, response):
			t.Fatalf("unexpected response: %+v", msg)
		}
	})

	t.Run("forward", func(t *testing.T) {
		err := b.SubscribeRequest(ctx, request.Stream, func(_ context.Context, msg Message) Message {
			t.Fatal("unexpected request handler call")
			return msg
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer clearSubscriptions(b, request.Stream)

		remote := intKey(0)
		reqID := lastID(b) + 1

		b.routing.register(keys(remote))
		defer b.routing.unregister(remote)

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(groupName, remote))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			fwdID := lastID(b)
			pkey := KeyFromBytes(request.PartitionKey)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case fwd.id != fwdID || !fwd.ack.equal(local) || !fwd.pkey.equal(pkey[:]) || !equalMessage(fwd.msg, request):
				t.Fatalf("unexpected fwd: %+v", fwd)
			}

			pubsub.Publish(ctx, localStream, marshalAck(ack{
				id: fwdID,
			}, nil))
			pubsub.Publish(ctx, localStream, marshalResp(resp{
				id:  reqID,
				msg: response,
			}, nil))
		}()

		msg, err := b.Request(ctx, request)
		switch {
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		case !equalMessage(msg, response):
			t.Fatalf("unexpected response: %+v", msg)
		case hasPendingAck(b, reqID):
			t.Fatalf("unexpected pending ack: %d", reqID)
		}
	})

	t.Run("forward-timeout", func(t *testing.T) {
		ackTimeout := b.ackTimeout
		b.ackTimeout = time.Millisecond
		defer func() { b.ackTimeout = ackTimeout }()

		requestChan := make(chan Message, 1)
		err := b.SubscribeRequest(ctx, request.Stream, func(_ context.Context, msg Message) Message {
			requestChan <- msg
			return response
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer clearSubscriptions(b, request.Stream)

		remote := intKey(0)
		reqID := lastID(b) + 1

		b.routing.register(keys(remote))

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(groupName, remote))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Fatalf("unexpected frame type: %s", frame.typ())
			}

			fwdID := lastID(b)
			pkey := KeyFromBytes(request.PartitionKey)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case fwd.id != fwdID || !fwd.ack.equal(local) || !fwd.pkey.equal(pkey[:]) || !equalMessage(fwd.msg, request):
				t.Fatalf("unexpected fwd: %+v", fwd)
			}

			msg := <-requestChan
			switch {
			case !equalMessage(msg, request):
				t.Fatalf("unexpected request message: %+v", msg)
			case containsKey(keys(b.routing.keys), remote):
				t.Fatalf("unexpected routing keys: %v", b.routing.keys)
			}
		}()

		msg, err := b.Request(ctx, request)
		switch {
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		case !equalMessage(msg, response):
			t.Fatalf("unexpected response: %+v", msg)
		case hasPendingAck(b, reqID):
			t.Fatalf("unexpected pending ack: %d", reqID)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()

		_, err := b.Request(ctx, request)
		if err != ctx.Err() {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()

		_, err := b.Request(ctx, request)
		if err != context.DeadlineExceeded {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestBrokerClose(t *testing.T) {
	const (
		groupName     = "testgroup"
		messageStream = "my-message-stream"
	)

	ctx := context.Background()
	now := time.Date(1988, time.September, 26, 13, 14, 15, 0, time.UTC)
	errch := make(chan error, 1)
	msg := Message{
		Stream:       messageStream,
		Time:         now,
		PartitionKey: []byte("partition one"),
		Data:         []byte("message data"),
	}

	pubsub := newPubsubRecorder()

	local := intKey(1)
	remote := KeyFromBytes(msg.PartitionKey)
	remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(groupName, remote[:]))
	defer remoteSub.Unsubscribe(ctx)

	b, err := NewBroker(ctx, pubsub,
		NodeKey(local.array()),
		Group(groupName),
		AckTimeout(time.Hour),            // disable ack timeouts
		StabilizationInterval(time.Hour), // disable stabilization
		ErrorHandler(func(err error) {
			errch <- err
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b.routing.register(remote[:])
	b.Subscribe(ctx, messageStream, func(_ context.Context, msg Message) {
		t.Fatal("unexpected message handler call")
	})

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var codec DefaultCodec
		pubsub.Publish(ctx, messageStream, codec.EncodeMessage(msg))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-remoteChan // wait for fwd frame
		if err := b.Close(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}()

	if err := <-errch; err != ErrClosed {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBrokerShutdown(t *testing.T) {
	const (
		groupName     = "testgroup"
		messageStream = "my-message-stream"
		requestStream = "my-request-stream"
	)

	ctx := context.Background()
	now := time.Date(1988, time.September, 26, 13, 14, 15, 0, time.UTC)
	local := intKey(1)
	pubsub := newPubsubRecorder()

	b, err := NewBroker(ctx, pubsub,
		NodeKey(local.array()),
		Group(groupName),
		AckTimeout(time.Hour),            // disable ack timeouts
		StabilizationInterval(time.Hour), // disable stabilization
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	receivedMessage := make(chan struct{})
	receivedRequest := make(chan struct{})
	finishedMessage := make(chan struct{})

	b.Subscribe(ctx, messageStream, func(_ context.Context, msg Message) {
		close(receivedMessage)
		<-finishedMessage
	})
	b.SubscribeRequest(ctx, requestStream, func(_ context.Context, msg Message) Message {
		close(receivedRequest)
		<-finishedMessage
		return msg
	})

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var codec DefaultCodec
		pubsub.Publish(ctx, messageStream, codec.EncodeMessage(Message{
			Stream: messageStream,
			Time:   now,
			Data:   []byte("message data"),
		}))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		pubsub.Publish(ctx, requestStream, marshalReq(req{
			id:    7,
			reply: b.routing.local,
			msg: Message{
				Stream: requestStream,
				Time:   now,
				Data:   []byte("request data"),
			},
		}, nil))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-receivedMessage
		<-receivedRequest
		if err := b.Shutdown(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(1 * time.Millisecond)
		close(finishedMessage)
	}()
}

func TestBrokerStabilization(t *testing.T) {
	const groupName = "testgroup"

	ctx := context.Background()

	local := intKey(1)
	localStream := nodeStream(groupName, local)

	remote := intKey(11)

	pubsub := newPubsubRecorder()
	remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(groupName, remote))
	defer remoteSub.Unsubscribe(ctx)

	newBroker := func(ackTimeout time.Duration) (*Broker, error) {
		b := &Broker{
			ackTimeout: ackTimeout,
			routing: newRoutingTable(options{
				nodeKey:         local,
				stabilizerCount: 1,
			}),
			pubsub: newPubSub(pubsub, options{
				groupName: groupName,
				nodeKey:   local,
			}),
			leaving:     make(chan struct{}),
			pendingAcks: make(map[uint64]pendingAck),
		}

		b.routing.register(keys(remote))

		err := b.pubsub.subscribeGroup(ctx, b.processGroupProtocol)
		return b, err
	}

	t.Run("ack", func(t *testing.T) {
		b, err := newBroker(time.Hour) // disable ack timeouts
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		b.wg.Add(1)
		go b.stabilize(time.Millisecond) // wg.Done is called by stabilize

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()

			frame := <-remoteChan
			close(b.leaving)
			if frame.typ() != frameTypePing {
				t.Fatalf("unexpected frame type for %v: %s", remote, frame.typ())
			}

			const pingID = 1

			ping, err := unmarshalPing(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case ping.id != pingID || !ping.sender.equal(local):
				t.Fatalf("unexpected ping: %+v", ping)
			}

			pubsub.Publish(ctx, localStream, marshalInfo(info{
				id:        pingID,
				neighbors: keys(remote),
			}, nil))

			switch {
			case hasPendingAck(b, pingID):
				t.Fatalf("unexpected pending ack: %d", pingID)
			case !containsKey(keys(b.routing.keys), remote):
				t.Fatalf("unexpected routing keys: %v", b.routing.keys)
			}
		}()

		b.wg.Wait()
	})

	t.Run("timeout", func(t *testing.T) {
		b, err := newBroker(time.Millisecond)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		const pingID = 1

		b.wg.Add(1)
		go b.stabilize(time.Millisecond) // wg.Done is called by stabilize

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()

			frame := <-remoteChan
			close(b.leaving)
			if frame.typ() != frameTypePing {
				t.Fatalf("unexpected frame type for %v: %s", remote, frame.typ())
			}

			ping, err := unmarshalPing(frame)
			switch {
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			case ping.id != pingID || !ping.sender.equal(local):
				t.Fatalf("unexpected ping: %+v", ping)
			}

		}()

		b.wg.Wait()

		switch {
		case hasPendingAck(b, pingID):
			t.Fatalf("unexpected pending ack: %d", pingID)
		case containsKey(keys(b.routing.keys), remote):
			t.Fatalf("unexpected routing keys: %v", b.routing.keys)
		}
	})
}

func clearSubscriptions(b *Broker, stream string) {
	b.pubsub.subsMtx.Lock()
	sub, has := b.pubsub.subs[stream]
	delete(b.pubsub.subs, stream)
	b.pubsub.subsMtx.Unlock()

	if has {
		sub.Unsubscribe(context.Background())
		b.handlerGroups.Delete(stream)
		b.requestHandlers.Delete(stream)
	}
}

func lastID(b *Broker) uint64 {
	return atomic.LoadUint64(&b.id)
}

func hasPendingAck(b *Broker, id uint64) bool {
	b.pendingAckMtx.Lock()
	_, has := b.pendingAcks[id]
	b.pendingAckMtx.Unlock()
	return has
}
