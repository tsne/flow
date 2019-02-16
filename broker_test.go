package flow

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerPublish(t *testing.T) {
	ctx := context.Background()
	pubsub := newPubsubRecorder()

	b, err := NewBroker(ctx, pubsub,
		disableStabilization(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = b.Publish(ctx, Message{
		Stream:       "store-stream",
		PartitionKey: []byte("partition one"),
		Data:         []byte("message data"),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBrokerSubscription(t *testing.T) {
	const clique = "test-clique"

	ctx := context.Background()
	msg := Message{
		Stream:       "message-stream",
		PartitionKey: []byte("partition one"),
		Data:         []byte("message data"),
	}

	local := KeyFromBytes(msg.PartitionKey)
	localStream := nodeStream(clique, local)

	messageChan := make(chan Message, 1)
	pubsub := newPubsubRecorder()
	b, err := NewBroker(ctx, pubsub,
		WithMessageHandler(msg.Stream, func(_ context.Context, msg Message) { messageChan <- msg }),
		WithPartition(clique, local),
		disableAckTimeouts(),
		disableReqTimeouts(),
		disableStabilization(),
	)
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

		b.routing.registerKey(remote)
		defer b.routing.unregister(remote)

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(clique, remote))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case fwd.id != fwdID || fwd.ack != local || !equalMsg(fwd.msg, newMsg(msg)):
				t.Errorf("unexpected fwd: %+v", fwd)
				t.Fail()
				return
			}

			pubsub.Publish(ctx, localStream, marshalAck(ack{
				id: fwdID,
			}))

			if hasPendingReply(b, fwdID) {
				t.Errorf("unexpected pending reply: %d", fwdID)
				t.Fail()
				return
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

		b.routing.registerKey(remote)
		defer b.routing.unregister(remote)

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(clique, remote))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case fwd.id != fwdID || fwd.ack != local || !equalMsg(fwd.msg, newMsg(msg)):
				t.Errorf("unexpected fwd: %+v", fwd)
				t.Fail()
				return
			}

			// dispatch locally
			receivedMsg := <-messageChan
			switch {
			case !equalMessage(receivedMsg, msg):
				t.Errorf("unexpected message: %+v", receivedMsg)
				t.Fail()
				return
			case hasPendingReply(b, fwdID):
				t.Errorf("unexpected pending reply: %d", fwdID)
				t.Fail()
				return
			case containsKey(b.routing.keys, remote):
				t.Errorf("unexpected routing keys: %v", b.routing.keys)
				t.Fail()
				return
			}
		}()

		err := b.Publish(ctx, msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestBrokerCliqueProtocol(t *testing.T) {
	const clique = "test-clique"

	ctx := context.Background()

	localKey := []byte("local")
	local := KeyFromBytes(localKey)
	localStream := nodeStream(clique, local)

	remoteKey := []byte("remote")
	remote := KeyFromBytes(remoteKey)
	remoteStream := nodeStream(clique, remote)

	pubsub := newPubsubRecorder()
	cliqueChan, cliqueSub := pubsub.SubscribeChan(ctx, clique)
	defer cliqueSub.Unsubscribe(ctx)

	remoteChan, remoteSub := pubsub.SubscribeChan(ctx, remoteStream)
	defer remoteSub.Unsubscribe(ctx)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		frame := <-cliqueChan
		if frame.typ() != frameTypeJoin {
			t.Errorf("unexpected frame type: %s", frame.typ())
			t.Fail()
			return
		}

		join, err := unmarshalJoin(frame)
		switch {
		case err != nil:
			t.Errorf("unexpected error: %v", err)
			t.Fail()
			return
		case join.sender != local:
			t.Errorf("unexpected join: %+v", join)
			t.Fail()
			return
		}

		pubsub.Publish(ctx, localStream, marshalInfo(info{
			id: 13,
			neighbors: keys{
				0, 0, 0, 0, 0, 0, 1, 1,
				0, 0, 0, 0, 0, 0, 1, 2,
				0, 0, 0, 0, 0, 0, 1, 3,
			},
		}))
	}()

	const stream = "mystream"
	incoming := make(chan Message, 1)
	b, err := NewBroker(ctx, pubsub,
		WithMessageHandler(stream, func(_ context.Context, m Message) { incoming <- m }),
		WithPartition(clique, local),
		disableAckTimeouts(),
		disableStabilization(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wg.Wait()

	if len(b.routing.keys) != 3 || !containsKey(b.routing.keys, 0x101) || !containsKey(b.routing.keys, 0x102) || !containsKey(b.routing.keys, 0x103) {
		t.Fatalf("unexpected routing keys: %v", b.routing.keys)
	}

	b.routing.unregister(0x101)
	b.routing.unregister(0x102)
	b.routing.unregister(0x103)

	if len(b.routing.keys) != 0 {
		t.Fatalf("unexpected routing keys: %v", b.routing.keys)
	}

	// --- join -->
	// <-- info ---
	t.Run("join", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		go func() {
			defer wg.Done()

			<-cliqueChan // consume JOIN
			frame := <-remoteChan
			if frame.typ() != frameTypeInfo {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			info, err := unmarshalInfo(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case info.id != 0 || info.neighbors.length() != 1 || info.neighbors.at(0) != b.routing.local:
				t.Errorf("unexpected info: %+v", info)
				t.Fail()
				return
			}
		}()

		pubsub.Publish(ctx, clique, marshalJoin(join{sender: remote}))

		if !containsKey(b.routing.keys, remote) {
			t.Fatalf("unexpected routing keys: %+v", b.routing.keys)
		}
	})

	// --- ping -->
	// <-- info ---
	t.Run("ping", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		id := nextID(b)
		expectedNeighbors := b.routing.neighbors()

		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeInfo {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			info, err := unmarshalInfo(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case info.id != id || !bytes.Equal(info.neighbors, expectedNeighbors):
				t.Errorf("unexpected info: %+v", info)
				t.Fail()
				return
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

		id := nextID(b)
		msg := Message{
			Stream:       stream,
			PartitionKey: nil,
			Data:         []byte("message data"),
		}

		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeAck {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			ack, err := unmarshalAck(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case ack.id != id:
				t.Errorf("unexpected ack: %+v", ack)
				t.Fail()
				return
			}
		}()

		pubsub.Publish(ctx, localStream, marshalFwd(fwd{
			id:  id,
			ack: remote,
			msg: newMsg(msg),
		}))

		if in := <-incoming; !equalMessage(in, msg) {
			t.Fatalf("unexpected incoming message: %+v", in)
		}
	})

	// --- fwd -->
	// <-- ack ---
	//              --- fwd -->
	//              <-- ack ---
	t.Run("forward", func(t *testing.T) {
		b.routing.registerKey(remote)
		defer b.routing.unregister(remote)

		wg.Add(1)
		defer wg.Wait()

		id := nextID(b)
		msg := Message{
			Stream:       stream,
			PartitionKey: remoteKey,
			Data:         []byte("message data"),
		}

		go func() {
			defer wg.Done()

			// ack
			frame := <-remoteChan
			if frame.typ() != frameTypeAck {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			sentAck, err := unmarshalAck(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case sentAck.id != id || len(sentAck.data) != 0:
				t.Errorf("unexpected ack: %+v", sentAck)
				t.Fail()
				return
			}

			// fwd
			frame = <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case fwd.id != fwdID || fwd.ack != local || !equalMsg(fwd.msg, newMsg(msg)):
				t.Errorf("unexpected fwd: %+v", fwd)
				t.Fail()
				return
			}

			pubsub.Publish(ctx, localStream, marshalAck(ack{
				id: fwdID,
			}))

			if hasPendingReply(b, fwdID) {
				t.Errorf("unexpected pending reply: %d", fwdID)
				t.Fail()
				return
			}
		}()

		pubsub.Publish(ctx, localStream, marshalFwd(fwd{
			id:  id,
			ack: remote,
			msg: newMsg(msg),
		}))

	})

	// --- fwd -->
	// <-- ack ---
	//              --- fwd -->
	//                timeout
	//                dispatch
	t.Run("forward-timeout", func(t *testing.T) {
		b.routing.registerKey(remote)
		defer b.routing.unregister(remote)

		wg.Add(1)
		defer wg.Wait()

		id := nextID(b)
		msg := Message{
			Stream:       "mystream",
			PartitionKey: remoteKey,
			Data:         []byte("message data"),
		}

		go func() {
			defer wg.Done()

			ackTimeout := b.ackTimeout
			b.ackTimeout = time.Millisecond
			defer func() { b.ackTimeout = ackTimeout }()

			// ack
			frame := <-remoteChan
			if frame.typ() != frameTypeAck {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			sentAck, err := unmarshalAck(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case sentAck.id != id:
				t.Errorf("unexpected ack: %+v", sentAck)
				t.Fail()
				return
			}

			// fwd
			frame = <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case fwd.id != fwdID || fwd.ack != local || !equalMsg(fwd.msg, newMsg(msg)):
				t.Errorf("unexpected fwd: %+v", fwd)
				t.Fail()
				return
			}

			// dispatch
			dispatchedMsg := <-incoming
			switch {
			case !equalMessage(dispatchedMsg, msg):
				t.Errorf("unexpected dispatched message: %+v", dispatchedMsg)
				t.Fail()
				return
			case containsKey(b.routing.keys, remote):
				t.Errorf("unexpected routing keys: %v", b.routing.keys)
				t.Fail()
				return
			}
		}()

		pubsub.Publish(ctx, localStream, marshalFwd(fwd{
			id:  id,
			ack: remote,
			msg: newMsg(msg),
		}))
	})

	// leave
	t.Run("leave", func(t *testing.T) {
		wg.Add(1)
		defer wg.Wait()

		go func() {
			defer wg.Done()

			<-cliqueChan // consume LEAV
		}()

		pubsub.Publish(ctx, clique, marshalLeave(leave{node: remote}))

		if containsKey(b.routing.keys, remote) {
			t.Fatalf("unexpected routing keys: %+v", b.routing.keys)
		}
	})
}

func TestBrokerRequest(t *testing.T) {
	const clique = "test-clique"

	ctx := context.Background()

	local := Key(1)
	localStream := nodeStream(clique, local)

	request := Message{
		Stream:       "myrequests",
		PartitionKey: []byte("partition one"),
		Data:         []byte("request data"),
	}

	response := Message{
		Data: []byte("response data"),
	}

	var requestHandler RequestHandler // set in the sub-tests

	pubsub := newPubsubRecorder()
	b, err := NewBroker(ctx, pubsub,
		WithRequestHandler(request.Stream, func(ctx context.Context, msg Message) Message {
			return requestHandler(ctx, msg)
		}),
		WithPartition(clique, local),
		disableAckTimeouts(),
		disableReqTimeouts(),
		disableStabilization(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// --- msg -->
	//              dispatch
	// <-- ack ---
	t.Run("dispatch", func(t *testing.T) {
		requestHandler = func(_ context.Context, msg Message) Message {
			if !equalMessage(msg, request) {
				t.Fatalf("unexpected request: %+v", msg)
			}
			return response
		}

		msg, err := b.Request(ctx, request)
		switch {
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		case !equalMessage(msg, response):
			t.Fatalf("unexpected response: %+v", msg)
		}
	})

	// --- msg -->
	//               --- fwd -->
	//               <-- ack ---
	//                            dispatch
	// <--------- ack ----------
	t.Run("forward", func(t *testing.T) {
		requestHandler = func(_ context.Context, msg Message) Message {
			t.Fatal("unexpected request handler call")
			return msg
		}

		remote := Key(0)
		reqID := lastID(b) + 1

		b.routing.registerKey(remote)
		defer b.routing.unregister(remote)

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(clique, remote))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case fwd.id != fwdID || fwd.ack != local || !equalMsg(fwd.msg, newMsg(request)):
				t.Errorf("unexpected fwd: %+v", fwd)
				t.Fail()
				return
			}

			pubsub.Publish(ctx, localStream, marshalAck(ack{
				id: fwdID,
			}))
			pubsub.Publish(ctx, localStream, marshalAck(ack{
				id:   reqID,
				data: response.Data,
			}))
		}()

		msg, err := b.Request(ctx, request)
		switch {
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		case !equalMessage(msg, response):
			t.Fatalf("unexpected response: %+v", msg)
		case hasPendingReply(b, reqID):
			t.Fatalf("unexpected pending reply: %d", reqID)
		}
	})

	// --- msg -->
	//                       --- fwd -->
	//                         timeout
	//              dispatch
	// <-- ack ---
	t.Run("forward-timeout", func(t *testing.T) {
		ackTimeout := b.ackTimeout
		b.ackTimeout = time.Millisecond
		defer func() { b.ackTimeout = ackTimeout }()

		requestChan := make(chan Message, 1)
		requestHandler = func(_ context.Context, msg Message) Message {
			requestChan <- msg
			return response
		}

		remote := Key(0)
		reqID := lastID(b) + 1

		b.routing.registerKey(remote)

		remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(clique, remote))
		defer remoteSub.Unsubscribe(ctx)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(1)
		go func() {
			defer wg.Done()

			frame := <-remoteChan
			if frame.typ() != frameTypeFwd {
				t.Errorf("unexpected frame type: %s", frame.typ())
				t.Fail()
				return
			}

			fwdID := lastID(b)
			fwd, err := unmarshalFwd(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case fwd.id != fwdID || fwd.ack != local || !equalMsg(fwd.msg, newMsg(request)):
				t.Errorf("unexpected fwd: %+v", fwd)
				t.Fail()
				return
			}

			msg := <-requestChan
			switch {
			case !equalMessage(msg, request):
				t.Errorf("unexpected request message: %+v", msg)
				t.Fail()
				return
			case containsKey(b.routing.keys, remote):
				t.Errorf("unexpected routing keys: %+v", b.routing.keys)
				t.Fail()
				return
			}
		}()

		msg, err := b.Request(ctx, request)
		switch {
		case err != nil:
			t.Fatalf("unexpected error: %v", err)
		case !equalMessage(msg, response):
			t.Fatalf("unexpected response: %+v", msg)
		case hasPendingReply(b, reqID):
			t.Fatalf("unexpected pending reply: %d", reqID)
		}
	})

	// --- msg -->
	//   timeout
	t.Run("timeout", func(t *testing.T) {
		reqTimeout := b.reqTimeout
		b.reqTimeout = time.Millisecond
		defer func() { b.reqTimeout = reqTimeout }()

		req := request
		req.Stream = "non-existing-stream"

		_, err := b.Request(ctx, req)
		if err != ErrTimeout {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("deadline-exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()

		req := request
		req.Stream = "non-existing-stream"

		_, err := b.Request(ctx, req)
		if err != ctx.Err() {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestBrokerClose(t *testing.T) {
	const (
		clique        = "test-clique"
		messageStream = "my-message-stream"
	)

	ctx := context.Background()
	errch := make(chan error, 1)
	msg := Message{
		Stream:       messageStream,
		PartitionKey: []byte("partition one"),
		Data:         []byte("message data"),
	}

	pubsub := newPubsubRecorder()

	local := Key(1)
	remote := KeyFromBytes(msg.PartitionKey)
	remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(clique, remote))
	defer remoteSub.Unsubscribe(ctx)

	b, err := NewBroker(ctx, pubsub,
		WithMessageHandler(messageStream, func(_ context.Context, msg Message) {
			t.Fatal("unexpected message handler call")
		}),
		WithPartition(clique, local),
		disableAckTimeouts(),
		disableStabilization(),
		WithErrorHandler(func(err error) { errch <- err }),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b.routing.registerKey(remote)

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
			t.Errorf("unexpected error: %v", err)
			t.Fail()
			return
		}
	}()

	if err := <-errch; err != ErrClosed {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBrokerShutdown(t *testing.T) {
	const (
		clique        = "test-clique"
		messageStream = "my-message-stream"
		requestStream = "my-request-stream"
	)

	ctx := context.Background()
	local := Key(1)
	pubsub := newPubsubRecorder()

	receivedMessage := make(chan struct{})
	receivedRequest := make(chan struct{})
	finishedMessage := make(chan struct{})

	b, err := NewBroker(ctx, pubsub,
		WithMessageHandler(messageStream, func(_ context.Context, msg Message) {
			close(receivedMessage)
			<-finishedMessage
		}),
		WithRequestHandler(requestStream, func(_ context.Context, msg Message) Message {
			close(receivedRequest)
			<-finishedMessage
			return msg
		}),
		WithPartition(clique, local),
		disableAckTimeouts(),
		disableStabilization(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var codec DefaultCodec
		pubsub.Publish(ctx, messageStream, codec.EncodeMessage(Message{
			Stream: messageStream,
			Data:   []byte("message data"),
		}))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		pubsub.Publish(ctx, requestStream, marshalMsg(msg{
			id:     7,
			reply:  []byte(nodeStream(b.clique, b.routing.local)),
			stream: []byte(requestStream),
			data:   []byte("request data"),
		}))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-receivedMessage
		<-receivedRequest
		if err := b.Shutdown(ctx); err != nil {
			t.Errorf("unexpected error: %v", err)
			t.Fail()
			return
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
	const clique = "test-clique"

	ctx := context.Background()

	local := Key(1)
	localStream := nodeStream(clique, local)

	remote := Key(11)

	pubsub := newPubsubRecorder()
	remoteChan, remoteSub := pubsub.SubscribeChan(ctx, nodeStream(clique, remote))
	defer remoteSub.Unsubscribe(ctx)

	newBroker := func(ackTimeout time.Duration) (*Broker, error) {
		b := &Broker{
			clique:     clique,
			ackTimeout: ackTimeout,
			routing: newRoutingTable(options{
				nodeKey:       local,
				stabilization: Stabilization{Stabilizers: 1},
			}),
			pubsub:         newPubSub(pubsub, options{}),
			leaving:        make(chan struct{}),
			pendingReplies: make(map[uint64]pendingReply),
		}

		b.routing.registerKey(remote)

		// subscribe clique
		err := b.pubsub.subscribe(ctx, nodeStream(clique, local), "", b.processCliqueProtocol)
		if err != nil {
			return nil, err
		}

		err = b.pubsub.subscribe(ctx, clique, "", b.processCliqueProtocol)
		if err != nil {
			return nil, err
		}

		return b, nil
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
				t.Errorf("unexpected frame type for %v: %s", remote, frame.typ())
				t.Fail()
				return
			}

			const pingID = 1

			ping, err := unmarshalPing(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case ping.id != pingID || ping.sender != local:
				t.Errorf("unexpected ping: %+v", ping)
				t.Fail()
				return
			}

			pubsub.Publish(ctx, localStream, marshalInfo(info{
				id:        pingID,
				neighbors: keys{0, 0, 0, 0, 0, 0, 0, byte(remote)},
			}))

			switch {
			case hasPendingReply(b, pingID):
				t.Errorf("unexpected pending reply: %d", pingID)
				t.Fail()
				return
			case !containsKey(b.routing.keys, remote):
				t.Errorf("unexpected routing keys: %+v", b.routing.keys)
				t.Fail()
				return
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
				t.Errorf("unexpected frame type for %v: %s", remote, frame.typ())
				t.Fail()
				return
			}

			ping, err := unmarshalPing(frame)
			switch {
			case err != nil:
				t.Errorf("unexpected error: %v", err)
				t.Fail()
				return
			case ping.id != pingID || ping.sender != local:
				t.Errorf("unexpected ping: %+v", ping)
				t.Fail()
				return
			}

		}()

		b.wg.Wait()

		switch {
		case hasPendingReply(b, pingID):
			t.Fatalf("unexpected pending reply: %d", pingID)
		case containsKey(b.routing.keys, remote):
			t.Fatalf("unexpected routing keys: %+v", b.routing.keys)
		}
	})
}

func nextID(b *Broker) uint64 {
	return atomic.AddUint64(&b.id, 1)
}

func lastID(b *Broker) uint64 {
	return atomic.LoadUint64(&b.id)
}

func hasPendingReply(b *Broker, id uint64) bool {
	b.pendingRepliesMtx.Lock()
	_, has := b.pendingReplies[id]
	b.pendingRepliesMtx.Unlock()
	return has
}

func disableStabilization() Option {
	return WithStabilization(Stabilization{Interval: time.Hour})
}

func disableAckTimeouts() Option {
	return WithAckTimeout(time.Hour)
}

func disableReqTimeouts() Option {
	return WithRequestTimeout(time.Hour)
}
