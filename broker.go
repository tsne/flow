package flow

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// MessageHandler represents a callback function for handling incoming messages.
// The binary parts of the passed message should be assumed to be valid
// only during the function call. It is the handler's responsibility to
// copy the data which should be reused.
type MessageHandler func(context.Context, Message)

// RequestHandler represents a callback function for handling incoming requests.
// The binary parts of the passed message should be assumed to be valid
// only during the function call. It is the handler's responsibility to
// copy the data which should be reused.
type RequestHandler func(context.Context, Message) Message

type pendingAck struct {
	receiver key
	errch    chan error
}

// Broker represents a single node within a group. It enables
// the publishing and subscribing capabilities of the pub/sub
// system. Each subscribed message is handled by the responsible
// broker, which is determined by the respective node key.
type Broker struct {
	messagesInFlight uint64
	requestsInFlight uint64
	shuttingDown     uint64

	codec      Codec
	onError    func(error)
	ackTimeout time.Duration

	routing routingTable
	pubsub  pubsub
	id      uint64

	wg      sync.WaitGroup
	leaving chan struct{}

	pendingAckMtx sync.Mutex
	pendingAcks   map[uint64]pendingAck // id => pending ack

	pendingRespMtx sync.Mutex
	pendingResps   map[uint64]chan Message // id => response channel

	handlerGroups   sync.Map // stream => handler group
	requestHandlers sync.Map // stream => request handler
}

// NewBroker creates a new broker which uses the pub/sub system
// for publishing messages and subscribing to streams.
//
// Because pubsub could possibly be shared between multiple brokers,
// the caller of this function is responsible for closing all
// connections to the pub/sub system.
func NewBroker(ctx context.Context, pubsub PubSub, o ...Option) (*Broker, error) {
	opts := defaultOptions()
	if err := opts.apply(o...); err != nil {
		return nil, err
	}

	b := &Broker{
		codec:        opts.codec,
		onError:      opts.errorHandler,
		ackTimeout:   opts.ackTimeout,
		routing:      newRoutingTable(opts),
		pubsub:       newPubSub(pubsub, opts),
		leaving:      make(chan struct{}),
		pendingAcks:  make(map[uint64]pendingAck),
		pendingResps: make(map[uint64]chan Message),
	}

	err := b.pubsub.subscribeGroup(ctx, b.processGroupProtocol)
	if err != nil {
		return nil, err
	}

	join := marshalJoin(join{sender: b.routing.local}, nil)
	if err := b.pubsub.sendToGroup(ctx, join); err != nil {
		b.pubsub.unsubscribeAll(ctx)
		return nil, err
	}

	b.wg.Add(1)
	go b.stabilize(opts.stabilizationInterval)
	return b, nil
}

// Close notifies all group members about a leaving broker and
// disconnects from the pub/sub system.
func (b *Broker) Close() error {
	return b.shutdown(context.Background(), func() error { return nil })
}

// Shutdown gracefully shuts down the broker. It notifies all group
// members about a leaving broker and waits until all messages and
// requests are processed. If the given context expires before, the
// context's error will be returned.
func (b *Broker) Shutdown(ctx context.Context) error {
	return b.shutdown(ctx, func() error {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for atomic.LoadUint64(&b.messagesInFlight) != 0 || atomic.LoadUint64(&b.requestsInFlight) != 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
		return nil
	})
}

// Publish persists the message and publishes it to the pub/sub system.
// If the message has no partition key, the message will be processed
// by a random broker within a group.
// All binary data of the passed message needs to be valid only during
// the method call.
func (b *Broker) Publish(ctx context.Context, msg Message) error {
	if b.isShuttingDown() {
		return ErrClosed
	}
	if err := msg.validate(); err != nil {
		return err
	}
	return b.pubsub.publish(ctx, msg.Stream, b.codec.EncodeMessage(msg))
}

// Request sends a request message and waits for its response.
// If the message has no partition key, the request will be processed
// by a random broker within a group.
// All binary data of the passed message needs to be valid only during
// the method call.
func (b *Broker) Request(ctx context.Context, msg Message) (Message, error) {
	if b.isShuttingDown() {
		return Message{}, ErrClosed
	}
	if err := msg.validate(); err != nil {
		return Message{}, err
	}

	return b.sendRequest(ctx, msg.Stream, req{
		id:    b.nextID(),
		reply: b.routing.local,
		msg:   msg,
	})
}

// Subscribe subscribes to the messages of the specified stream.
// These messsages are partitioned within the group the broker is
// assigned to.
func (b *Broker) Subscribe(ctx context.Context, stream string, handler MessageHandler) error {
	g, has := b.handlerGroups.LoadOrStore(stream, &handlerGroup{})
	g.(*handlerGroup).add(handler)
	if has {
		return nil
	}
	return b.pubsub.subscribe(ctx, stream, b.processMessage)
}

// SubscribeRequest subscribes to the requests of the specified stream.
// These requests are partitioned within the group the broker is
// assigned to.
func (b *Broker) SubscribeRequest(ctx context.Context, stream string, handler RequestHandler) error {
	_, has := b.requestHandlers.LoadOrStore(stream, handler)
	if has {
		return errorf("already subscribed to '%s'", stream)
	}
	return b.pubsub.subscribe(ctx, stream, b.processRequest)
}

func (b *Broker) processMessage(ctx context.Context, stream string, data []byte) {
	atomic.AddUint64(&b.messagesInFlight, 1)
	defer atomic.AddUint64(&b.messagesInFlight, ^uint64(0))

	var (
		req req
		err error
	)
	req.msg, err = b.codec.DecodeMessage(stream, data)
	if err != nil {
		b.onError(errorf("unmarshal message: %v", err))
		return
	}

	if len(req.msg.PartitionKey) == 0 {
		b.dispatchMsg(ctx, req)
	} else {
		pkey := KeyFromBytes(req.msg.PartitionKey)
		b.forward(ctx, req, pkey[:], b.dispatchMsg)
	}
}

func (b *Broker) processRequest(ctx context.Context, stream string, data []byte) {
	atomic.AddUint64(&b.requestsInFlight, 1)
	defer atomic.AddUint64(&b.requestsInFlight, ^uint64(0))

	frame, err := frameFromBytes(data)
	switch {
	case err != nil:
		b.onError(errorf("request subscription: %v", err))
		return
	case frame.typ() != frameTypeReq:
		b.onError(errorf("unexpected request frame type: %s", frame.typ()))
		return
	}

	b.handleReq(ctx, frame)
}

func (b *Broker) processGroupProtocol(ctx context.Context, stream string, data []byte) {
	frame, err := frameFromBytes(data)
	if err != nil {
		b.onError(errorf("group subscription: %v", err))
		return
	}

	switch frame.typ() {
	case frameTypeJoin:
		b.handleJoin(ctx, frame)
	case frameTypeLeave:
		b.handleLeave(frame)
	case frameTypeInfo:
		b.handleInfo(frame)
	case frameTypePing:
		b.handlePing(ctx, frame)
	case frameTypeFwd:
		b.handleFwd(ctx, frame)
	case frameTypeAck:
		b.handleAck(frame)
	case frameTypeResp:
		b.handleResp(frame)
	default:
		b.onError(errorf("unexpected group frame type: %s", frame.typ()))
	}
}

func (b *Broker) handleJoin(ctx context.Context, frame frame) {
	join, err := unmarshalJoin(frame)
	switch {
	case err != nil:
		b.onError(errorf("unmarshal join: %v", err))
		return
	case join.sender.equal(b.routing.local):
		return
	}

	neighbors := b.routing.neighbors(nil)
	b.routing.register(keys(join.sender))

	sender := join.sender.array()
	b.sendTo(ctx, sender[:], marshalInfo(info{
		neighbors: neighbors,
	}, frame))
}

func (b *Broker) handleLeave(frame frame) {
	leave, err := unmarshalLeave(frame)
	if err != nil {
		b.onError(errorf("unmarshal leave: %v", err))
		return
	}

	b.routing.unregister(leave.node)
}

func (b *Broker) handleInfo(frame frame) {
	info, err := unmarshalInfo(frame)
	if err != nil {
		b.onError(errorf("unmarshal info: %v", err))
		return
	}

	b.routing.register(info.neighbors)
	b.notifyAck(info.id, nil)
}

func (b *Broker) handlePing(ctx context.Context, frame frame) {
	ping, err := unmarshalPing(frame)
	if err != nil {
		b.onError(errorf("unmarshal ping: %v", err))
		return
	}

	sender := ping.sender.array()
	b.sendTo(ctx, sender[:], marshalInfo(info{
		id:        ping.id,
		neighbors: b.routing.neighbors(nil),
	}, frame))
}

func (b *Broker) handleFwd(ctx context.Context, frame frame) {
	fwd, err := unmarshalFwd(frame)
	if err != nil {
		b.onError(errorf("unmarshal fwd: %v", err))
		return
	}

	b.sendTo(ctx, fwd.ack, marshalAck(ack{
		id: fwd.id,
	}, nil))

	b.forward(ctx, req{msg: fwd.msg}, fwd.pkey, b.dispatchMsg)
}

func (b *Broker) handleAck(frame frame) {
	ack, err := unmarshalAck(frame)
	if err != nil {
		b.onError(errorf("unmarshal ack: %v", err))
		return
	}

	b.notifyAck(ack.id, nil)
}

func (b *Broker) handleReq(ctx context.Context, frame frame) {
	req, err := unmarshalReq(frame)
	if err != nil {
		b.onError(errorf("unmarshal req: %v", err))
		return
	}

	if len(req.msg.PartitionKey) == 0 {
		b.dispatchReq(ctx, req)
	} else {
		pkey := KeyFromBytes(req.msg.PartitionKey)
		b.forward(ctx, req, pkey[:], b.dispatchReq)
	}
}

func (b *Broker) handleResp(frame frame) {
	resp, err := unmarshalResp(frame)
	if err != nil {
		b.onError(errorf("unmarshal resp: %v", err))
		return
	}

	b.notifyResp(resp.id, resp.msg)
}

func (b *Broker) dispatchMsg(ctx context.Context, req req) {
	if g, has := b.handlerGroups.Load(req.msg.Stream); has {
		g.(*handlerGroup).dispatch(ctx, req.msg)
	}
}

func (b *Broker) dispatchReq(ctx context.Context, req req) {
	if h, has := b.requestHandlers.Load(req.msg.Stream); has {
		b.sendTo(ctx, req.reply, marshalResp(resp{
			id:  req.id,
			msg: h.(RequestHandler)(ctx, req.msg),
		}, nil))
	}
}

func (b *Broker) forward(ctx context.Context, req req, pkey key, dispatch func(context.Context, req)) {
	var (
		id       uint64
		fwdframe frame
		keybuf   Key
	)
	for {
		succ := b.routing.successor(pkey[:], keybuf[:])
		if len(succ) == 0 {
			dispatch(ctx, req)
			return
		}

		if len(fwdframe) == 0 {
			id = b.nextID()
			fwdframe = marshalFwd(fwd{
				id:   id,
				ack:  b.routing.local,
				pkey: pkey[:],
				msg:  req.msg,
			}, nil)
		}

		err := b.sendFrame(ctx, id, succ, fwdframe)
		if err == nil {
			return
		} else if err != context.DeadlineExceeded {
			b.onError(err)
			return
		}

		// The node was suspected and removed from the
		// valid keys. We look for the next successor
		// to handle the message.
	}
}

// send a frame to a node and wait for an ack
func (b *Broker) sendFrame(ctx context.Context, ackID uint64, receiver key, frame frame) error {
	errch := make(chan error, 1)
	b.pendingAckMtx.Lock()
	b.pendingAcks[ackID] = pendingAck{
		receiver: receiver,
		errch:    errch,
	}
	b.pendingAckMtx.Unlock()

	ctx, cancel := context.WithTimeout(ctx, b.ackTimeout)
	defer cancel()

	if err := b.pubsub.sendToNode(ctx, receiver, frame); err != nil {
		b.notifyAck(ackID, err)
	}

	select {
	case err := <-errch:
		return err
	case <-ctx.Done():
		err := ctx.Err()
		b.notifyAck(ackID, err)
		return err
	}
}

func (b *Broker) notifyAck(ackID uint64, err error) {
	b.pendingAckMtx.Lock()
	ack, has := b.pendingAcks[ackID]
	delete(b.pendingAcks, ackID)
	b.pendingAckMtx.Unlock()

	if has {
		if err == context.DeadlineExceeded {
			b.routing.suspect(ack.receiver)
		}
		ack.errch <- err
	}
}

// send a request to a node and wait for a response
func (b *Broker) sendRequest(ctx context.Context, stream string, req req) (Message, error) {
	msgch := make(chan Message, 1)
	b.pendingRespMtx.Lock()
	b.pendingResps[req.id] = msgch
	b.pendingRespMtx.Unlock()

	err := b.pubsub.publish(ctx, stream, marshalReq(req, nil))
	if err != nil {
		b.notifyResp(req.id, Message{})
	}

	select {
	case msg := <-msgch:
		return msg, err
	case <-ctx.Done():
		b.notifyResp(req.id, Message{})
		return Message{}, ctx.Err()
	}
}

func (b *Broker) notifyResp(reqID uint64, msg Message) {
	b.pendingRespMtx.Lock()
	msgch, has := b.pendingResps[reqID]
	delete(b.pendingResps, reqID)
	b.pendingRespMtx.Unlock()

	if has {
		msgch <- msg
	}
}

func (b *Broker) sendTo(ctx context.Context, target key, frame frame) {
	if err := b.pubsub.sendToNode(ctx, target, frame); err != nil {
		b.onError(errorf("send to node: %v", err))
	}
}

func (b *Broker) nextID() uint64 {
	return atomic.AddUint64(&b.id, 1)
}

func (b *Broker) isShuttingDown() bool {
	return atomic.LoadUint64(&b.shuttingDown) != 0
}

func (b *Broker) shutdown(ctx context.Context, wait func() error) error {
	atomic.StoreUint64(&b.shuttingDown, 1)
	close(b.leaving)

	leave := marshalLeave(leave{node: b.routing.local}, nil)
	err := b.pubsub.sendToGroup(ctx, leave)

	if waitErr := wait(); waitErr != nil {
		err = waitErr
	}

	b.pubsub.unsubscribeAll(ctx)

	// cancel pending acks
	b.pendingAckMtx.Lock()
	ids := make([]uint64, 0, len(b.pendingAcks))
	for id := range b.pendingAcks {
		ids = append(ids, id)
	}
	b.pendingAckMtx.Unlock()
	for _, id := range ids {
		b.notifyAck(id, ErrClosed)
	}

	b.wg.Wait()
	return err
}

func (b *Broker) stabilize(interval time.Duration) {
	defer b.wg.Done()

	ping := ping{sender: b.routing.local}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		frame  frame
		stabs  keys
		nstabs int
	)

	for {
		select {
		case <-b.leaving:
			return
		case <-ticker.C:
		}

		stabs = b.routing.stabilizers(stabs)
		nstabs = stabs.length()

		for i := 0; i < nstabs; i++ {
			stab := stabs.at(i)
			ping.id = b.nextID()
			frame = marshalPing(ping, frame)

			err := b.sendFrame(context.Background(), ping.id, stab, frame)
			if err != nil && err != ErrClosed && err != context.DeadlineExceeded {
				b.onError(errorf("stabilization: %v", err))
			}
		}
	}
}

type handlerGroup struct {
	mtx      sync.RWMutex
	handlers []MessageHandler
}

func (g *handlerGroup) add(h MessageHandler) {
	g.mtx.Lock()
	g.handlers = append(g.handlers, h)
	g.mtx.Unlock()
}

func (g *handlerGroup) dispatch(ctx context.Context, msg Message) {
	g.mtx.RLock()
	handlers := g.handlers
	g.mtx.RUnlock()

	for _, h := range handlers {
		h(ctx, msg)
	}
}
