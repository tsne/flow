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
type MessageHandler func(Message)

// RequestHandler represents a callback function for handling incoming requests.
// The binary parts of the passed message should be assumed to be valid
// only during the function call. It is the handler's responsibility to
// copy the data which should be reused.
type RequestHandler func(Message) Message

type pendingAck struct {
	receiver key
	timer    *time.Timer
	errch    chan error
}

// Broker represents a single node within a group. It enables
// the publishing and subscribing capabilities of the pub/sub
// system. Each subscribed message is handled by the responsible
// broker, which is determined by the respective node key.
type Broker struct {
	codec       Codec
	onError     func(error)
	ackTimeout  time.Duration
	respTimeout time.Duration

	routing routingTable
	pubsub  pubsub
	store   Store
	id      uint64

	wg      sync.WaitGroup
	closing chan struct{}

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
// Because the PubSub interface does not contain a close operation,
// the caller of this function is responsible for closing all
// connections to the pub/sub system.
func NewBroker(pubsub PubSub, o ...Option) (*Broker, error) {
	opts := defaultOptions()
	if err := opts.apply(o...); err != nil {
		return nil, err
	}

	b := &Broker{
		codec:        opts.codec,
		onError:      opts.errorHandler,
		ackTimeout:   opts.ackTimeout,
		respTimeout:  opts.respTimeout,
		routing:      newRoutingTable(opts),
		pubsub:       newPubSub(pubsub, opts),
		store:        opts.store,
		closing:      make(chan struct{}),
		pendingAcks:  make(map[uint64]pendingAck),
		pendingResps: make(map[uint64]chan Message),
	}

	err := b.pubsub.subscribeGroup(b.processGroupProtocol)
	if err != nil {
		return nil, err
	}

	join := marshalJoin(join{sender: b.routing.local}, nil)
	if err := b.pubsub.sendToGroup(join); err != nil {
		b.pubsub.shutdown()
		return nil, err
	}

	b.wg.Add(1)
	go b.stabilize(opts.stabilizationInterval)
	return b, nil
}

// Close notifies all group members about a leaving broker and
// diconnects from the pub/sub system.
func (b *Broker) Close() error {
	close(b.closing)

	// send leave message
	leave := marshalLeave(leave{node: b.routing.local}, nil)
	if err := b.pubsub.sendToGroup(leave); err != nil {
		b.onError(errorf("group broadcast: %v", err))
	}

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

	// shutdown pub/sub
	b.pubsub.shutdown()
	return nil
}

// Publish persists the message and publishes it to the pub/sub system.
// If the message has no partition key, the message will be processed
// by a random broker within a group.
// All binary data of the passed message needs to be valid only during
// the method call.
func (b *Broker) Publish(msg Message) error {
	if err := msg.validate(); err != nil {
		return err
	}

	if err := b.store.Store(msg); err != nil {
		return err
	}
	return b.pubsub.publish(msg.Stream, b.codec.EncodeMessage(msg))
}

// Request sends a request message and waits for its response.
// If the message has no partition key, the request will be processed
// by a random broker within a group.
// All binary data of the passed message needs to be valid only during
// the method call.
func (b *Broker) Request(ctx context.Context, msg Message) (Message, error) {
	if err := msg.validate(); err != nil {
		return Message{}, err
	}

	id := b.nextID()
	msgch := make(chan Message, 1)

	b.awaitResp(id, msgch)
	err := b.pubsub.publish(msg.Stream, marshalReq(req{
		id:    id,
		reply: b.routing.local,
		msg:   msg,
	}, nil))
	if err != nil {
		b.notifyResp(id, Message{})
	}

	timer := startTimer(b.respTimeout)
	defer stopTimer(timer)

	select {
	case <-timer.C:
		return Message{}, ErrTimeout
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case resp := <-msgch:
		return resp, err
	}
}

// Subscribe subscribes to the messages of the specified stream.
// These messsages are partitioned within the group the broker is
// assigned to.
func (b *Broker) Subscribe(stream string, handler MessageHandler) error {
	g, has := b.handlerGroups.LoadOrStore(stream, &handlerGroup{})
	g.(*handlerGroup).add(handler)
	if has {
		return nil
	}
	return b.pubsub.subscribe(stream, b.processMessage)
}

// SubscribeRequest subscribes to the requests if the specified stream.
// These requests are partitioned within the group the broker is
// assigned to.
func (b *Broker) SubscribeRequest(stream string, handler RequestHandler) error {
	_, has := b.requestHandlers.LoadOrStore(stream, handler)
	if has {
		return errorf("already subscribed to '%s'", stream)
	}
	return b.pubsub.subscribe(stream, b.processRequest)
}

func (b *Broker) processMessage(stream string, data []byte) {
	var (
		req req
		err error
	)
	req.msg, err = b.codec.DecodeMessage(stream, data)
	if err != nil {
		b.onError(errorf("unmarshal message: %v", err))
		return
	}

	var (
		buf  Key
		pkey key
	)
	if len(req.msg.PartitionKey) != 0 {
		buf = BytesKey(req.msg.PartitionKey)
		pkey = buf[:]
	}
	b.forward(req, pkey, b.dispatchMsg)
}

func (b *Broker) processRequest(stream string, data []byte) {
	frame, err := frameFromBytes(data)
	switch {
	case err != nil:
		b.onError(errorf("request subscription: %v", err))
		return
	case frame.typ() != frameTypeReq:
		b.onError(errorf("unexpected request frame type: %s", frame.typ()))
		return
	}

	b.handleReq(frame)
}

func (b *Broker) processGroupProtocol(stream string, data []byte) {
	frame, err := frameFromBytes(data)
	if err != nil {
		b.onError(errorf("group subscription: %v", err))
		return
	}

	switch frame.typ() {
	case frameTypeJoin:
		b.handleJoin(frame)
	case frameTypeLeave:
		b.handleLeave(frame)
	case frameTypeInfo:
		b.handleInfo(frame)
	case frameTypePing:
		b.handlePing(frame)
	case frameTypeFwd:
		b.handleFwd(frame)
	case frameTypeAck:
		b.handleAck(frame)
	case frameTypeResp:
		b.handleResp(frame)
	default:
		b.onError(errorf("unexpected group frame type: %s", frame.typ()))
	}
}

func (b *Broker) handleJoin(frame frame) {
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
	b.sendTo(sender[:], marshalInfo(info{
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

func (b *Broker) handlePing(frame frame) {
	ping, err := unmarshalPing(frame)
	if err != nil {
		b.onError(errorf("unmarshal ping: %v", err))
		return
	}

	sender := ping.sender.array()
	b.sendTo(sender[:], marshalInfo(info{
		id:        ping.id,
		neighbors: b.routing.neighbors(nil),
	}, frame))
}

func (b *Broker) handleFwd(frame frame) {
	fwd, err := unmarshalFwd(frame)
	if err != nil {
		b.onError(errorf("unmarshal fwd: %v", err))
		return
	}

	b.sendTo(fwd.ack, marshalAck(ack{
		id: fwd.id,
	}, nil))

	b.forward(req{msg: fwd.msg}, fwd.pkey, b.dispatchMsg)
}

func (b *Broker) handleAck(frame frame) {
	ack, err := unmarshalAck(frame)
	if err != nil {
		b.onError(errorf("unmarshal ack: %v", err))
		return
	}

	b.notifyAck(ack.id, nil)
}

func (b *Broker) handleReq(frame frame) {
	req, err := unmarshalReq(frame)
	if err != nil {
		b.onError(errorf("unmarshal req: %v", err))
		return
	}

	var (
		buf  Key
		pkey key
	)
	if len(req.msg.PartitionKey) != 0 {
		buf = BytesKey(req.msg.PartitionKey)
		pkey = buf[:]
	}

	b.forward(req, pkey, b.dispatchReq)
}

func (b *Broker) handleResp(frame frame) {
	resp, err := unmarshalResp(frame)
	if err != nil {
		b.onError(errorf("unmarshal resp: %v", err))
		return
	}

	b.notifyResp(resp.id, resp.msg)
}

func (b *Broker) dispatchMsg(req req) {
	if g, has := b.handlerGroups.Load(req.msg.Stream); has {
		g.(*handlerGroup).dispatch(req.msg)
	}
}

func (b *Broker) dispatchReq(req req) {
	if h, has := b.requestHandlers.Load(req.msg.Stream); has {
		b.sendTo(req.reply, marshalResp(resp{
			id:  req.id,
			msg: h.(RequestHandler)(req.msg),
		}, nil))
	}
}

func (b *Broker) forward(req req, pkey key, dispatch func(req)) {
	if len(pkey) == 0 {
		dispatch(req)
		return
	}

	var (
		id       uint64
		errch    chan error
		fwdframe frame
		keybuf   Key
	)
	for {
		succ := b.routing.successor(pkey[:], keybuf[:])
		if len(succ) == 0 {
			dispatch(req)
			return
		}

		if len(fwdframe) == 0 {
			id = b.nextID()
			// We need a buffered channel here, because of the error
			// handling below. If sending the message to the successor
			// fails, notifyAck must not block while writing the error
			// to the channel.
			errch = make(chan error, 1)
			fwdframe = marshalFwd(fwd{
				id:   id,
				ack:  b.routing.local,
				pkey: pkey[:],
				msg:  req.msg,
			}, nil)
		}

		b.awaitAck(succ, id, errch)
		if err := b.pubsub.sendToNode(succ, fwdframe); err != nil {
			b.notifyAck(id, err)
		}

		if err := <-errch; err == nil {
			return
		} else if err != ErrTimeout {
			b.onError(err)
			return
		}

		// The node was suspected and removed from the
		// valid keys. We look for the next successor
		// to handle the message.
	}
}

func (b *Broker) awaitAck(receiver key, id uint64, errch chan error) {
	b.pendingAckMtx.Lock()
	b.pendingAcks[id] = pendingAck{
		receiver: receiver,
		errch:    errch,
		timer: time.AfterFunc(b.ackTimeout, func() {
			b.notifyAck(id, ErrTimeout)
		}),
	}
	b.pendingAckMtx.Unlock()
}

func (b *Broker) notifyAck(id uint64, err error) {
	b.pendingAckMtx.Lock()
	ack, has := b.pendingAcks[id]
	delete(b.pendingAcks, id)
	b.pendingAckMtx.Unlock()
	if !has {
		return
	}
	ack.timer.Stop()
	if err == ErrTimeout {
		b.routing.suspect(ack.receiver)
	}
	ack.errch <- err
}

func (b *Broker) awaitResp(id uint64, msgch chan Message) {
	b.pendingRespMtx.Lock()
	b.pendingResps[id] = msgch
	b.pendingRespMtx.Unlock()
}

func (b *Broker) notifyResp(id uint64, msg Message) {
	b.pendingRespMtx.Lock()
	msgch, has := b.pendingResps[id]
	delete(b.pendingResps, id)
	b.pendingRespMtx.Unlock()
	if has {
		msgch <- msg
	}
}

func (b *Broker) sendTo(target key, frame frame) {
	if err := b.pubsub.sendToNode(target, frame); err != nil {
		b.onError(errorf("send to node: %v", err))
	}
}

func (b *Broker) nextID() uint64 {
	return atomic.AddUint64(&b.id, 1)
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
		errch  chan error
	)
	for {
		select {
		case <-b.closing:
			return
		case <-ticker.C:
		}

		stabs = b.routing.stabilizers(stabs)
		nstabs = stabs.length()
		if cap(errch) < nstabs {
			errch = make(chan error, nstabs)
		}

		for i := 0; i < nstabs; i++ {
			key := stabs.at(i)
			ping.id = b.nextID()
			frame = marshalPing(ping, frame)

			b.awaitAck(key, ping.id, errch)
			if err := b.pubsub.sendToNode(key, frame); err != nil {
				b.notifyAck(ping.id, err)
			}
		}

		// consume errors
		for i := 0; i < nstabs; i++ {
			if err := <-errch; err != nil && err != ErrClosed && err != ErrTimeout {
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

func (g *handlerGroup) dispatch(msg Message) {
	g.mtx.RLock()
	handlers := g.handlers
	g.mtx.RUnlock()

	for _, h := range handlers {
		h(msg)
	}
}
