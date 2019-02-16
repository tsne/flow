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

type reply struct {
	data []byte
	err  error
}

type pendingReply struct {
	receiver Key
	replych  chan reply
	timer    *time.Timer
}

// Broker represents a single node within a clique. It enables
// the publishing and subscribing capabilities of the pub/sub
// system. Each subscribed message is handled by the responsible
// broker, which is determined by the respective node key within a
// clique.
type Broker struct {
	messagesInFlight uint64
	requestsInFlight uint64
	shuttingDown     uint64

	clique     string
	ackTimeout time.Duration
	reqTimeout time.Duration
	codec      Codec
	onError    func(error)

	routing routingTable
	pubsub  pubsub
	id      uint64

	wg      sync.WaitGroup
	leaving chan struct{}

	pendingRepliesMtx sync.Mutex
	pendingReplies    map[uint64]pendingReply // id => pending reply

	messageHandlers map[string]MessageHandler // stream => message handler
	requestHandlers map[string]RequestHandler // stream => request handler
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
		clique:          opts.clique,
		ackTimeout:      opts.ackTimeout,
		reqTimeout:      opts.reqTimeout,
		codec:           opts.codec,
		onError:         opts.errorHandler,
		routing:         newRoutingTable(opts),
		pubsub:          newPubSub(pubsub, opts),
		leaving:         make(chan struct{}),
		pendingReplies:  make(map[uint64]pendingReply),
		messageHandlers: opts.messageHandlers,
		requestHandlers: opts.requestHandlers,
	}

	if err := b.pubsub.subscribe(ctx, nodeStream(b.clique, b.routing.local), "", b.processCliqueProtocol); err != nil {
		b.pubsub.shutdown(ctx)
		return nil, err
	}

	if err := b.pubsub.subscribe(ctx, b.clique, "", b.processCliqueProtocol); err != nil {
		b.pubsub.shutdown(ctx)
		return nil, err
	}

	for stream := range b.messageHandlers {
		if err := b.pubsub.subscribe(ctx, stream, b.clique, b.processMessage); err != nil {
			b.pubsub.shutdown(ctx)
			return nil, err
		}
	}

	for stream := range b.requestHandlers {
		if err := b.pubsub.subscribe(ctx, stream, b.clique, b.processRequest); err != nil {
			b.pubsub.shutdown(ctx)
			return nil, err
		}
	}

	join := marshalJoin(join{sender: b.routing.local})
	if err := b.broadcast(ctx, join); err != nil {
		b.pubsub.shutdown(ctx)
		return nil, err
	}

	b.wg.Add(1)
	go b.stabilize(opts.stabilization.Interval)
	return b, nil
}

// Close notifies all clique members about a leaving broker and
// disconnects from the pub/sub system.
func (b *Broker) Close() error {
	return b.shutdown(context.Background(), func() error { return nil })
}

// Shutdown gracefully shuts down the broker. It notifies all clique
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

// Publish forwards the message directly to the pub/sub system.
// If the message does not contain any partition key, the message will
// be processed by a random broker within a clique.
// All binary data of the passed message needs to be valid only during
// the method call.
func (b *Broker) Publish(ctx context.Context, msg Message) error {
	if b.isShuttingDown() {
		return ErrClosed
	}
	if err := msg.validate(); err != nil {
		return err
	}
	return b.pubsub.send(ctx, msg.Stream, b.codec.EncodeMessage(msg))
}

// Request sends a request message and waits for its response.
// If the message has no partition key, the request will be processed
// by a random broker within a clique.
// All binary data of the passed message needs to be valid only during
// the method call.
func (b *Broker) Request(ctx context.Context, request Message) (Message, error) {
	if b.isShuttingDown() {
		return Message{}, ErrClosed
	}
	if err := request.validate(); err != nil {
		return Message{}, err
	}

	reply := b.awaitReply(ctx, b.routing.local, b.reqTimeout, func(ctx context.Context, id uint64) error {
		return b.pubsub.send(ctx, request.Stream, marshalMsg(msg{
			id:     id,
			reply:  []byte(nodeStream(b.clique, b.routing.local)),
			stream: []byte(request.Stream),
			pkey:   request.PartitionKey,
			data:   request.Data,
		}))
	})
	return Message{Data: reply.data}, reply.err
}

func (b *Broker) processMessage(ctx context.Context, stream string, data []byte) {
	atomic.AddUint64(&b.messagesInFlight, 1)
	defer atomic.AddUint64(&b.messagesInFlight, ^uint64(0))

	decoded, err := b.codec.DecodeMessage(stream, data)
	if err != nil {
		b.onError(errorf("decode message: %v", err))
		return
	}

	b.forwardMsg(ctx, msg{
		stream: []byte(decoded.Stream),
		pkey:   decoded.PartitionKey,
		data:   decoded.Data,
	})
}

func (b *Broker) processRequest(ctx context.Context, stream string, data []byte) {
	atomic.AddUint64(&b.requestsInFlight, 1)
	defer atomic.AddUint64(&b.requestsInFlight, ^uint64(0))

	f, err := unmarshalFrame(data)
	switch {
	case err != nil:
		b.onError(errorf("request subscription: %v", err))
		return
	case f.typ() != frameTypeMsg:
		b.onError(errorf("unexpected request frame type: %s", f.typ()))
		return
	}

	msg, err := unmarshalMsg(f)
	if err != nil {
		b.onError(errorf("unmarshal msg: %v", err))
		return
	}

	b.forwardMsg(ctx, msg)
}

func (b *Broker) processCliqueProtocol(ctx context.Context, stream string, data []byte) {
	f, err := unmarshalFrame(data)
	if err != nil {
		b.onError(errorf("clique subscription: %v", err))
		return
	}

	switch f.typ() {
	case frameTypeJoin:
		b.handleJoin(ctx, f)
	case frameTypeLeave:
		b.handleLeave(f)
	case frameTypeInfo:
		b.handleInfo(f)
	case frameTypePing:
		b.handlePing(ctx, f)
	case frameTypeFwd:
		b.handleFwd(ctx, f)
	case frameTypeAck:
		b.handleAck(f)
	default:
		b.onError(errorf("unexpected clique frame type: %s", f.typ()))
	}
}

func (b *Broker) handleJoin(ctx context.Context, f frame) {
	join, err := unmarshalJoin(f)
	switch {
	case err != nil:
		b.onError(errorf("unmarshal join: %v", err))
		return
	case join.sender == b.routing.local:
		return
	}

	neighbors := b.routing.neighbors()
	b.routing.registerKey(join.sender)

	err = b.sendTo(ctx, join.sender, marshalInfo(info{neighbors: neighbors}))
	if err != nil {
		b.onError(errorf("send info: %v", err))
	}
}

func (b *Broker) handleLeave(f frame) {
	leave, err := unmarshalLeave(f)
	if err != nil {
		b.onError(errorf("unmarshal leave: %v", err))
		return
	}

	b.routing.unregister(leave.node)
}

func (b *Broker) handleInfo(f frame) {
	info, err := unmarshalInfo(f)
	if err != nil {
		b.onError(errorf("unmarshal info: %v", err))
		return
	}

	b.routing.registerKeys(info.neighbors)
	b.notifyReply(info.id, reply{})
}

func (b *Broker) handlePing(ctx context.Context, f frame) {
	ping, err := unmarshalPing(f)
	if err != nil {
		b.onError(errorf("unmarshal ping: %v", err))
		return
	}

	err = b.sendTo(ctx, ping.sender, marshalInfo(info{
		id:        ping.id,
		neighbors: b.routing.neighbors(),
	}))
	if err != nil {
		b.onError(errorf("send info: %v", err))
	}
}

func (b *Broker) handleFwd(ctx context.Context, f frame) {
	fwd, err := unmarshalFwd(f)
	if err != nil {
		b.onError(errorf("unmarshal fwd: %v", err))
		return
	}

	err = b.sendTo(ctx, fwd.ack, marshalAck(ack{id: fwd.id}))
	if err != nil {
		b.onError(errorf("send ack: %v", err))
	}

	b.forwardMsg(ctx, fwd.msg)
}

func (b *Broker) handleAck(frame frame) {
	ack, err := unmarshalAck(frame)
	if err != nil {
		b.onError(errorf("unmarshal ack: %v", err))
		return
	}

	b.notifyReply(ack.id, reply{data: ack.data})
}

func (b *Broker) forwardMsg(ctx context.Context, msg msg) {
	if len(msg.pkey) == 0 {
		b.dispatchMsg(ctx, msg)
		return
	}

	partition := KeyFromBytes(msg.pkey)
	for {
		succ := b.routing.successor(partition)
		if succ == b.routing.local {
			b.dispatchMsg(ctx, msg)
			return
		}

		reply := b.awaitReply(ctx, succ, b.ackTimeout, func(ctx context.Context, id uint64) error {
			return b.sendTo(ctx, succ, marshalFwd(fwd{
				id:  id,
				ack: b.routing.local,
				msg: msg,
			}))
		})
		if reply.err == nil {
			return
		} else if reply.err != ErrTimeout {
			b.onError(reply.err)
			return
		}

		// The node was suspected and removed from the
		// valid keys. We look for the next successor
		// to handle the message.
	}
}

func (b *Broker) dispatchMsg(ctx context.Context, msg msg) {
	var reply reply
	if h := b.messageHandlers[string(msg.stream)]; h != nil {
		h(ctx, Message{
			Stream:       string(msg.stream),
			PartitionKey: msg.pkey,
			Data:         msg.data,
		})
	} else if h := b.requestHandlers[string(msg.stream)]; h != nil {
		resp := h(ctx, Message{
			Stream:       string(msg.stream),
			PartitionKey: msg.pkey,
			Data:         msg.data,
		})
		reply.data = resp.Data
	} else {
		return
	}

	if len(msg.reply) != 0 {
		ack := ack{id: msg.id, data: reply.data}
		err := b.pubsub.send(ctx, string(msg.reply), marshalAck(ack))
		if err != nil {
			b.onError(errorf("send ack: %v", err))
		}
	}
}

func (b *Broker) awaitReply(ctx context.Context, receiver Key, timeout time.Duration, send func(context.Context, uint64) error) reply {
	id := atomic.AddUint64(&b.id, 1)
	replych := make(chan reply, 1)

	b.pendingRepliesMtx.Lock()
	b.pendingReplies[id] = pendingReply{
		receiver: receiver,
		replych:  replych,
		timer: time.AfterFunc(timeout, func() {
			b.notifyReply(id, reply{err: ErrTimeout})
		}),
	}
	b.pendingRepliesMtx.Unlock()

	if err := send(ctx, id); err != nil {
		b.notifyReply(id, reply{err: err})
	}

	select {
	case reply := <-replych:
		return reply
	case <-ctx.Done():
		reply := reply{err: ctx.Err()}
		b.notifyReply(id, reply)
		return reply
	}
}

func (b *Broker) notifyReply(id uint64, reply reply) {
	b.pendingRepliesMtx.Lock()
	pending, has := b.pendingReplies[id]
	delete(b.pendingReplies, id)
	b.pendingRepliesMtx.Unlock()

	if has {
		pending.timer.Stop()
		if reply.err == ErrTimeout && pending.receiver != b.routing.local {
			b.routing.suspect(pending.receiver)
		}
		pending.replych <- reply
	}
}

func (b *Broker) sendTo(ctx context.Context, target Key, f frame) error {
	return b.pubsub.send(ctx, nodeStream(b.clique, target), f)
}

func (b *Broker) broadcast(ctx context.Context, f frame) error {
	return b.pubsub.send(ctx, b.clique, f)
}

func (b *Broker) isShuttingDown() bool {
	return atomic.LoadUint64(&b.shuttingDown) != 0
}

func (b *Broker) shutdown(ctx context.Context, wait func() error) error {
	atomic.StoreUint64(&b.shuttingDown, 1)
	close(b.leaving)

	leave := marshalLeave(leave{node: b.routing.local})
	err := b.broadcast(ctx, leave)

	if waitErr := wait(); waitErr != nil {
		err = waitErr
	}

	b.pubsub.shutdown(ctx)

	// cancel pending replies
	b.pendingRepliesMtx.Lock()
	ids := make([]uint64, 0, len(b.pendingReplies))
	for id := range b.pendingReplies {
		ids = append(ids, id)
	}
	b.pendingRepliesMtx.Unlock()
	for _, id := range ids {
		b.notifyReply(id, reply{err: ErrClosed})
	}

	b.wg.Wait()
	return err
}

func (b *Broker) stabilize(interval time.Duration) {
	defer b.wg.Done()

	ping := ping{sender: b.routing.local}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var frame frame

	stabs := make([]Key, 1+b.routing.stabilizerCount) // successor + stabilizers
	for {
		select {
		case <-b.leaving:
			return
		case <-ticker.C:
		}

		nstabs := b.routing.stabilizers(stabs)
		for i := 0; i < nstabs; i++ {
			stab := stabs[i]
			reply := b.awaitReply(context.Background(), stab, b.ackTimeout, func(ctx context.Context, id uint64) error {
				ping.id = id
				frame = marshalPing(ping, frame)
				return b.sendTo(ctx, stab, frame)
			})

			if reply.err != nil && reply.err != ErrClosed && reply.err != ErrTimeout {
				b.onError(errorf("stabilization: %v", reply.err))
			}
		}
	}
}

func nodeStream(clique string, node Key) string {
	buf := alloc(len(clique)+1+2*keySize, nil)
	n := copy(buf, clique)
	buf[n] = '.'
	node.writeString(buf[n+1:])
	return string(buf)
}
