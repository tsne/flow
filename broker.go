package flow

import (
	"sync"
	"sync/atomic"
	"time"
)

// Message holds the data of a streaming message.
type Message struct {
	Stream       string    // published to the pub/sub system
	Source       []byte    // the source the message comes from
	Time         time.Time // the time the message was created
	PartitionKey []byte    // the key for partitioning
	Data         []byte    // the data which should be sent
}

// Handler represents a callback function for handling incoming messages.
type Handler func(msg Message)

type pendingResp struct {
	receiver key
	timer    *time.Timer
	errch    chan error
}

// Broker represents a single node within a group. It enables
// the publishing and subscribing capabilities of the pub/sub
// system. Each subscribed message is handled by the responsible
// broker, which is determined by the respective node key.
type Broker struct {
	ackTimeout time.Duration

	routing routingTable
	pubsub  pubsub
	storage storage
	respID  uint64

	wg      sync.WaitGroup
	closing chan struct{}

	pendingMtx   sync.Mutex
	pendingResps map[uint64]pendingResp // id => pending response

	handlersMtx sync.RWMutex
	handlers    map[string][]Handler // stream => handlers
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
		ackTimeout:   opts.ackTimeout,
		routing:      newRoutingTable(opts),
		pubsub:       newPubSub(pubsub, opts),
		storage:      newStorage(opts),
		closing:      make(chan struct{}),
		pendingResps: make(map[uint64]pendingResp),
		handlers:     make(map[string][]Handler),
	}

	err := b.pubsub.subscribeGroup(b.processGroupSubs)
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
		logf("group broadcast error: %v", err)
	}

	// cancel pending responses
	b.pendingMtx.Lock()
	rids := make([]uint64, 0, len(b.pendingResps))
	for rid := range b.pendingResps {
		rids = append(rids, rid)
	}
	b.pendingMtx.Unlock()
	for _, rid := range rids {
		b.notifyResp(rid, errClosing)
	}

	// shutdown pub/sub
	b.pubsub.shutdown()

	b.wg.Wait()
	return nil
}

// Publish persists the message and publishes it to the pub/sub system.
// If the message has no partition key, the message will be processed
// by a random broker within the group.
func (b *Broker) Publish(msg Message) error {
	if msg.Stream == "" {
		return errorString("missing message stream")
	}

	if err := b.storage.persist(msg); err != nil {
		return err
	}

	return b.pubsub.publish(msg.Stream, marshalPub(pub{
		source:       msg.Source,
		time:         msg.Time,
		partitionKey: msg.PartitionKey,
		payload:      msg.Data,
	}, nil))
}

// Subscribe subscribes to the messages of the specified stream.
// These messsages are partitioned within the group the broker is
// assigned to.
func (b *Broker) Subscribe(stream string, handler Handler) error {
	b.handlersMtx.Lock()
	handlers, has := b.handlers[stream]
	b.handlers[stream] = append(handlers, handler)
	b.handlersMtx.Unlock()
	if has {
		return nil
	}
	return b.pubsub.subscribe(stream, b.processSub)
}

func (b *Broker) processGroupSubs(stream string, data []byte) {
	frame, err := frameFromBytes(data)
	if err != nil {
		logf("group subscription error: %v", err)
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
	default:
		logf("unexpected frame type: %s", frame.typ())
	}
}

func (b *Broker) processSub(stream string, data []byte) {
	frm, err := frameFromBytes(data)
	if err != nil {
		logf("subscription error: %v", err)
		return
	}

	pub, err := unmarshalPub(frm)
	if err != nil {
		logf("pub unmarshal error: %v", err)
		return
	}

	if len(pub.partitionKey) == 0 {
		b.dispatch(Message{
			Stream:       stream,
			Source:       pub.source,
			Time:         pub.time,
			PartitionKey: pub.partitionKey,
			Data:         pub.payload,
		})
		return
	}

	pkey := BytesKey(pub.partitionKey)

	var (
		rid      uint64
		errch    chan error
		fwdframe frame
		succ     key
	)
	for {
		succ = b.routing.successor(pkey[:], succ)
		if len(succ) == 0 {
			b.dispatch(Message{
				Stream:       stream,
				Source:       pub.source,
				Time:         pub.time,
				PartitionKey: pub.partitionKey,
				Data:         pub.payload,
			})
			return
		}

		if len(fwdframe) == 0 {
			rid = b.nextRespID()
			// We need a buffered channel here, because of the error
			// handling below. If sending the message to the successor
			// fails, notifyResp must not block while writing the error
			// to the channel.
			errch = make(chan error, 1)
			fwdframe = marshalFwd(fwd{
				id:     rid,
				origin: b.routing.local,
				key:    pkey[:],
				stream: stream,
				pub:    pub,
			}, nil)
		}

		b.awaitResp(succ, rid, errch)
		if err := b.pubsub.sendToNode(succ, fwdframe); err != nil {
			b.notifyResp(rid, err)
		}
		if err := <-errch; err != nil {
			if err == errRespTimeout {
				// The node was suspected and removed from the
				// valid keys. We look for the next successor
				// to handle the message.
				continue
			}
			logf("subscription error: %v", err)
		}
		return
	}
}

func (b *Broker) handleJoin(frame frame) {
	join, err := unmarshalJoin(frame)
	if err != nil {
		logf("join unmarshal error: %v", err)
		return
	}
	b.routing.register(keys(join.sender))
	if !join.sender.equal(b.routing.local) {
		sender := join.sender.array()
		b.sendTo(sender[:], marshalInfo(info{
			neighbors: b.routing.neighbors(nil),
		}, frame))
	}
}

func (b *Broker) handleLeave(frame frame) {
	leave, err := unmarshalLeave(frame)
	if err != nil {
		logf("leave unmarshal error: %v", err)
		return
	}
	b.routing.unregister(leave.node)
}

func (b *Broker) handleInfo(frame frame) {
	info, err := unmarshalInfo(frame)
	if err != nil {
		logf("info unmarshal error: %v", err)
		return
	}

	b.notifyResp(info.id, nil)
	b.routing.register(info.neighbors)
}

func (b *Broker) handlePing(frame frame) {
	ping, err := unmarshalPing(frame)
	if err != nil {
		logf("ping unmarshal error: %v", err)
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
		logf("fwd unmarshal error: %v", err)
		return
	}

	if succ := b.routing.successor(fwd.partitionKey, nil); len(succ) != 0 {
		b.sendTo(succ, frame)
		return
	}

	b.dispatch(Message{
		Stream:       fwd.stream,
		Source:       fwd.source,
		Time:         fwd.time,
		PartitionKey: fwd.partitionKey,
		Data:         fwd.payload,
	})

	origin := fwd.origin.array()
	b.sendTo(origin[:], marshalAck(ack{
		id: fwd.id,
	}, frame))
}

func (b *Broker) handleAck(frame frame) {
	ack, err := unmarshalAck(frame)
	if err != nil {
		logf("ack unmarshal error: %v", err)
		return
	}
	b.notifyResp(ack.id, ack.err)
}

func (b *Broker) dispatch(msg Message) {
	b.handlersMtx.RLock()
	handlers := b.handlers[msg.Stream]
	b.handlersMtx.RUnlock()
	for _, h := range handlers {
		h(msg)
	}
}

func (b *Broker) awaitResp(receiver key, rid uint64, errch chan error) {
	b.pendingMtx.Lock()
	b.pendingResps[rid] = pendingResp{
		receiver: receiver,
		errch:    errch,
		timer: time.AfterFunc(b.ackTimeout, func() {
			b.notifyResp(rid, errRespTimeout)
		}),
	}
	b.pendingMtx.Unlock()
}

func (b *Broker) notifyResp(rid uint64, err error) {
	b.pendingMtx.Lock()
	resp, has := b.pendingResps[rid]
	delete(b.pendingResps, rid)
	b.pendingMtx.Unlock()
	if !has {
		return
	}
	resp.timer.Stop()
	if err == errRespTimeout {
		b.routing.suspect(resp.receiver)
	}
	resp.errch <- err
}

func (b *Broker) sendTo(target key, frame frame) {
	if err := b.pubsub.sendToNode(target, frame); err != nil {
		logf("node send error: %v", err)
	}
}

func (b *Broker) nextRespID() uint64 {
	return atomic.AddUint64(&b.respID, 1)
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

		for i, n := 0, stabs.length(); i < n; i++ {
			key := stabs.at(i)
			ping.id = b.nextRespID()
			frame = marshalPing(ping, frame)

			b.awaitResp(key, ping.id, errch)
			if err := b.pubsub.sendToNode(key, frame); err != nil {
				b.notifyResp(ping.id, err)
			}
		}

		// consume errors
		for i := 0; i < nstabs; i++ {
			if err := <-errch; err != nil && err != errClosing {
				logf("stabilization error: %v", err)
			}
		}
	}
}
