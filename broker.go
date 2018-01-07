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
type Handler func(msg *Message)

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

	wg     sync.WaitGroup
	closed chan struct{}

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
		closed:       make(chan struct{}),
		pendingResps: make(map[uint64]pendingResp),
		handlers:     make(map[string][]Handler),
	}

	err := b.pubsub.subscribeGroup(b.processGroupSubs)
	if err != nil {
		return nil, err
	}

	join := joinMsg{sender: b.routing.local}
	if err := b.pubsub.sendToGroup(join.marshal(nil)); err != nil {
		return nil, err
	}

	b.wg.Add(1)
	go b.stabilize(opts.stabilizationInterval)
	return b, nil
}

// Close notifies all group members about a leaving broker and
// diconnects from the pub/sub system.
func (b *Broker) Close() error {
	close(b.closed)

	leave := leaveMsg{node: b.routing.local}
	b.pubsub.sendToGroup(leave.marshal(nil))
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

	pub := pubMsg{
		source:       msg.Source,
		time:         msg.Time,
		partitionKey: msg.PartitionKey,
		payload:      msg.Data,
	}
	return b.pubsub.publish(msg.Stream, pub.marshal(nil))
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
	msg, err := messageFromBytes(data)
	if err != nil {
		logf("group subscription error: %v", err)
		return
	}

	switch msg.typ() {
	case msgTypeJoin:
		b.handleJoin(msg)
	case msgTypeLeave:
		b.handleLeave(msg)
	case msgTypeInfo:
		b.handleInfo(msg)
	case msgTypePing:
		b.handlePing(msg)
	case msgTypeFwd:
		b.handleFwd(msg)
	case msgTypeAck:
		b.handleAck(msg)
	default:
		logf("unexpected message type: %s", msg.typ())
	}
}

func (b *Broker) processSub(stream string, data []byte) {
	msg, err := messageFromBytes(data)
	if err != nil {
		logf("subscription error: %v", err)
		return
	}

	var pub pubMsg
	if err := pub.unmarshal(msg); err != nil {
		logf("published message error: %v", err)
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

	key := BytesKey(pub.partitionKey)

	var (
		rid    uint64
		fwdmsg message
		errch  chan error
	)
	for {
		succ := b.routing.successor(key[:])
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

		if len(fwdmsg) == 0 {
			rid = b.nextRespID()
			fwd := fwdMsg{
				id:     rid,
				origin: b.routing.local,
				key:    key[:],
				stream: stream,
				pubMsg: pub,
			}
			fwdmsg = fwd.marshal(nil)
			errch = make(chan error)
		}

		b.awaitResp(succ, rid, errch)
		b.pubsub.sendToNode(succ, fwdmsg)
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

func (b *Broker) handleJoin(msg message) {
	var join joinMsg
	if err := join.unmarshal(msg); err != nil {
		logf("join unmarshal error: %v", err)
		return
	}
	b.routing.register(keys(join.sender))
	if !join.sender.equal(b.routing.local) {
		sender := join.sender.array()
		info := infoMsg{
			neighbors: b.routing.neighbors(),
		}
		b.pubsub.sendToNode(sender[:], info.marshal(msg))
	}
}

func (b *Broker) handleLeave(msg message) {
	var leave leaveMsg
	if err := leave.unmarshal(msg); err != nil {
		logf("leave unmarshal error: %v", err)
		return
	}
	b.routing.unregister(leave.node)
}

func (b *Broker) handleInfo(msg message) {
	var info infoMsg
	if err := info.unmarshal(msg); err != nil {
		logf("info unmarshal error: %v", err)
		return
	}

	b.notifyResp(info.id, nil)
	b.routing.register(info.neighbors)
}

func (b *Broker) handlePing(msg message) {
	var ping pingMsg
	if err := ping.unmarshal(msg); err != nil {
		logf("ping unmarshal error: %v", err)
		return
	}

	sender := ping.sender.array()
	info := infoMsg{
		id:        ping.id,
		neighbors: b.routing.neighbors(),
	}
	b.pubsub.sendToNode(sender[:], info.marshal(msg))
}

func (b *Broker) handleFwd(msg message) {
	var fwd fwdMsg
	if err := fwd.unmarshal(msg); err != nil {
		logf("fwd unmarshal error: %v", err)
		return
	}

	if succ := b.routing.successor(fwd.partitionKey); len(succ) != 0 {
		b.pubsub.sendToNode(succ, msg)
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
	ack := ackMsg{id: fwd.id}
	b.pubsub.sendToNode(origin[:], ack.marshal(msg))
}

func (b *Broker) handleAck(msg message) {
	var ack ackMsg
	if err := ack.unmarshal(msg); err != nil {
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
		h(&msg)
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
	if resp.errch != nil {
		resp.errch <- err
	}
}

func (b *Broker) nextRespID() uint64 {
	return atomic.AddUint64(&b.respID, 1)
}

func (b *Broker) stabilize(interval time.Duration) {
	defer b.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var msg message
	for {
		select {
		case <-b.closed:
			return
		case <-ticker.C:
		}

		stabs := b.routing.stabilizers()
		ping := pingMsg{sender: b.routing.local}
		for i, n := 0, stabs.length(); i < n; i++ {
			key := stabs.at(i)
			ping.id = b.nextRespID()
			msg = ping.marshal(msg)
			if b.pubsub.sendToNode(key, msg) == nil {
				b.awaitResp(key, ping.id, nil)
			}
		}
	}
}
