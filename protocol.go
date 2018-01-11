package flow

import (
	"encoding/binary"
	"time"
)

// message format:
//   | protocol version  (uint32) |
//   | message type      (uint32) |
//   | len(payload)      (uint32) |
//   | payload           ([]byte) |
//
// The version field is reserved for later message evolution. Currently
// this field is set, but not used anywhere.

const (
	protocolVersion = 1
	headerLen       = 12
)

type msgType uint

const (
	// broadcast
	msgTypeJoin  = msgType(uint('J')<<24 | uint('O')<<16 | uint('I')<<8 | uint('N'))
	msgTypeLeave = msgType(uint('L')<<24 | uint('E')<<16 | uint('A')<<8 | uint('V'))

	// single node
	msgTypeInfo = msgType(uint('I')<<24 | uint('N')<<16 | uint('F')<<8 | uint('O'))
	msgTypePing = msgType(uint('P')<<24 | uint('I')<<16 | uint('N')<<8 | uint('G'))
	msgTypeFwd  = msgType(uint('F')<<24 | uint('W')<<16 | uint('D')<<8 | uint(' '))
	msgTypeAck  = msgType(uint('A')<<24 | uint('C')<<16 | uint('K')<<8 | uint(' '))

	// pubsub
	msgTypePub = msgType(uint('P')<<24 | uint('U')<<16 | uint('B')<<8 | uint(' '))
)

func (t msgType) String() string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(t))
	res := buf[:]
	if res[3] == ' ' {
		res = buf[:3]
	}
	return string(res)
}

type message []byte

func messageFromBytes(p []byte) (message, error) {
	if len(p) < headerLen {
		return nil, errMalformedMessage
	}
	return message(p), nil
}

func (m message) typ() msgType {
	return msgType(binary.BigEndian.Uint32(m[4:8]))
}

func (m message) payload() []byte {
	plen := binary.BigEndian.Uint32(m[8:12])
	return m[headerLen : headerLen+plen]
}

func (m *message) reset(typ msgType, payloadSize int) {
	*m = alloc(headerLen+payloadSize, *m)
	binary.BigEndian.PutUint32((*m)[0:4], protocolVersion)
	binary.BigEndian.PutUint32((*m)[4:8], uint32(typ))
	binary.BigEndian.PutUint32((*m)[8:12], uint32(payloadSize))
}

// join
type join struct {
	sender key
}

func marshalJoin(join join, buf message) message {
	buf.reset(msgTypeJoin, len(join.sender))
	copy(buf.payload(), join.sender)
	return buf
}

func unmarshalJoin(msg message) (join join, err error) {
	if join.sender, err = keyFromBytes(msg.payload()); err != nil {
		return join, errMalformedJoin
	}
	return join, nil
}

// leave
type leave struct {
	node key
}

func marshalLeave(leave leave, buf message) message {
	buf.reset(msgTypeLeave, len(leave.node))
	copy(buf.payload(), leave.node)
	return buf
}

func unmarshalLeave(msg message) (leave leave, err error) {
	if leave.node, err = keyFromBytes(msg.payload()); err != nil {
		return leave, errMalformedLeave
	}
	return leave, nil
}

// info
type info struct {
	id        uint64
	neighbors keys
}

func marshalInfo(info info, buf message) message {
	buf.reset(msgTypeInfo, 8+len(info.neighbors))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, info.id)
	copy(p[8:], info.neighbors)
	return buf
}

func unmarshalInfo(msg message) (info info, err error) {
	p := msg.payload()
	if len(p) < 8 {
		return info, errMalformedInfo
	}

	info.id = binary.BigEndian.Uint64(p)
	if info.neighbors, err = keysFromBytes(p[8:]); err != nil {
		return info, errMalformedInfo
	}
	return info, nil
}

// ping
type ping struct {
	id     uint64 // used for replies
	sender key
}

func marshalPing(ping ping, buf message) message {
	buf.reset(msgTypePing, 8+len(ping.sender))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, ping.id)
	copy(p[8:], ping.sender)
	return buf
}

func unmarshalPing(msg message) (ping ping, err error) {
	p := msg.payload()
	if len(p) < 8+KeySize {
		return ping, errMalformedPing
	}

	ping.id = binary.BigEndian.Uint64(p)
	if ping.sender, err = keyFromBytes(p[8:]); err != nil {
		return ping, errMalformedPing
	}
	return ping, nil
}

// ack
type ack struct {
	id  uint64
	err error
}

func marshalAck(ack ack, buf message) message {
	var errtext string
	if ack.err != nil {
		errtext = ack.err.Error()
	}
	buf.reset(msgTypeAck, 8+len(errtext))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, ack.id)
	copy(p[8:], errtext)
	return buf
}

func unmarshalAck(msg message) (ack ack, err error) {
	p := msg.payload()
	if len(p) < 8 {
		return ack, errMalformedAck
	}

	ack.id = binary.BigEndian.Uint64(p)
	if perr := p[8:]; len(perr) == 0 {
		ack.err = nil
	} else {
		ack.err = ackError(perr)
	}
	return ack, nil
}

// pub
type pub struct {
	source       []byte
	time         time.Time
	partitionKey []byte
	payload      []byte
}

func pubSize(pub pub) int {
	return 20 + len(pub.source) + len(pub.partitionKey) + len(pub.payload)
}

func marshalPub(pub pub, buf message) message {
	buf.reset(msgTypePub, pubSize(pub))
	marshalPubInto(buf.payload(), pub)
	return buf
}

func marshalPubInto(p []byte, pub pub) {
	binary.BigEndian.PutUint32(p, uint32(len(pub.source)))
	copy(p[4:], pub.source)
	binary.BigEndian.PutUint64(p[4+len(pub.source):], uint64(pub.time.Unix()))
	binary.BigEndian.PutUint32(p[12+len(pub.source):], uint32(pub.time.Nanosecond()))
	binary.BigEndian.PutUint32(p[16+len(pub.source):], uint32(len(pub.partitionKey)))
	copy(p[20+len(pub.source):], pub.partitionKey)
	copy(p[20+len(pub.source)+len(pub.partitionKey):], pub.payload)
}

func unmarshalPub(msg message) (pub, error) {
	return unmarshalPubFrom(msg.payload())
}

func unmarshalPubFrom(p []byte) (pub pub, err error) {
	if len(p) < 20 {
		return pub, errMalformedPub
	}

	sourceLen := binary.BigEndian.Uint32(p)
	pkeyLen := binary.BigEndian.Uint32(p[16+sourceLen:])
	secs := int64(binary.BigEndian.Uint64(p[4+sourceLen:]))
	nsecs := int64(binary.BigEndian.Uint32(p[12+sourceLen:]))

	pub.source = p[4 : 4+sourceLen]
	pub.time = time.Unix(secs, nsecs).UTC()
	pub.partitionKey = p[20+sourceLen : 20+sourceLen+pkeyLen]
	pub.payload = p[20+sourceLen+pkeyLen:]
	return pub, nil
}

// fwd
type fwd struct {
	id     uint64
	origin key
	key    key
	stream string
	pub
}

func marshalFwd(fwd fwd, buf message) message {
	buf.reset(msgTypeFwd, 8+2*KeySize+4+len(fwd.stream)+pubSize(fwd.pub))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, fwd.id)
	copy(p[8:], fwd.origin)
	copy(p[8+KeySize:], fwd.key)
	binary.BigEndian.PutUint32(p[8+2*KeySize:], uint32(len(fwd.stream)))
	copy(p[12+2*KeySize:], fwd.stream)
	marshalPubInto(p[12+2*KeySize+len(fwd.stream):], fwd.pub)
	return buf
}

func unmarshalFwd(msg message) (fwd fwd, err error) {
	p := msg.payload()
	if len(p) < 12+2*KeySize {
		return fwd, errMalformedFwd
	}

	streamLen := binary.BigEndian.Uint32(p[8+2*KeySize:])

	fwd.id = binary.BigEndian.Uint64(p)
	fwd.origin = p[8 : 8+KeySize]
	fwd.key = p[8+KeySize : 8+2*KeySize]
	fwd.stream = string(p[12+2*KeySize : 12+2*KeySize+streamLen])
	if fwd.pub, err = unmarshalPubFrom(p[12+2*KeySize+streamLen:]); err != nil {
		return fwd, errMalformedFwd
	}
	return fwd, nil
}
