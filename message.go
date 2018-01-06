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
	if n := headerLen + payloadSize; len(*m) < n {
		*m = make(message, n)
	}
	binary.BigEndian.PutUint32((*m)[0:4], protocolVersion)
	binary.BigEndian.PutUint32((*m)[4:8], uint32(typ))
	binary.BigEndian.PutUint32((*m)[8:12], uint32(payloadSize))
}

// join
type joinMsg struct {
	sender key
}

func (m *joinMsg) marshal(msg message) message {
	msg.reset(msgTypeJoin, len(m.sender))
	copy(msg.payload(), m.sender)
	return msg
}

func (m *joinMsg) unmarshal(msg message) (err error) {
	if m.sender, err = keyFromBytes(msg.payload()); err != nil {
		return errMalformedJoinMsg
	}
	return nil
}

// leave
type leaveMsg struct {
	node key
}

func (m *leaveMsg) marshal(msg message) message {
	msg.reset(msgTypeLeave, len(m.node))
	copy(msg.payload(), m.node)
	return msg
}

func (m *leaveMsg) unmarshal(msg message) (err error) {
	if m.node, err = keyFromBytes(msg.payload()); err != nil {
		return errMalformedLeaveMsg
	}
	return nil
}

// info
type infoMsg struct {
	id        uint64
	neighbors keys
}

func (m *infoMsg) marshal(msg message) message {
	msg.reset(msgTypeInfo, 8+len(m.neighbors))
	p := msg.payload()
	binary.BigEndian.PutUint64(p, m.id)
	copy(p[8:], m.neighbors)
	return msg
}

func (m *infoMsg) unmarshal(msg message) error {
	p := msg.payload()
	if len(p) < 8 {
		return errMalformedInfoMsg
	}

	var err error
	m.id = binary.BigEndian.Uint64(p)
	if m.neighbors, err = keysFromBytes(p[8:]); err != nil {
		return errMalformedInfoMsg
	}
	return nil
}

// ping
type pingMsg struct {
	id     uint64 // used for replies
	sender key
}

func (m *pingMsg) marshal(msg message) message {
	msg.reset(msgTypePing, 8+len(m.sender))
	p := msg.payload()
	binary.BigEndian.PutUint64(p, m.id)
	copy(p[8:], m.sender)
	return msg
}

func (m *pingMsg) unmarshal(msg message) error {
	p := msg.payload()
	if len(p) < 8+KeySize {
		return errMalformedPingMsg
	}

	var err error
	m.id = binary.BigEndian.Uint64(p)
	if m.sender, err = keyFromBytes(p[8:]); err != nil {
		return errMalformedPingMsg
	}
	return nil
}

// ack
type ackMsg struct {
	id  uint64
	err error
}

func (m *ackMsg) marshal(msg message) message {
	var errtext string
	if m.err != nil {
		errtext = m.err.Error()
	}
	msg.reset(msgTypeAck, 8+len(errtext))
	p := msg.payload()
	binary.BigEndian.PutUint64(p, m.id)
	copy(p[8:], errtext)
	return msg
}

func (m *ackMsg) unmarshal(msg message) error {
	p := msg.payload()
	if len(p) < 8 {
		return errMalformedAckMsg
	}

	m.id = binary.BigEndian.Uint64(p)
	if perr := p[8:]; len(perr) == 0 {
		m.err = nil
	} else {
		m.err = ackError(perr)
	}
	return nil
}

// pub
type pubMsg struct {
	source       []byte
	time         time.Time
	partitionKey []byte
	payload      []byte
}

func (m *pubMsg) marshalSize() int {
	return 20 + len(m.source) + len(m.partitionKey) + len(m.payload)
}

func (m *pubMsg) marshalInto(p []byte) {
	binary.BigEndian.PutUint32(p, uint32(len(m.source)))
	copy(p[4:], m.source)
	binary.BigEndian.PutUint64(p[4+len(m.source):], uint64(m.time.UTC().Unix()))
	binary.BigEndian.PutUint32(p[12+len(m.source):], uint32(m.time.Nanosecond()))
	binary.BigEndian.PutUint32(p[16+len(m.source):], uint32(len(m.partitionKey)))
	copy(p[20+len(m.source):], m.partitionKey)
	copy(p[20+len(m.source)+len(m.partitionKey):], m.payload)
}

func (m *pubMsg) unmarshalFrom(p []byte) error {
	if len(p) < 20 {
		return errMalformedPubMsg
	}

	sourceLen := binary.BigEndian.Uint32(p)
	pkeyLen := binary.BigEndian.Uint32(p[16+sourceLen:])
	secs := int64(binary.BigEndian.Uint64(p[4+sourceLen:]))
	nsecs := int64(binary.BigEndian.Uint32(p[12+sourceLen:]))

	m.source = p[4 : 4+sourceLen]
	m.time = time.Unix(secs, nsecs).UTC()
	m.partitionKey = p[20+sourceLen : 20+sourceLen+pkeyLen]
	m.payload = p[20+sourceLen+pkeyLen:]
	return nil
}

func (m *pubMsg) marshal(msg message) message {
	msg.reset(msgTypePub, m.marshalSize())
	m.marshalInto(msg.payload())
	return msg
}

func (m *pubMsg) unmarshal(msg message) error {
	return m.unmarshalFrom(msg.payload())
}

// fwd
type fwdMsg struct {
	id     uint64
	origin key
	key    key
	stream string
	pubMsg
}

func (m *fwdMsg) marshal(msg message) message {
	msg.reset(msgTypeFwd, 8+2*KeySize+4+len(m.stream)+m.pubMsg.marshalSize())
	p := msg.payload()
	binary.BigEndian.PutUint64(p, m.id)
	copy(p[8:], m.origin)
	copy(p[8+KeySize:], m.key)
	binary.BigEndian.PutUint32(p[8+2*KeySize:], uint32(len(m.stream)))
	copy(p[12+2*KeySize:], m.stream)
	m.pubMsg.marshalInto(p[12+2*KeySize+len(m.stream):])
	return msg
}

func (m *fwdMsg) unmarshal(msg message) error {
	p := msg.payload()
	if len(p) < 12+2*KeySize {
		return errMalformedFwdMsg
	}

	streamLen := binary.BigEndian.Uint32(p[8+2*KeySize:])

	m.id = binary.BigEndian.Uint64(p)
	m.origin = p[8 : 8+KeySize]
	m.key = p[8+KeySize : 8+2*KeySize]
	m.stream = string(p[12+2*KeySize : 12+2*KeySize+streamLen])
	if err := m.pubMsg.unmarshalFrom(p[12+2*KeySize+streamLen:]); err != nil {
		return errMalformedFwdMsg
	}
	return nil
}
