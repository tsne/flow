package flow

import (
	"encoding/binary"
	"time"
)

// frame format:
//   | protocol version  (uint32) |
//   | frame type        (uint32) |
//   | len(payload)      (uint32) |
//   | payload           ([]byte) |
//
// The version field is reserved for later frame evolution. Currently
// this field is set, but not used anywhere.

const (
	protocolVersion = 1
	headerLen       = 12
)

type frameType uint

const (
	// broadcast
	frameTypeJoin  = frameType(uint('J')<<24 | uint('O')<<16 | uint('I')<<8 | uint('N'))
	frameTypeLeave = frameType(uint('L')<<24 | uint('E')<<16 | uint('A')<<8 | uint('V'))

	// single node
	frameTypeInfo = frameType(uint('I')<<24 | uint('N')<<16 | uint('F')<<8 | uint('O'))
	frameTypePing = frameType(uint('P')<<24 | uint('I')<<16 | uint('N')<<8 | uint('G'))
	frameTypeFwd  = frameType(uint('F')<<24 | uint('W')<<16 | uint('D')<<8 | uint(' '))
	frameTypeAck  = frameType(uint('A')<<24 | uint('C')<<16 | uint('K')<<8 | uint(' '))

	// pubsub
	frameTypePub = frameType(uint('P')<<24 | uint('U')<<16 | uint('B')<<8 | uint(' '))
)

func (t frameType) String() string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(t))
	res := buf[:]
	if res[3] == ' ' {
		res = buf[:3]
	}
	return string(res)
}

type frame []byte

func frameFromBytes(p []byte) (frame, error) {
	if len(p) < headerLen {
		return nil, errMalformedFrame
	}
	return frame(p), nil
}

func (f frame) typ() frameType {
	return frameType(binary.BigEndian.Uint32(f[4:8]))
}

func (f frame) payload() []byte {
	plen := binary.BigEndian.Uint32(f[8:12])
	return f[headerLen : headerLen+plen]
}

func (f *frame) reset(typ frameType, payloadSize int) {
	*f = alloc(headerLen+payloadSize, *f)
	binary.BigEndian.PutUint32((*f)[0:4], protocolVersion)
	binary.BigEndian.PutUint32((*f)[4:8], uint32(typ))
	binary.BigEndian.PutUint32((*f)[8:12], uint32(payloadSize))
}

// join
type join struct {
	sender key
}

func marshalJoin(join join, buf frame) frame {
	buf.reset(frameTypeJoin, len(join.sender))
	copy(buf.payload(), join.sender)
	return buf
}

func unmarshalJoin(f frame) (join join, err error) {
	if join.sender, err = keyFromBytes(f.payload()); err != nil {
		return join, errMalformedJoin
	}
	return join, nil
}

// leave
type leave struct {
	node key
}

func marshalLeave(leave leave, buf frame) frame {
	buf.reset(frameTypeLeave, len(leave.node))
	copy(buf.payload(), leave.node)
	return buf
}

func unmarshalLeave(f frame) (leave leave, err error) {
	if leave.node, err = keyFromBytes(f.payload()); err != nil {
		return leave, errMalformedLeave
	}
	return leave, nil
}

// info
type info struct {
	id        uint64
	neighbors keys
}

func marshalInfo(info info, buf frame) frame {
	buf.reset(frameTypeInfo, 8+len(info.neighbors))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, info.id)
	copy(p[8:], info.neighbors)
	return buf
}

func unmarshalInfo(f frame) (info info, err error) {
	p := f.payload()
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

func marshalPing(ping ping, buf frame) frame {
	buf.reset(frameTypePing, 8+len(ping.sender))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, ping.id)
	copy(p[8:], ping.sender)
	return buf
}

func unmarshalPing(f frame) (ping ping, err error) {
	p := f.payload()
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

func marshalAck(ack ack, buf frame) frame {
	var errtext string
	if ack.err != nil {
		errtext = ack.err.Error()
	}
	buf.reset(frameTypeAck, 8+len(errtext))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, ack.id)
	copy(p[8:], errtext)
	return buf
}

func unmarshalAck(f frame) (ack ack, err error) {
	p := f.payload()
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

func marshalPub(pub pub, buf frame) frame {
	buf.reset(frameTypePub, pubSize(pub))
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

func unmarshalPub(f frame) (pub, error) {
	return unmarshalPubFrom(f.payload())
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

func marshalFwd(fwd fwd, buf frame) frame {
	buf.reset(frameTypeFwd, 8+2*KeySize+4+len(fwd.stream)+pubSize(fwd.pub))
	p := buf.payload()
	binary.BigEndian.PutUint64(p, fwd.id)
	copy(p[8:], fwd.origin)
	copy(p[8+KeySize:], fwd.key)
	binary.BigEndian.PutUint32(p[8+2*KeySize:], uint32(len(fwd.stream)))
	copy(p[12+2*KeySize:], fwd.stream)
	marshalPubInto(p[12+2*KeySize+len(fwd.stream):], fwd.pub)
	return buf
}

func unmarshalFwd(f frame) (fwd fwd, err error) {
	p := f.payload()
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
