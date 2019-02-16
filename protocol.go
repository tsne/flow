package flow

import (
	"encoding/binary"
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
	frameHeaderLen  = 12 // version + frame type + payload length
)

type frameType uint

const (
	// clique
	frameTypeJoin  = frameType(uint('J')<<24 | uint('O')<<16 | uint('I')<<8 | uint('N'))
	frameTypeLeave = frameType(uint('L')<<24 | uint('E')<<16 | uint('A')<<8 | uint('V'))
	frameTypeInfo  = frameType(uint('I')<<24 | uint('N')<<16 | uint('F')<<8 | uint('O'))
	frameTypePing  = frameType(uint('P')<<24 | uint('I')<<16 | uint('N')<<8 | uint('G'))

	// messages
	frameTypeMsg = frameType(uint('M')<<24 | uint('S')<<16 | uint('G')<<8 | uint(' '))
	frameTypeFwd = frameType(uint('F')<<24 | uint('W')<<16 | uint('D')<<8 | uint(' '))
	frameTypeAck = frameType(uint('A')<<24 | uint('C')<<16 | uint('K')<<8 | uint(' '))
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

func newFrame(typ frameType, payloadSize int, buf frame) frame {
	buf = alloc(frameHeaderLen+payloadSize, buf)
	binary.BigEndian.PutUint32(buf[0:4], protocolVersion)
	binary.BigEndian.PutUint32(buf[4:8], uint32(typ))
	binary.BigEndian.PutUint32(buf[8:12], uint32(payloadSize))
	return buf
}

func unmarshalFrame(p []byte) (frame, error) {
	if len(p) < frameHeaderLen || len(p) < frameHeaderLen+frame(p).payloadSize() {
		return nil, errMalformedFrame
	}
	return frame(p), nil
}

func (f frame) typ() frameType {
	return frameType(binary.BigEndian.Uint32(f[4:8]))
}

func (f frame) payloadSize() int {
	return int(binary.BigEndian.Uint32(f[8:]))
}

func (f frame) payload() []byte {
	return f[frameHeaderLen : frameHeaderLen+f.payloadSize()]
}

// join
type join struct {
	sender Key
}

func marshalJoin(join join) frame {
	f := newFrame(frameTypeJoin, keySize, nil)
	marshalKey(join.sender, f.payload())
	return f
}

func unmarshalJoin(f frame) (join join, err error) {
	if join.sender, err = unmarshalKey(f.payload()); err != nil {
		return join, errMalformedJoin
	}
	return join, nil
}

// leave
type leave struct {
	node Key
}

func marshalLeave(leave leave) frame {
	f := newFrame(frameTypeLeave, keySize, nil)
	marshalKey(leave.node, f.payload())
	return f
}

func unmarshalLeave(f frame) (leave leave, err error) {
	if leave.node, err = unmarshalKey(f.payload()); err != nil {
		return leave, errMalformedLeave
	}
	return leave, nil
}

// info
type info struct {
	id        uint64
	neighbors keys
}

func marshalInfo(info info) frame {
	f := newFrame(frameTypeInfo, 8+len(info.neighbors), nil)
	p := f.payload()

	binary.BigEndian.PutUint64(p, info.id)
	copy(p[8:], info.neighbors)
	return f
}

func unmarshalInfo(f frame) (info info, err error) {
	p := f.payload()
	if len(p) < 8 {
		return info, errMalformedInfo
	}

	info.id = binary.BigEndian.Uint64(p)
	if info.neighbors, err = unmarshalKeys(p[8:]); err != nil {
		return info, errMalformedInfo
	}
	return info, nil
}

// ping
type ping struct {
	id     uint64 // used for replies
	sender Key
}

func marshalPing(ping ping, buf frame) frame {
	f := newFrame(frameTypePing, 8+keySize, buf)
	p := f.payload()

	binary.BigEndian.PutUint64(p, ping.id)
	marshalKey(ping.sender, p[8:])
	return f
}

func unmarshalPing(f frame) (ping ping, err error) {
	p := f.payload()
	if len(p) < 8+keySize {
		return ping, errMalformedPing
	}

	ping.id = binary.BigEndian.Uint64(p)
	if ping.sender, err = unmarshalKey(p[8:]); err != nil {
		return ping, errMalformedPing
	}
	return ping, nil
}

// msg
type msg struct {
	id     uint64
	reply  []byte // node stream of sender
	stream []byte
	pkey   []byte
	data   []byte
}

func msgSize(msg msg) int {
	return 20 + len(msg.reply) + len(msg.stream) + len(msg.pkey) + len(msg.data)
}

func marshalMsgPayload(msg msg, p []byte) {
	replyLen := uint32(len(msg.reply))
	streamLen := uint32(len(msg.stream))
	pkeyLen := uint32(len(msg.pkey))

	binary.BigEndian.PutUint64(p, msg.id)
	binary.BigEndian.PutUint32(p[8:], replyLen)
	copy(p[12:], msg.reply)
	binary.BigEndian.PutUint32(p[12+replyLen:], streamLen)
	copy(p[16+replyLen:], msg.stream)
	binary.BigEndian.PutUint32(p[16+replyLen+streamLen:], pkeyLen)
	copy(p[20+replyLen+streamLen:], msg.pkey)
	copy(p[20+replyLen+streamLen+pkeyLen:], msg.data)
}

func unmarshalMsgPayload(p []byte) (msg msg, err error) {
	const minSize = 20

	if len(p) < minSize {
		return msg, errMalformedMsg
	}

	replyLen := binary.BigEndian.Uint32(p[8:])
	if len(p) < minSize+int(replyLen) {
		return msg, errMalformedMsg
	}
	streamLen := binary.BigEndian.Uint32(p[12+replyLen:])
	if len(p) < minSize+int(replyLen+streamLen) {
		return msg, errMalformedMsg
	}
	pkeyLen := binary.BigEndian.Uint32(p[16+replyLen+streamLen:])
	if len(p) < minSize+int(replyLen+streamLen+pkeyLen) {
		return msg, errMalformedMsg
	}

	msg.id = binary.BigEndian.Uint64(p)
	msg.reply = p[12 : 12+replyLen]
	msg.stream = p[16+replyLen : 16+replyLen+streamLen]
	msg.pkey = p[20+replyLen+streamLen : 20+replyLen+streamLen+pkeyLen]
	msg.data = p[20+replyLen+streamLen+pkeyLen:]
	return msg, nil
}

func marshalMsg(msg msg) frame {
	f := newFrame(frameTypeMsg, msgSize(msg), nil)
	marshalMsgPayload(msg, f.payload())
	return f
}

func unmarshalMsg(f frame) (msg msg, err error) {
	return unmarshalMsgPayload(f.payload())
}

// fwd
type fwd struct {
	id  uint64
	ack Key
	msg msg // needs to be last
}

func marshalFwd(fwd fwd) frame {
	f := newFrame(frameTypeFwd, 8+keySize+msgSize(fwd.msg), nil)
	p := f.payload()

	binary.BigEndian.PutUint64(p, fwd.id)
	marshalKey(fwd.ack, p[8:])
	marshalMsgPayload(fwd.msg, p[8+keySize:])
	return f
}

func unmarshalFwd(f frame) (fwd fwd, err error) {
	p := f.payload()
	if len(p) < 8+keySize {
		return fwd, errMalformedFwd
	}

	fwd.id = binary.BigEndian.Uint64(p)
	if fwd.ack, err = unmarshalKey(p[8 : 8+keySize]); err != nil {
		return fwd, errMalformedFwd
	}
	if fwd.msg, err = unmarshalMsgPayload(p[8+keySize:]); err != nil {
		return fwd, errMalformedFwd
	}
	return fwd, nil
}

// ack
type ack struct {
	id   uint64
	data []byte
}

func marshalAck(ack ack) frame {
	f := newFrame(frameTypeAck, 8+len(ack.data), nil)
	p := f.payload()

	binary.BigEndian.PutUint64(p, ack.id)
	copy(p[8:], ack.data)
	return f
}

func unmarshalAck(f frame) (ack ack, err error) {
	p := f.payload()
	if len(p) < 8 {
		return ack, errMalformedAck
	}

	ack.id = binary.BigEndian.Uint64(p)
	ack.data = p[8:]
	return ack, nil
}
