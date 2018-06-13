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
	headerLen       = 12 // version + frame type + payload length
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

	// request/response
	frameTypeReq  = frameType(uint('R')<<24 | uint('E')<<16 | uint('Q')<<8 | uint(' '))
	frameTypeResp = frameType(uint('R')<<24 | uint('E')<<16 | uint('S')<<8 | uint('P'))
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

func newFrame(typ frameType, payloadSize int, buf frame) frame {
	buf = alloc(headerLen+payloadSize, buf)
	binary.BigEndian.PutUint32(buf[0:4], protocolVersion)
	binary.BigEndian.PutUint32(buf[4:8], uint32(typ))
	binary.BigEndian.PutUint32(buf[8:12], uint32(payloadSize))
	return buf
}

func (f frame) typ() frameType {
	return frameType(binary.BigEndian.Uint32(f[4:8]))
}

func (f frame) payload() []byte {
	plen := binary.BigEndian.Uint32(f[8:12])
	return f[headerLen : headerLen+plen]
}

// join
type join struct {
	sender key
}

func marshalJoin(join join, buf frame) frame {
	buf = newFrame(frameTypeJoin, len(join.sender), buf)
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
	buf = newFrame(frameTypeLeave, len(leave.node), buf)
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
	buf = newFrame(frameTypeInfo, 8+len(info.neighbors), buf)
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
	buf = newFrame(frameTypePing, 8+len(ping.sender), buf)
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

// fwd
type fwd struct {
	id   uint64
	ack  key
	pkey key
	msg  Message
}

func marshalFwd(fwd fwd, buf frame) frame {
	buf = newFrame(frameTypeFwd, 8+2*KeySize+messageSize(fwd.msg), buf)
	p := buf.payload()

	binary.BigEndian.PutUint64(p, fwd.id)
	copy(p[8:], fwd.ack)
	copy(p[8+KeySize:], fwd.pkey)
	marshalMessageInto(p[8+2*KeySize:], fwd.msg)
	return buf
}

func unmarshalFwd(f frame) (fwd fwd, err error) {
	p := f.payload()
	if len(p) < 8+2*KeySize {
		return fwd, errMalformedFwd
	}

	fwd.id = binary.BigEndian.Uint64(p)
	fwd.ack = p[8 : 8+KeySize]
	fwd.pkey = p[8+KeySize : 8+2*KeySize]
	if fwd.msg, err = unmarshalMessageFrom(p[8+2*KeySize:]); err != nil {
		return fwd, errMalformedFwd
	}
	return fwd, nil
}

// ack
type ack struct {
	id uint64
}

func marshalAck(ack ack, buf frame) frame {
	buf = newFrame(frameTypeAck, 8, buf)
	binary.BigEndian.PutUint64(buf.payload(), ack.id)
	return buf
}

func unmarshalAck(f frame) (ack ack, err error) {
	p := f.payload()
	if len(p) < 8 {
		return ack, errMalformedAck
	}

	ack.id = binary.BigEndian.Uint64(p)
	return ack, nil
}

// req
type req struct {
	id    uint64
	reply key
	msg   Message
}

func marshalReq(req req, buf frame) frame {
	buf = newFrame(frameTypeReq, 8+KeySize+messageSize(req.msg), buf)
	p := buf.payload()

	binary.BigEndian.PutUint64(p, req.id)
	copy(p[8:], req.reply)
	marshalMessageInto(p[8+KeySize:], req.msg)
	return buf
}

func unmarshalReq(f frame) (req req, err error) {
	p := f.payload()
	if len(p) < 8+KeySize {
		return req, errMalformedReq
	}

	req.id = binary.BigEndian.Uint64(p)
	req.reply = p[8 : 8+KeySize]
	if req.msg, err = unmarshalMessageFrom(p[8+KeySize:]); err != nil {
		return req, errMalformedReq
	}
	return req, nil
}

// resp
type resp struct {
	id  uint64
	msg Message
}

func marshalResp(resp resp, buf frame) frame {
	buf = newFrame(frameTypeResp, 8+messageSize(resp.msg), buf)
	p := buf.payload()

	binary.BigEndian.PutUint64(p, resp.id)
	marshalMessageInto(p[8:], resp.msg)
	return buf
}

func unmarshalResp(f frame) (resp resp, err error) {
	p := f.payload()
	if len(p) < 8 {
		return resp, errMalformedResp
	}

	resp.id = binary.BigEndian.Uint64(p)
	if resp.msg, err = unmarshalMessageFrom(p[8:]); err != nil {
		return resp, errMalformedResp
	}
	return resp, nil
}

// utility functions

func messageSize(msg Message) int {
	return 24 + len(msg.Stream) + len(msg.PartitionKey) + len(msg.Data)
}

func marshalMessageInto(p []byte, msg Message) {
	streamLen := uint32(len(msg.Stream))
	partitionKeyLen := uint32(len(msg.PartitionKey))
	dataLen := uint32(len(msg.Data))

	binary.BigEndian.PutUint32(p, streamLen)
	copy(p[4:], msg.Stream)
	binary.BigEndian.PutUint64(p[4+streamLen:], uint64(msg.Time.Unix()))
	binary.BigEndian.PutUint32(p[12+streamLen:], uint32(msg.Time.Nanosecond()))
	binary.BigEndian.PutUint32(p[16+streamLen:], partitionKeyLen)
	copy(p[20+streamLen:], msg.PartitionKey)
	binary.BigEndian.PutUint32(p[20+streamLen+partitionKeyLen:], dataLen)
	copy(p[24+streamLen+partitionKeyLen:], msg.Data)
}

func unmarshalMessageFrom(p []byte) (msg Message, err error) {
	if len(p) < 24 {
		return msg, errMalformedMessage
	}

	streamLen := binary.BigEndian.Uint32(p)
	partitionKeyLen := binary.BigEndian.Uint32(p[16+streamLen:])
	dataLen := binary.BigEndian.Uint32(p[20+streamLen+partitionKeyLen:])

	secs := int64(binary.BigEndian.Uint64(p[4+streamLen:]))
	nsecs := int64(binary.BigEndian.Uint32(p[12+streamLen:]))

	msg.Stream = string(p[4 : 4+streamLen])
	msg.Time = time.Unix(secs, nsecs).UTC()
	msg.PartitionKey = p[20+streamLen : 20+streamLen+partitionKeyLen]
	msg.Data = p[24+streamLen+partitionKeyLen : 24+streamLen+partitionKeyLen+dataLen]
	return msg, nil
}
