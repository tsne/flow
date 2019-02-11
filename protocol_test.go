package flow

import (
	"bytes"
	"reflect"
	"testing"
)

func TestFrameTypeString(t *testing.T) {
	frameTypeStrings := map[frameType]string{
		frameTypeJoin:  "JOIN",
		frameTypeLeave: "LEAV",
		frameTypeInfo:  "INFO",
		frameTypePing:  "PING",
		frameTypeFwd:   "FWD",
		frameTypeAck:   "ACK",
	}

	for typ, str := range frameTypeStrings {
		if s := typ.String(); s != str {
			t.Errorf("unexpected string representation for %s: %s", str, s)
		}
	}
}

func TestFrameFromBytes(t *testing.T) {
	_, err := frameFromBytes(nil)
	if err != errMalformedFrame {
		t.Fatalf("unexpected error: %v", err)
	}

	frame, err := frameFromBytes(make([]byte, frameHeaderLen))
	switch {
	case err != nil:
		t.Errorf("unexpected error: %v", err)
	case len(frame) != frameHeaderLen:
		t.Errorf("unexpected frame length: %d", len(frame))
	}
}

func TestNewFrame(t *testing.T) {
	frame := newFrame(frameTypeJoin, 7, nil)
	switch {
	case frame.typ() != frameTypeJoin:
		t.Errorf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 7:
		t.Errorf("unexpected frame payload length: %d", len(frame.payload()))
	}
}

func TestFrameTyp(t *testing.T) {
	frame := frame("....LEAV....")
	if frame.typ() != frameTypeLeave {
		t.Errorf("unexpected frame type: %s", frame.typ())
	}
}

func TestFramePayload(t *testing.T) {
	frame := frame{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 'p', 'a', 'y', 'l', 'o', 'a', 'd', '1'}
	if p := frame.payload(); string(p) != "payload" {
		t.Errorf("unexpected frame payload: %s", p)
	}
}

func TestMarshalJoin(t *testing.T) {
	join := join{
		sender: intKey(7),
	}

	frame := marshalJoin(join)
	switch {
	case frame.typ() != frameTypeJoin:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != KeySize:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err := unmarshalJoin(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(join, unmarshalled):
		t.Fatalf("unexpected join frame: %#v", unmarshalled)
	}
}

func TestMarshalLeave(t *testing.T) {
	leave := leave{
		node: intKey(7),
	}

	frame := marshalLeave(leave)
	switch {
	case frame.typ() != frameTypeLeave:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != KeySize:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err := unmarshalLeave(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(leave, unmarshalled):
		t.Fatalf("unexpected leave frame: %#v", unmarshalled)
	}
}

func TestMarshalInfo(t *testing.T) {
	info := info{
		id:        7,
		neighbors: intKeys(1, 2, 3, 4),
	}

	frame := marshalInfo(info)
	switch {
	case frame.typ() != frameTypeInfo:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 8+4*KeySize:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err := unmarshalInfo(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(info, unmarshalled):
		t.Fatalf("unexpected info frame: %#v", unmarshalled)
	}
}

func TestMarshalPing(t *testing.T) {
	ping := ping{
		id:     7,
		sender: intKey(1),
	}

	frame := marshalPing(ping, nil)
	switch {
	case frame.typ() != frameTypePing:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 8+KeySize:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err := unmarshalPing(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ping, unmarshalled):
		t.Fatalf("unexpected ping frame: %#v", unmarshalled)
	}
}

func TestMarshalAck(t *testing.T) {
	ack := ack{
		id:   7,
		data: []byte("ack data"),
	}

	frame := marshalAck(ack)
	switch {
	case frame.typ() != frameTypeAck:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 16:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err := unmarshalAck(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ack, unmarshalled):
		t.Fatalf("unexpected ack frame: %#v", unmarshalled)
	}
}

func TestMarshalFwd(t *testing.T) {
	key := intKey(1)
	fwd := fwd{
		id:  7,
		ack: key,
		msg: msg{
			id:     13,
			reply:  []byte("reply"),
			stream: []byte("stream"),
			pkey:   []byte("partition key"),
			data:   []byte("payload"),
		},
	}

	frame := marshalFwd(fwd)
	switch {
	case frame.typ() != frameTypeFwd:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 59+KeySize:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err := unmarshalFwd(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(fwd, unmarshalled):
		t.Fatalf("unexpected fwd frame: %#v", unmarshalled)
	}
}

func newMsg(m Message) msg {
	return msg{
		stream: []byte(m.Stream),
		pkey:   m.PartitionKey,
		data:   m.Data,
	}
}

func equalMsg(left, right msg) bool {
	return bytes.Equal(left.stream, right.stream) &&
		bytes.Equal(left.pkey, right.pkey) &&
		bytes.Equal(left.data, right.data)
}
