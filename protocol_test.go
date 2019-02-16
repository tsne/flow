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
		frameTypeMsg:   "MSG",
		frameTypeFwd:   "FWD",
		frameTypeAck:   "ACK",
	}

	for typ, str := range frameTypeStrings {
		if s := typ.String(); s != str {
			t.Errorf("unexpected string representation for %s: %s", str, s)
		}
	}
}

func TestNewFrame(t *testing.T) {
	frame := newFrame(frameTypeJoin, 7, nil)
	switch {
	case frame.typ() != frameTypeJoin:
		t.Errorf("unexpected frame type: %s", frame.typ())
	case frame.payloadSize() != 7:
		t.Errorf("unexpected frame payload size: %d", frame.payloadSize())
	}
}

func TestUnmarshalFrame(t *testing.T) {
	_, err := unmarshalFrame(nil)
	if err != errMalformedFrame {
		t.Fatalf("unexpected error: %v", err)
	}

	frame, err := unmarshalFrame(make([]byte, frameHeaderLen))
	switch {
	case err != nil:
		t.Errorf("unexpected error: %v", err)
	case len(frame) != frameHeaderLen:
		t.Errorf("unexpected frame length: %d", len(frame))
	}
}

func TestFrameTyp(t *testing.T) {
	frames := map[frameType]frame{
		frameTypeJoin:  frame("....JOIN...."),
		frameTypeLeave: frame("....LEAV...."),
		frameTypeInfo:  frame("....INFO...."),
		frameTypePing:  frame("....PING...."),
		frameTypeMsg:   frame("....MSG ...."),
		frameTypeFwd:   frame("....FWD ...."),
		frameTypeAck:   frame("....ACK ...."),
	}

	for typ, frame := range frames {
		if frame.typ() != typ {
			t.Errorf("unexpected frame type for %q: %s", frame, frame.typ())
		}
	}

}

func TestFramePayloadSize(t *testing.T) {
	frame := frame{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7}
	if size := frame.payloadSize(); size != 7 {
		t.Errorf("unexpected payload size: %d", size)
	}
}

func TestFramePayload(t *testing.T) {
	frame := frame{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 'p', 'a', 'y', 'l', 'o', 'a', 'd', '1'}
	if p := frame.payload(); string(p) != "payload" {
		t.Errorf("unexpected payload: %s", p)
	}
}

func TestMarshalJoin(t *testing.T) {
	join := join{
		sender: 7,
	}

	frame := marshalJoin(join)
	switch {
	case frame.typ() != frameTypeJoin:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case frame.payloadSize() != keySize:
		t.Fatalf("unexpected frame payload size: %d", frame.payloadSize())
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
		node: 7,
	}

	frame := marshalLeave(leave)
	switch {
	case frame.typ() != frameTypeLeave:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case frame.payloadSize() != keySize:
		t.Fatalf("unexpected frame payload size: %d", frame.payloadSize())
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
		id: 7,
		neighbors: keys{
			0, 0, 0, 0, 0, 0, 0, 1,
			0, 0, 0, 0, 0, 0, 0, 2,
			0, 0, 0, 0, 0, 0, 0, 3,
			0, 0, 0, 0, 0, 0, 0, 4,
		},
	}

	frame := marshalInfo(info)
	switch {
	case frame.typ() != frameTypeInfo:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case frame.payloadSize() != 8+4*keySize:
		t.Fatalf("unexpected frame payload size: %d", frame.payloadSize())
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
		sender: 1,
	}

	frame := marshalPing(ping, nil)
	switch {
	case frame.typ() != frameTypePing:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case frame.payloadSize() != 8+keySize:
		t.Fatalf("unexpected frame payload size: %d", frame.payloadSize())
	}

	unmarshalled, err := unmarshalPing(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ping, unmarshalled):
		t.Fatalf("unexpected ping frame: %#v", unmarshalled)
	}
}

func TestMarshalMsg(t *testing.T) {
	msg := msg{
		id:     7,
		reply:  []byte("reply"),
		stream: []byte("stream"),
		pkey:   []byte("pkey"),
		data:   []byte("data"),
	}

	if size := msgSize(msg); size != 39 {
		t.Fatalf("unexpected msg size: %d", size)
	}

	frame := marshalMsg(msg)
	switch {
	case frame.typ() != frameTypeMsg:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case frame.payloadSize() != 39:
		t.Fatalf("unexpected frame payload size: %d", frame.payloadSize())
	}

	unmarshalled, err := unmarshalMsg(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(msg, unmarshalled):
		t.Fatalf("unexpected msg frame: %#v", unmarshalled)
	}
}

func TestMarshalFwd(t *testing.T) {
	fwd := fwd{
		id:  7,
		ack: 1,
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
	case frame.payloadSize() != 59+keySize:
		t.Fatalf("unexpected frame payload size: %d", frame.payloadSize())
	}

	unmarshalled, err := unmarshalFwd(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(fwd, unmarshalled):
		t.Fatalf("unexpected fwd frame: %#v", unmarshalled)
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
	case frame.payloadSize() != 16:
		t.Fatalf("unexpected frame payload size: %d", frame.payloadSize())
	}

	unmarshalled, err := unmarshalAck(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ack, unmarshalled):
		t.Fatalf("unexpected ack frame: %#v", unmarshalled)
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
