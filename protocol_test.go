package flow

import (
	"reflect"
	"testing"
	"time"
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

	frame, err := frameFromBytes(make([]byte, headerLen))
	switch {
	case err != nil:
		t.Errorf("unexpected error: %v", err)
	case len(frame) != headerLen:
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

	frame := marshalJoin(join, nil)
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

	frame := marshalLeave(leave, nil)
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

	frame := marshalInfo(info, nil)
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
		id:  7,
		err: ackError("error text"),
	}

	frame := marshalAck(ack, nil)
	switch {
	case frame.typ() != frameTypeAck:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 18:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err := unmarshalAck(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ack, unmarshalled):
		t.Fatalf("unexpected ack frame: %#v", unmarshalled)
	}

	// no error
	ack.err = nil

	frame = marshalAck(ack, nil)
	switch {
	case frame.typ() != frameTypeAck:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 8:
		t.Fatalf("unexpected frame payload length: %d", len(frame.payload()))
	}

	unmarshalled, err = unmarshalAck(frame)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ack, unmarshalled):
		t.Fatalf("unexpected ack frame: %#v", unmarshalled)
	}
}

func TestMarshalFwd(t *testing.T) {
	keys := intKeys(1, 2)
	fwd := fwd{
		id:     7,
		origin: keys.at(0),
		key:    keys.at(1),
		msg: Message{
			Stream:       "stream",
			Source:       []byte("source id"),
			Time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
			PartitionKey: []byte("partition key"),
			Data:         []byte("payload"),
		},
	}

	frame := marshalFwd(fwd, nil)
	switch {
	case frame.typ() != frameTypeFwd:
		t.Fatalf("unexpected frame type: %s", frame.typ())
	case len(frame.payload()) != 71+2*KeySize:
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
