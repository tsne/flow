package flow

import (
	"reflect"
	"testing"
	"time"
)

func TestMsgTypeString(t *testing.T) {
	msgTypeStrings := map[msgType]string{
		msgTypeJoin:  "JOIN",
		msgTypeLeave: "LEAV",
		msgTypeInfo:  "INFO",
		msgTypePing:  "PING",
		msgTypeFwd:   "FWD",
		msgTypeAck:   "ACK",
		msgTypePub:   "PUB",
	}

	for typ, str := range msgTypeStrings {
		if s := typ.String(); s != str {
			t.Fatalf("unexpected string representation for %s: %s", str, s)
		}
	}
}

func TestMessageFromBytes(t *testing.T) {
	_, err := messageFromBytes(nil)
	if err != errMalformedMessage {
		t.Fatalf("unexpected error: %v", err)
	}

	msg, err := messageFromBytes(make([]byte, headerLen))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(msg) != headerLen:
		t.Fatalf("unexpected length of message: %d", len(msg))
	}
}

func TestMessageTyp(t *testing.T) {
	msg := message("....LEAV....")
	if msg.typ() != msgTypeLeave {
		t.Fatalf("unexpected message type: %s", msg.typ())
	}
}

func TestMessagePayload(t *testing.T) {
	msg := message{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 'p', 'a', 'y', 'l', 'o', 'a', 'd', '1'}
	if p := msg.payload(); string(p) != "payload" {
		t.Fatalf("unexpected message payload: %s", p)
	}
}

func TestMessageReset(t *testing.T) {
	var msg message
	msg.reset(msgTypeJoin, 7)
	switch {
	case msg.typ() != msgTypeJoin:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != 7:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}
}

func TestJoinMsgMarshalling(t *testing.T) {
	join := joinMsg{
		sender: intKey(7),
	}

	msg := join.marshal(nil)
	switch {
	case msg.typ() != msgTypeJoin:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != KeySize:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	var unmarshalled joinMsg
	err := unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(join, unmarshalled):
		t.Fatalf("unexpected join message: %#v", unmarshalled)
	}
}

func TestLeaveMsgMarshalling(t *testing.T) {
	leave := leaveMsg{
		node: intKey(7),
	}

	msg := leave.marshal(nil)
	switch {
	case msg.typ() != msgTypeLeave:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != KeySize:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	var unmarshalled leaveMsg
	err := unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(leave, unmarshalled):
		t.Fatalf("unexpected leave message: %#v", unmarshalled)
	}
}

func TestInfoMsgMarshalling(t *testing.T) {
	info := infoMsg{
		id:        7,
		neighbors: intKeys(1, 2, 3, 4),
	}

	msg := info.marshal(nil)
	switch {
	case msg.typ() != msgTypeInfo:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != 8+4*KeySize:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	var unmarshalled infoMsg
	err := unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(info, unmarshalled):
		t.Fatalf("unexpected info message: %#v", unmarshalled)
	}
}

func TestPingMsgMarshalling(t *testing.T) {
	ping := pingMsg{
		id:     7,
		sender: intKey(1),
	}

	msg := ping.marshal(nil)
	switch {
	case msg.typ() != msgTypePing:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != 8+KeySize:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	var unmarshalled pingMsg
	err := unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ping, unmarshalled):
		t.Fatalf("unexpected ping message: %#v", unmarshalled)
	}
}

func TestAckMsgMarshalling(t *testing.T) {
	ack := ackMsg{
		id:  7,
		err: ackError("error text"),
	}

	msg := ack.marshal(nil)
	switch {
	case msg.typ() != msgTypeAck:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != 18:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	var unmarshalled ackMsg
	err := unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ack, unmarshalled):
		t.Fatalf("unexpected ack message: %#v", unmarshalled)
	}

	// no error
	ack = ackMsg{
		id: 7,
	}

	msg = ack.marshal(nil)
	switch {
	case msg.typ() != msgTypeAck:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != 8:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	err = unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(ack, unmarshalled):
		t.Fatalf("unexpected ack message: %#v", unmarshalled)
	}
}

func TestPubMsgMarshalling(t *testing.T) {
	pub := pubMsg{
		source:       []byte("source id"),
		time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
		partitionKey: []byte("partition key"),
		payload:      []byte("payload"),
	}

	msg := pub.marshal(nil)
	switch {
	case msg.typ() != msgTypePub:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != 49:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	var unmarshalled pubMsg
	err := unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(pub, unmarshalled):
		t.Fatalf("unexpected pub message: %#v", unmarshalled)
	}
}

func TestFwdMsgMarshalling(t *testing.T) {
	keys := intKeys(1, 2)
	fwd := fwdMsg{
		id:     7,
		origin: keys.at(0),
		key:    keys.at(1),
		stream: "stream",
		pubMsg: pubMsg{
			source:       []byte("source id"),
			time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
			partitionKey: []byte("partition key"),
			payload:      []byte("payload"),
		},
	}

	msg := fwd.marshal(nil)
	switch {
	case msg.typ() != msgTypeFwd:
		t.Fatalf("unexpected message type: %s", msg.typ())
	case len(msg.payload()) != 67+2*KeySize:
		t.Fatalf("unexpected message payload length: %d", len(msg.payload()))
	}

	var unmarshalled fwdMsg
	err := unmarshalled.unmarshal(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !reflect.DeepEqual(fwd, unmarshalled):
		t.Fatalf("unexpected fwd message: %#v", unmarshalled)
	}
}
