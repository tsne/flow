package flow

import (
	"bytes"
	"testing"
)

func TestMessageEncoding(t *testing.T) {
	msg := Message{
		Stream:       "message stream",
		Source:       []byte("message source"),
		PartitionKey: []byte("partition key"),
		Data:         []byte("message data"),
	}

	data := msg.Encode()

	var unmarshalled Message
	err := unmarshalled.Decode(data)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !equalMessage(msg, unmarshalled):
		t.Fatalf("unexpected unmarshalled message: %+v", unmarshalled)
	}

	// decode from invalid data
	err = unmarshalled.Decode([]byte("invalid data"))
	switch {
	case err == nil:
		t.Fatal("error expected, got none")
	case !equalMessage(msg, unmarshalled): // should be untouched
		t.Fatalf("unexpected message: %+v", unmarshalled)
	}
}

func equalMessage(left, right Message) bool {
	return bytes.Equal(left.Source, right.Source) &&
		left.Time.Equal(right.Time) &&
		bytes.Equal(left.PartitionKey, right.PartitionKey) &&
		bytes.Equal(left.Data, right.Data)
}
