package flow

import (
	"bytes"
	"testing"
)

func TestMessageEncoding(t *testing.T) {
	codec := DefaultCodec{}

	msg := Message{
		Stream:       "message stream",
		PartitionKey: []byte("partition key"),
		Data:         []byte("message data"),
	}

	data := codec.EncodeMessage(msg)

	unmarshalled, err := codec.DecodeMessage(msg.Stream, data)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !equalMessage(msg, unmarshalled):
		t.Fatalf("unexpected unmarshalled message: %+v", unmarshalled)
	}

	// decode from invalid data
	_, err = codec.DecodeMessage(msg.Stream, []byte("invalid data"))
	if err == nil {
		t.Fatal("error expected, got none")
	}
}

func equalMessage(left, right Message) bool {
	return left.Time.Equal(right.Time) &&
		bytes.Equal(left.PartitionKey, right.PartitionKey) &&
		bytes.Equal(left.Data, right.Data)
}
