package flow

import "time"

// Message holds the data of a streaming message. A message is able to
// marshal into and unmarshal from a binary representation, which is used
// internally to send messages over the wire.
type Message struct {
	Stream       string    // published to the pub/sub system
	Time         time.Time // the time the message was created
	PartitionKey []byte    // the key for partitioning
	Data         []byte    // the data which should be sent
}

// Codec defines an interface for encoding messages to and
// decoding messages from binary data.
type Codec interface {
	EncodeMessage(msg Message) []byte
	DecodeMessage(stream string, data []byte) (Message, error)
}

// DefaultCodec represents the default codec for messages. If no custom
// codec is set, the default codec will be used for message encoding and
// decoding.
type DefaultCodec struct{}

// EncodeMessage encodes the message into its binary representation.
func (c DefaultCodec) EncodeMessage(msg Message) []byte {
	buf := alloc(messageSize(msg), nil)
	marshalMessageInto(buf, msg)
	return buf
}

// DecodeMessage decodes the message from its binary representation.
// The decoded message retains the given data to avoid copies. If the
// data buffer should be reused after the decoding, the caller is
// responsible for copying the data before passing it.
func (c DefaultCodec) DecodeMessage(stream string, data []byte) (Message, error) {
	msg, err := unmarshalMessageFrom(data)
	if err != nil {
		return msg, err
	}
	if msg.Stream == "" {
		msg.Stream = stream
	}
	return msg, nil
}
