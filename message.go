package flow

import "time"

// Message holds the data of a streaming message. A message is able to
// marshal into and unmarshal from a binary representation, which is used
// internally to send messages over the wire.
type Message struct {
	Stream       string    // published to the pub/sub system
	Source       []byte    // the source the message comes from
	Time         time.Time // the time the message was created
	PartitionKey []byte    // the key for partitioning
	Data         []byte    // the data which should be sent
}

// Encode encodes the message into its binary representation.
func (m Message) Encode() []byte {
	buf := alloc(messageSize(m), nil)
	marshalMessageInto(buf, m)
	return buf
}

// Decode decodes the message from its binary representation.
// The message retains the given data to avoid copies.
func (m *Message) Decode(data []byte) error {
	msg, err := unmarshalMessageFrom(data)
	if err != nil {
		return err
	}
	*m = msg
	return nil
}

// Codec defines an interface for encoding messages to and
// decoding messages from binary data.
type Codec interface {
	EncodeMessage(msg Message) []byte
	DecodeMessage(stream string, data []byte) (Message, error)
}

type binaryCodec struct{}

func (c binaryCodec) EncodeMessage(msg Message) []byte {
	return msg.Encode()
}

func (c binaryCodec) DecodeMessage(stream string, data []byte) (Message, error) {
	var msg Message
	if err := msg.Decode(data); err != nil {
		return msg, err
	}
	if msg.Stream == "" {
		msg.Stream = stream
	}
	return msg, nil
}
