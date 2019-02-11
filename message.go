package flow

import "encoding/binary"

// Message holds the data of a streaming message. A message is able to
// marshal into and unmarshal from a binary representation, which is used
// internally to send messages over the wire.
type Message struct {
	Stream       string // published to the pub/sub system
	PartitionKey []byte // the key for partitioning
	Data         []byte // the data which should be sent
}

func (m *Message) validate() error {
	if m.Stream == "" {
		return errorString("missing message stream")
	}
	return nil
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
	streamLen := len(msg.Stream)
	pkeyLen := len(msg.PartitionKey)

	p := alloc(8+streamLen+pkeyLen+len(msg.Data), nil)
	binary.BigEndian.PutUint32(p, uint32(streamLen))
	copy(p[4:], msg.Stream)
	binary.BigEndian.PutUint32(p[4+streamLen:], uint32(pkeyLen))
	copy(p[8+streamLen:], msg.PartitionKey)
	copy(p[8+streamLen+pkeyLen:], msg.Data)
	return p
}

// DecodeMessage decodes the message from its binary representation.
// The decoded message retains the given data to avoid copies. If the
// data buffer should be reused after the decoding, the caller is
// responsible for copying the data before passing it.
func (c DefaultCodec) DecodeMessage(stream string, p []byte) (Message, error) {
	const minSize = 8

	if len(p) < minSize {
		return Message{}, errMalformedMessage
	}

	streamLen := binary.BigEndian.Uint32(p)
	if len(p) < minSize+int(streamLen) {
		return Message{}, errMalformedMessage
	}

	pkeyLen := binary.BigEndian.Uint32(p[4+streamLen:])
	if len(p) < minSize+int(streamLen+pkeyLen) {
		return Message{}, errMalformedMessage
	}

	msg := Message{
		Stream:       string(p[4 : 4+streamLen]),
		PartitionKey: p[8+streamLen : 8+streamLen+pkeyLen],
		Data:         p[8+streamLen+pkeyLen:],
	}

	if msg.Stream == "" {
		msg.Stream = stream
	}
	return msg, nil
}
