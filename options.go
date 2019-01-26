package flow

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"
)

const defaultGroupName = "_global"

// Option represents an option which can be used to configure
// a broker.
type Option func(*options) error

// Stabilization defines the stabilization parameters. A stabilization
// spreads the node-local information of the group view and therefore
// stabilizes the whole group.
type Stabilization struct {
	Successors  int           // number of successors to spread local group structure
	Stabilizers int           // number of pinged nodes
	Interval    time.Duration // duration for the stabilization interval
}

type options struct {
	messageHandlers map[string][]MessageHandler
	requestHandlers map[string]RequestHandler
	codec           Codec
	errorHandler    func(error)
	groupName       string
	nodeKey         key
	stabilization   Stabilization
	ackTimeout      time.Duration
}

func defaultOptions() options {
	// TODO: verify defaults
	return options{
		messageHandlers: make(map[string][]MessageHandler),
		requestHandlers: make(map[string]RequestHandler),
		codec:           DefaultCodec{},
		errorHandler:    func(err error) { fmt.Fprintln(os.Stderr, err) },
		groupName:       defaultGroupName,
		nodeKey:         nil, // will be set in 'apply'
		stabilization: Stabilization{
			Successors:  5,
			Stabilizers: 5,
			Interval:    10 * time.Second,
		},
		ackTimeout: 750 * time.Millisecond,
	}
}

func (o *options) apply(opts ...Option) error {
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return err
		}
	}

	if len(o.nodeKey) == 0 {
		o.nodeKey = alloc(KeySize, nil)
		if err := randomKey(o.nodeKey); err != nil {
			return err
		}
	}
	return nil
}

// WithMessageHandler defines a handler for incoming messages of the
// specified stream. These messages are partitioned within the group
// the broker is assigned to. A broker can have multiple message
// handler per stream.
func WithMessageHandler(stream string, h MessageHandler) Option {
	return func(o *options) error {
		switch {
		case stream == "":
			return optionError("no message stream specified")
		case h == nil:
			return optionError("no message handler specified")
		}

		o.messageHandlers[stream] = append(o.messageHandlers[stream], h)
		return nil
	}
}

// WithRequestHandler defines a handler for incoming requests of the
// specified stream. These requests are partitioned within the group
// the broker is assigned to. A broker can only have a one request
// handler per stream.
func WithRequestHandler(stream string, h RequestHandler) Option {
	return func(o *options) error {
		switch {
		case stream == "":
			return optionError("no request stream specified")
		case h == nil:
			return optionError("no request handler specified")
		case o.requestHandlers[stream] != nil:
			return optionError("request handler for stream '" + stream + "' already exists")
		}

		o.requestHandlers[stream] = h
		return nil
	}
}

// WithErrorHandler uses the given handler function to report errors, which
// occur concurrently (e.g. when receiving an invalid subscribed message).
// If no error handler is assigned, these errors will be logged to stderr.
func WithErrorHandler(f func(error)) Option {
	return func(o *options) error {
		if f == nil {
			return optionError("no error handler specified")
		}
		o.errorHandler = f
		return nil
	}
}

// WithPartition assigns a group and a key to the broker.
// All broker within a group are arranged in a ring structure
// and eventually know each other. If the group is empty, a global
// default group will be assigned.
// The key defines the position in the group's ring structure and
// is used to partition message delivery. It should be unique within
// the assigned group.
func WithPartition(group string, key Key) Option {
	return func(o *options) error {
		if group == "" {
			group = defaultGroupName
		}

		o.groupName = group
		o.nodeKey = alloc(KeySize, o.nodeKey)
		copy(o.nodeKey, key[:])
		return nil
	}
}

// WithCodec assigns the desired message codec to the broker. The codec
// is used to encode and decode messages to and from binary data. If no
// message codec is assigned, an internal binary format will be used instead.
func WithCodec(c Codec) Option {
	return func(o *options) error {
		if c == nil {
			return optionError("no message codec specified")
		}
		o.codec = c
		return nil
	}
}

// WithStabilization defines the stabilization parameters. All parameters are
// optional and are apply only when they are not zero.
func WithStabilization(s Stabilization) Option {
	return func(o *options) error {
		switch {
		case s.Successors < 0:
			return optionError("negative successor count")
		case s.Stabilizers < 0:
			return optionError("negative stabilizer count")
		case s.Interval < 0:
			return optionError("negative stabilization interval")
		}

		if s.Successors > 0 {
			o.stabilization.Successors = s.Successors
		}
		if s.Stabilizers > 0 {
			o.stabilization.Stabilizers = s.Stabilizers
		}
		if s.Interval > 0 {
			o.stabilization.Interval = s.Interval
		}
		return nil
	}
}

// WithAckTimeout defines the timeout for acknowledging an internally
// forwarded message.
func WithAckTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return optionError("non-positive ack timeout")
		}
		o.ackTimeout = d
		return nil
	}
}

func defaultNodeKey() (key, error) {
	k := alloc(KeySize, nil)
	_, err := rand.Read(k)
	return k, err
}
