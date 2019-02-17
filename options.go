package flow

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const defaultClique = "_global"

// Option represents an option which can be used to configure
// a broker.
type Option func(*options) error

// Stabilization defines the stabilization parameters. A stabilization
// spreads the node-local information of the clique view and therefore
// stabilizes the whole clique.
type Stabilization struct {
	Successors  int           // number of successors to spread local clique structure
	Stabilizers int           // number of pinged nodes
	Interval    time.Duration // duration for the stabilization interval
}

type options struct {
	messageHandlers map[string]MessageHandler
	requestHandlers map[string]RequestHandler
	codec           Codec
	errorHandler    func(error)
	partitionLocks  []sync.Mutex
	clique          string
	nodeKey         Key
	hasNodeKey      bool
	stabilization   Stabilization
	ackTimeout      time.Duration
	reqTimeout      time.Duration
}

func defaultOptions() options {
	// TODO: verify defaults
	return options{
		messageHandlers: make(map[string]MessageHandler),
		requestHandlers: make(map[string]RequestHandler),
		codec:           DefaultCodec{},
		errorHandler:    func(err error) { fmt.Fprintln(os.Stderr, err) },
		clique:          defaultClique,
		stabilization: Stabilization{
			Successors:  5,
			Stabilizers: 5,
			Interval:    10 * time.Second,
		},
		ackTimeout: 500 * time.Millisecond,
		reqTimeout: time.Second,
	}
}

func (o *options) apply(opts ...Option) error {
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return err
		}
	}

	if o.messageHandlers[o.clique] != nil || o.requestHandlers[o.clique] != nil {
		return optionError("cannot subscribe to clique stream")
	} else if node := nodeStream(o.clique, o.nodeKey); o.messageHandlers[node] != nil || o.requestHandlers[node] != nil {
		return optionError("cannot subscribe to node stream")
	}

	if !o.hasNodeKey {
		var err error
		if o.nodeKey, err = RandomKey(); err != nil {
			return err
		}
	}
	return nil
}

// WithMessageHandler defines a handler for incoming messages of the
// specified stream. These messages are partitioned within the clique
// the broker is assigned to. A broker can only have a single message
// or request handler per stream.
func WithMessageHandler(stream string, h MessageHandler) Option {
	return func(o *options) error {
		switch {
		case stream == "":
			return optionError("no message stream specified")
		case h == nil:
			return optionError("no message handler specified")
		case o.messageHandlers[stream] != nil:
			return optionError("message handler for stream '" + stream + "' already exists")
		case o.requestHandlers[stream] != nil:
			return optionError("request handler for stream '" + stream + "' already exists")
		}

		o.messageHandlers[stream] = h
		return nil
	}
}

// WithRequestHandler defines a handler for incoming requests of the
// specified stream. These requests are partitioned within the clique
// the broker is assigned to. A broker can only have a single message
// or request handler per stream.
func WithRequestHandler(stream string, h RequestHandler) Option {
	return func(o *options) error {
		switch {
		case stream == "":
			return optionError("no request stream specified")
		case h == nil:
			return optionError("no request handler specified")
		case o.messageHandlers[stream] != nil:
			return optionError("message handler for stream '" + stream + "' already exists")
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

// WithPartition assigns a clique and a key to the broker.
// All broker within a clique are arranged in a ring structure
// and eventually know each other. If the clique name is empty,
// a global default clique will be assigned.
// The key defines the position in the clique's ring structure and
// is used to partition message delivery. It should be unique within
// the assigned clique.
func WithPartition(clique string, key Key) Option {
	return func(o *options) error {
		if clique == "" {
			clique = defaultClique
		}

		o.clique = clique
		o.nodeKey = key
		o.hasNodeKey = true
		return nil
	}
}

// WithSyncPartitions instructs the broker to run at most 'slots' message or
// request handlers in parallel. All partitions will be processed synchronously,
// i.e. a message or request handler will not be called in parallel for the same
// partition key. Messages without a partition key are still processed in
// parallel.
func WithSyncPartitions(slots int) Option {
	return func(o *options) error {
		if slots <= 0 {
			return optionError("non-positive number of synchronous partition slots")
		}

		o.partitionLocks = make([]sync.Mutex, slots)
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

// WithRequestTimeout defines the timeout for responding to sent requests.
func WithRequestTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return optionError("non-positive request timeout")
		}
		o.reqTimeout = d
		return nil
	}
}
