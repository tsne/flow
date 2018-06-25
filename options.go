package flow

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"
)

// Option represents an option which can be used to configure
// a broker.
type Option func(*options) error

type options struct {
	codec        Codec
	errorHandler func(error)

	groupName string
	nodeKey   key

	successorCount  int
	stabilizerCount int

	stabilizationInterval time.Duration
	ackTimeout            time.Duration
	respTimeout           time.Duration
}

func defaultOptions() options {
	// TODO: verify defaults
	return options{
		codec:        DefaultCodec{},
		errorHandler: func(err error) { fmt.Fprintln(os.Stderr, err) },

		groupName: "_flowgroup",
		nodeKey:   nil, // will be set in 'apply'

		successorCount:  5,
		stabilizerCount: 5,

		stabilizationInterval: 10 * time.Second,
		ackTimeout:            750 * time.Millisecond,
		respTimeout:           1500 * time.Millisecond,
	}
}

func (o *options) apply(opts ...Option) (err error) {
	for _, opt := range opts {
		if err = opt(o); err != nil {
			return err
		}
	}

	if len(o.nodeKey) == 0 {
		if o.nodeKey, err = defaultNodeKey(); err != nil {
			return err
		}
	}
	return nil
}

// MessageCodec assigns the desired message codec to the broker. The codec
// is used to encode and decode messages to and from binary data. If no
// message codec is assigned, an internal binary format will be used instead.
func MessageCodec(c Codec) Option {
	return func(o *options) error {
		if c == nil {
			return optionError("no message codec specified")
		}
		o.codec = c
		return nil
	}
}

// ErrorHandler uses the given handler function to report errors, which
// occur concurrently (e.g. when receiving an invalid subscribed message).
// If no error handler is assigned, these errors will be logged to stderr.
func ErrorHandler(f func(error)) Option {
	return func(o *options) error {
		if f == nil {
			return optionError("no error handler specified")
		}
		o.errorHandler = f
		return nil
	}
}

// Group assigns the broker to a group with the given name.
// If no group is assigned to a broker, a global default group
// will be used instead.
func Group(name string) Option {
	return func(o *options) error {
		if name == "" {
			return optionError("empty group name")
		}
		o.groupName = name
		return nil
	}
}

// NodeKey assigns a key to the broker. This key is used for
// message partitioning and should therefore be unique within
// the assigned group.
func NodeKey(k Key) Option {
	return func(o *options) error {
		o.nodeKey = alloc(KeySize, o.nodeKey)
		copy(o.nodeKey, k[:])
		return nil
	}
}

// Successors defines the number of successor nodes which should be used
// to spread information about the local group structure.
func Successors(n int) Option {
	return func(o *options) error {
		if n <= 0 {
			return optionError("non-positive successor count")
		}
		o.successorCount = n
		return nil
	}
}

// Stabilizers defines the number of nodes which should be pinged
// in the stabilization process.
func Stabilizers(n int) Option {
	return func(o *options) error {
		if n <= 0 {
			return optionError("non-positive stabilizer count")
		}
		o.stabilizerCount = n
		return nil
	}
}

// StabilizationInterval defines the duration for the stabilization interval.
func StabilizationInterval(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return optionError("non-positive stabilization interval")
		}
		o.stabilizationInterval = d
		return nil
	}
}

// AckTimeout defines the timeout for acknowledging an internally
// forwarded message.
func AckTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return optionError("non-positive ack timeout")
		}
		o.ackTimeout = d
		return nil
	}
}

// ResponseTimeout defines the timeout for receiving a response
// of a request.
func ResponseTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return optionError("non-positive response timeout")
		}
		o.respTimeout = d
		return nil
	}
}

func defaultNodeKey() (key, error) {
	k := alloc(KeySize, nil)
	_, err := rand.Read(k)
	return k, err
}
