package flow

import (
	"crypto/rand"
	"time"
)

// Option represents an option which can be used to configure
// a broker.
type Option func(*options) error

type options struct {
	codec Codec

	groupName string
	nodeKey   key

	store       Store
	storeFilter func(stream string) bool

	successorCount  int
	stabilizerCount int

	stabilizationInterval time.Duration
	ackTimeout            time.Duration
}

func defaultOptions() options {
	// TODO: verify defaults
	return options{
		codec: binaryCodec{},

		groupName: "_defaultgroup",
		nodeKey:   nil, // will be set in 'apply'

		store:       nullStore{},
		storeFilter: func(string) bool { return true },

		successorCount:  5,
		stabilizerCount: 5,

		stabilizationInterval: 10 * time.Second,
		ackTimeout:            500 * time.Millisecond,
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
		if len(o.nodeKey) != KeySize {
			o.nodeKey = alloc(KeySize, nil)
		}
		copy(o.nodeKey, k[:])
		return nil
	}
}

// Storage assigns a storage system to the broker. Each published
// message of this broker is stored using s as the storage engine.
func Storage(s Store) Option {
	return func(o *options) error {
		if s == nil {
			return optionError("no store specified")
		}
		o.store = s
		return nil
	}
}

// StorageFilter defines a filter function for the storage system.
// The given function decides which streams should be stored and which
// should not. If true is returned, the message will be stored.
// Otherwise the storage system is bypassed.
func StorageFilter(f func(stream string) bool) Option {
	return func(o *options) error {
		if f == nil {
			return optionError("no filter specified")
		}
		o.storeFilter = f
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

func defaultNodeKey() (key, error) {
	k := alloc(KeySize, nil)
	_, err := rand.Read(k)
	return k, err
}
