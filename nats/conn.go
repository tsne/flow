package nats

import (
	"fmt"
	"strings"
	"sync"
	"unicode"

	nats "github.com/nats-io/go-nats"

	"github.com/tsne/flow"
)

// Conn represents a connection to a NATS server.
type Conn struct {
	*nats.Conn // reuse the Publish method

	mtx  sync.Mutex
	subs map[string]*nats.Subscription // stream => subscription
}

// Connect connects to the NATS servers with the given addresses.
// Address formats follow the NATS convention. For multiple addresses
// addr should contain a comma separated list.
func Connect(addr string, opts ...Option) (*Conn, error) {
	o := nats.GetDefaultOptions()
	o.Servers = splitAddresses(addr)
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	conn, err := o.Connect()
	if err != nil {
		return nil, err
	}

	return &Conn{
		Conn: conn,
		subs: make(map[string]*nats.Subscription),
	}, nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	c.Conn.Close()
	return nil
}

// Subscribe installs a handler for the specfied stream. The handler shares
// the incoming message stream with other handlers of the same group. If
// there is already a handler for the given stream, an error will be returned.
func (c *Conn) Subscribe(stream, group string, h flow.PubSubHandler) error {
	sub, err := c.Conn.QueueSubscribe(stream, group, func(msg *nats.Msg) {
		h(msg.Subject, msg.Data)
	})
	if err != nil {
		return err
	}

	c.mtx.Lock()
	_, has := c.subs[stream]
	if !has {
		c.subs[stream] = sub
	}
	c.mtx.Unlock()

	if has {
		sub.Unsubscribe()
		return fmt.Errorf("already subscribed to stream '%s'", stream)
	}
	return nil
}

// Unsubscribe uninstalls the handler of the given stream.
func (c *Conn) Unsubscribe(stream string) error {
	c.mtx.Lock()
	sub := c.subs[stream]
	delete(c.subs, stream)
	c.mtx.Unlock()

	if sub == nil {
		return fmt.Errorf("not subscribed to stream '%s'", stream)
	}
	return sub.Unsubscribe()
}

func splitAddresses(a string) []string {
	const sep = ','

	a = strings.TrimFunc(a, func(ch rune) bool {
		return ch == rune(sep) || unicode.IsSpace(ch)
	})

	addrs := make([]string, 0, 1+strings.Count(a, string(sep)))
	for {
		idx := strings.IndexByte(a, sep)
		if idx < 0 {
			return append(addrs, strings.TrimSpace(a))
		}
		addrs = append(addrs, strings.TrimSpace(a[:idx]))
		a = a[idx+1:]
	}
}
