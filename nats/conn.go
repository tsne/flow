package nats

import (
	"context"
	"strings"
	"sync"
	"unicode"

	nats "github.com/nats-io/go-nats"

	"github.com/tsne/flow"
)

// Conn represents a connection to a NATS server.
type Conn struct {
	*nats.Conn
}

// Connect connects to the NATS servers with the given addresses.
// Address formats follow the NATS convention. For multiple addresses
// addr should contain a comma separated list.
func Connect(addr string, opts ...Option) (Conn, error) {
	o := nats.GetDefaultOptions()
	o.Servers = splitAddresses(addr)

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return Conn{}, err
		}
	}

	conn, err := o.Connect()
	return Conn{conn}, err
}

// Close closes the connection.
func (c Conn) Close() error {
	var wg sync.WaitGroup
	wg.Add(1)
	c.Conn.SetClosedHandler(func(_ *nats.Conn) { wg.Done() })

	if err := c.Conn.Drain(); err != nil {
		return err
	}

	wg.Wait()
	return nil
}

// Publish publishes data to the specified stream.
func (c Conn) Publish(ctx context.Context, stream string, data []byte) error {
	return c.Conn.Publish(stream, data)
}

// Subscribe installs a handler for the specified stream. The handler shares
// the incoming message stream with other handlers of the same group. If
// the group is empty, a separate stream will be assigned to the handler.
func (c Conn) Subscribe(ctx context.Context, stream, group string, h flow.PubSubHandler) (flow.Subscription, error) {
	handlerCtx := context.Background()
	sub, err := c.Conn.QueueSubscribe(stream, group, func(msg *nats.Msg) {
		h(handlerCtx, msg.Subject, msg.Data)
	})
	return subscription{sub}, err
}

type subscription struct {
	*nats.Subscription
}

func (s subscription) Unsubscribe(ctx context.Context) error {
	return s.Subscription.Drain()
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
