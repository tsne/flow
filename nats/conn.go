package nats

import (
	"strings"
	"unicode"

	nats "github.com/nats-io/go-nats"

	"github.com/tsne/flow"
)

// Conn represents a connection to a NATS server.
type Conn struct {
	*nats.Conn // reuse the Publish method
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
	c.Conn.Close()
	return nil
}

// Subscribe installs a handler for the specified stream. The handler shares
// the incoming message stream with other handlers of the same group. If
// the group is empty, a separate stream will be assigned to the handler.
func (c Conn) Subscribe(stream, group string, h flow.PubSubHandler) (flow.Subscription, error) {
	return c.Conn.QueueSubscribe(stream, group, func(msg *nats.Msg) {
		h(msg.Subject, msg.Data)
	})
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
