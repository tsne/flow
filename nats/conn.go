package nats

import (
	"fmt"
	"sync"

	"github.com/tsne/flow"

	"github.com/nats-io/nats"
)

type Conn struct {
	*nats.Conn // reuse the Publish method

	mtx  sync.Mutex
	subs map[string]*nats.Subscription // stream => subscription
}

func Connect(addr string) (*Conn, error) {
	conn, err := nats.Connect(addr)
	if err != nil {
		return nil, err
	}
	return &Conn{
		Conn: conn,
		subs: make(map[string]*nats.Subscription),
	}, nil
}

func (c *Conn) Close() error {
	c.Conn.Close()
	return nil
}

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
