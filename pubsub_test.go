package flow

import (
	"sync"
)

// pubsub stub

type subscription struct {
	h           PubSubHandler
	unsubscribe func() error
}

func (s *subscription) Unsubscribe() error {
	return s.unsubscribe()
}

type pubsubRecorder struct {
	subMtx sync.Mutex
	subs   map[string][]*subscription // stream => subscriptions

}

func newPubsubRecorder() *pubsubRecorder {
	return &pubsubRecorder{
		subs: make(map[string][]*subscription),
	}
}

func (r *pubsubRecorder) Publish(stream string, data []byte) error {
	r.subMtx.Lock()
	subs := r.subs[stream]
	r.subMtx.Unlock()

	for _, sub := range subs {
		sub.h(stream, data)
	}
	return nil
}

func (r *pubsubRecorder) Subscribe(stream, group string, h PubSubHandler) (Subscription, error) {
	sub := &subscription{h: h}
	sub.unsubscribe = func() error { return r.unsubscribe(stream, sub) }

	r.subMtx.Lock()
	r.subs[stream] = append(r.subs[stream], sub)
	r.subMtx.Unlock()
	return sub, nil
}

func (r *pubsubRecorder) SubscribeChan(stream string) (<-chan frame, Subscription) {
	ch := make(chan frame)
	sub, _ := r.Subscribe(stream, "", func(_ string, data []byte) {
		ch <- data
	})
	return ch, sub
}

func (r *pubsubRecorder) unsubscribe(stream string, sub *subscription) error {
	r.subMtx.Lock()
	defer r.subMtx.Unlock()

	subs := r.subs[stream]
	for i, s := range subs {
		if s == sub {
			subs = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	r.subs[stream] = subs
	return nil
}
