package flow

import (
	"reflect"
	"testing"
	"time"
)

func TestNewStorage(t *testing.T) {
	store := newStoreRecorder()
	storage := newStorage(options{
		store:       store,
		storeFilter: func(string) bool { return false },
	})
	if s, ok := storage.store.(*storeRecorder); !ok || s != store {
		t.Fatalf("unexpected persistence store: %T", storage.store)
	}
	if storage.filter == nil {
		t.Fatal("expected filter to be set")
	}
}

func TestStoragePersist(t *testing.T) {
	store := newStoreRecorder()
	storage := storage{
		store:  store,
		filter: func(stream string) bool { return stream != "ignore" },
	}

	msg := Message{
		Stream:       "ignore",
		Time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
		PartitionKey: []byte("partition key"),
		Data:         []byte("data"),
	}

	err := storage.persist(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case store.countMessages() != 0:
		t.Fatalf("unexpected number of messages: %d", store.countMessages())
	}

	msg.Stream = "stream"
	err = storage.persist(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case store.countMessages() != 1:
		t.Fatalf("unexpected number of messages: %d", store.countMessages())
	case !reflect.DeepEqual(*store.message(0), msg):
		t.Fatalf("unexpected message: %+v", store.message(0))
	}
}

type storeRecorder struct {
	messages []Message
}

func newStoreRecorder() *storeRecorder {
	return &storeRecorder{}
}

func (r *storeRecorder) Store(msg Message) error {
	r.messages = append(r.messages, msg)
	return nil
}

func (r *storeRecorder) countMessages() int {
	return len(r.messages)
}

func (r *storeRecorder) message(idx int) *Message {
	return &r.messages[idx]
}
