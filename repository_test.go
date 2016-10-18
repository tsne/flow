package flow

import (
	"reflect"
	"testing"
	"time"
)

func TestNewRepository(t *testing.T) {
	store := newStoreRecorder()
	repo := newRepository(options{
		store:       store,
		storeFilter: func(string) bool { return false },
	})
	if s, ok := repo.store.(*storeRecorder); !ok || s != store {
		t.Fatalf("unexpected persistence store: %T", repo.store)
	}
	if repo.filter == nil {
		t.Fatal("expected filter to be set")
	}
}

func TestRepositoryPersist(t *testing.T) {
	store := newStoreRecorder()
	repo := repository{
		store:  store,
		filter: func(stream string) bool { return stream != "ignore" },
	}

	msg := Message{
		Stream:       "ignore",
		Source:       []byte("source id"),
		Time:         time.Date(1988, time.September, 26, 1, 0, 0, 0, time.UTC),
		PartitionKey: []byte("partition key"),
		Data:         []byte("data"),
	}

	err := repo.persist(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case store.countMessages() != 0:
		t.Fatalf("unexpected number of messages: %d", store.countMessages())
	}

	msg.Stream = "stream"
	err = repo.persist(msg)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case store.countMessages() != 1:
		t.Fatalf("unexpected number of messages: %d", store.countMessages())
	case !reflect.DeepEqual(*store.message(0), msg):
		t.Fatalf("unexpected message: %+v", store.message(0))
	}
}
