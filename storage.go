package flow

// Store defines an interface for the storage system. It is used
// to store single messages.
type Store interface {
	Store(msg Message) error
}

type nullStore struct{}

func (s nullStore) Store(msg Message) error {
	return nil
}

type storage struct {
	store  Store
	filter func(stream string) bool
}

func newStorage(opts options) storage {
	return storage{
		store:  opts.store,
		filter: opts.storeFilter,
	}
}

func (s *storage) persist(msg Message) error {
	if !s.filter(msg.Stream) {
		return nil
	}
	return s.store.Store(msg)
}
