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
