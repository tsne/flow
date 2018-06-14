package flow

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
