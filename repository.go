package flow

type repository struct {
	store  Store
	filter func(stream string) bool
}

func newRepository(opts options) repository {
	return repository{
		store:  opts.store,
		filter: opts.storeFilter,
	}
}

func (r *repository) persist(msg Message) error {
	if !r.filter(msg.Stream) {
		return nil
	}
	return r.store.Store(&msg)
}
