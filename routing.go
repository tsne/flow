package flow

import "sync"

type routingTable struct {
	// immutable fields
	local           key
	successorCount  int
	stabilizerCount int

	mtx     sync.RWMutex
	keys    ring
	succIdx int
	stabIdx int // index of the next stabilizer
}

func newRoutingTable(opts options) routingTable {
	return routingTable{
		local:           opts.nodeKey,
		successorCount:  opts.successorCount,
		stabilizerCount: opts.stabilizerCount,
	}
}

func (r *routingTable) register(keys keys) {
	n := keys.length()

	r.mtx.Lock()
	r.keys.reserve(r.keys.length() + n)
	for i := 0; i < n; i++ {
		r.addKey(keys.at(i))
	}
	r.mtx.Unlock()
}

func (r *routingTable) unregister(k key) {
	if k.equal(r.local) {
		return
	}
	r.mtx.Lock()
	r.removeKey(k)
	r.mtx.Unlock()
}

func (r *routingTable) suspect(k key) {
	// TODO: For now, we simply unregister the address.
	// As an improvement, the address could be stored
	// for a retry in some point of the future.
	r.unregister(k)
}

// returns predecessor + successors + local
func (r *routingTable) neighbors() keys {
	neighborCount := r.successorCount + 1

	var (
		res  keys
		size int
	)
	r.mtx.RLock()
	if nkeys := r.keys.length(); neighborCount > nkeys {
		res = makeKeys(nkeys + 1)
		size = copy(res, r.keys)
	} else {
		idx := r.succIdx - 1
		if idx < 0 {
			idx = nkeys - 1
		}
		res = makeKeys(neighborCount + 1)
		p := res.slice(0, neighborCount)
		size = copy(p, r.keys.slice(idx, nkeys))
		size += copy(p[size:], r.keys)
	}
	r.mtx.RUnlock()

	copy(res[size:], r.local)
	return res
}

// returns succcessor and stabilizers
func (r *routingTable) stabilizers() keys {
	stabilizerCount := 1 + r.stabilizerCount

	var res keys
	r.mtx.RLock()
	if nkeys := r.keys.length(); stabilizerCount > nkeys {
		res = makeKeys(nkeys)
		copy(res, r.keys)
	} else {
		res = makeKeys(stabilizerCount)
		copy(res, r.keys.at(r.succIdx))
		n := copy(res[KeySize:], r.keys.slice(r.stabIdx, nkeys))
		copy(res[KeySize+n:], r.keys)
		r.stabIdx = (r.stabIdx + r.stabilizerCount) % nkeys
	}

	r.mtx.RUnlock()
	return res
}

func (r *routingTable) successor(k key) (succ key) {
	r.mtx.RLock()
	if idx := r.keys.successor(k); idx >= 0 {
		prevIdx := idx - 1
		if prevIdx < 0 {
			prevIdx = r.keys.length() - 1
		}

		curr := r.keys.at(idx)
		prev := r.keys.at(prevIdx)
		if (prevIdx != idx && !r.local.between(prev, curr)) || !k.between(prev, r.local) {
			succ = curr.clone()
		}
	}
	r.mtx.RUnlock()
	return
}

// The following functions have unprotected access.
// These functions should only be used within the
// routing table.

func (r *routingTable) addKey(k key) {
	if k.equal(r.local) {
		return
	}

	if r.keys.length() == 0 {
		r.succIdx = r.keys.add(k)
	} else {
		updateSucc := k.between(r.local, r.keys.at(r.succIdx))
		idx := r.keys.add(k)
		if updateSucc && idx >= 0 {
			r.succIdx = idx
		}
	}
}

func (r *routingTable) removeKey(k key) {
	if idx := r.keys.remove(k); idx < r.succIdx {
		r.succIdx--
	} else if idx == r.succIdx && r.succIdx == r.keys.length() {
		r.succIdx = 0
	}
}
