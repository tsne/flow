package flow

import "sync"

type routingTable struct {
	local           Key // immutable
	successorCount  int // immutable
	stabilizerCount int // immutable

	mtx     sync.RWMutex
	keys    keyRing
	succIdx int
	stabIdx int // index of the next stabilizer
}

func newRoutingTable(opts options) routingTable {
	return routingTable{
		local:           opts.nodeKey,
		successorCount:  opts.stabilization.Successors,
		stabilizerCount: opts.stabilization.Stabilizers,
	}
}

func (r *routingTable) registerKey(k Key) {
	r.mtx.Lock()
	r.addKey(k)
	r.mtx.Unlock()
}

func (r *routingTable) registerKeys(keys keys) {
	n := keys.length()

	r.mtx.Lock()
	r.keys.reserve(len(r.keys) + n)
	for i := 0; i < n; i++ {
		r.addKey(keys.at(i))
	}
	r.mtx.Unlock()
}

func (r *routingTable) unregister(k Key) {
	if k != r.local {
		r.mtx.Lock()
		r.removeKey(k)
		r.mtx.Unlock()
	}
}

func (r *routingTable) suspect(k Key) {
	// TODO: For now, we simply unregister the address.
	// As an improvement, the address could be stored
	// for a retry in some point of the future.
	r.unregister(k)
}

// returns predecessor + successors + local
func (r *routingTable) neighbors() (keys keys) {
	neighborCount := r.successorCount + 1

	r.mtx.RLock()
	if nkeys := len(r.keys); neighborCount > nkeys {
		keys = makeKeys(nkeys + 1)
		for i := 0; i < nkeys; i++ {
			keys.set(i, r.keys[i])
		}
	} else {
		idx := r.succIdx - 1
		if idx < 0 {
			idx = nkeys - 1
		}
		keys = makeKeys(neighborCount + 1)
		for i := 0; i < neighborCount; i++ {
			keys.set(i, r.keys[(idx+i)%nkeys])
		}
	}
	r.mtx.RUnlock()

	keys.set(keys.length()-1, r.local)
	return keys
}

// returns succcessor and stabilizers
func (r *routingTable) stabilizers(stabs []Key) (n int) {
	r.mtx.RLock()
	if nkeys := len(r.keys); len(stabs) > nkeys {
		n = copy(stabs, r.keys)
	} else {
		r.stabIdx %= nkeys
		stabs[0] = r.keys[r.succIdx]
		n = 1
		n += copy(stabs[n:], r.keys[r.stabIdx:])
		n += copy(stabs[n:], r.keys)
		r.stabIdx += r.stabilizerCount
	}
	r.mtx.RUnlock()
	return n
}

func (r *routingTable) successor(k Key) Key {
	succ := r.local

	r.mtx.RLock()
	if idx := r.keys.successor(k); idx >= 0 {
		prevIdx := idx - 1
		if prevIdx < 0 {
			prevIdx = len(r.keys) - 1
		}

		curr := r.keys[idx]
		prev := r.keys[prevIdx]
		if (prevIdx != idx && !r.local.between(prev, curr)) || !k.between(prev, r.local) {
			succ = curr
		}
	}
	r.mtx.RUnlock()
	return succ
}

// The following functions have unprotected access.
// These functions should only be used within the
// routing table.

func (r *routingTable) addKey(k Key) {
	if k == r.local {
		return
	}

	if len(r.keys) == 0 {
		r.succIdx = r.keys.add(k)
	} else {
		updateSucc := k.between(r.local, r.keys[r.succIdx])
		idx := r.keys.add(k)
		if updateSucc && idx >= 0 {
			r.succIdx = idx
		}
	}
}

func (r *routingTable) removeKey(k Key) {
	if idx := r.keys.remove(k); idx < r.succIdx {
		r.succIdx--
	} else if idx == r.succIdx && r.succIdx == len(r.keys) {
		r.succIdx = 0
	}
}
