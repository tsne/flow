package flow

import "sync"

type routingTable struct {
	local           key // immutable
	successorCount  int // immutable
	stabilizerCount int // immutable

	mtx     sync.RWMutex
	keys    ring
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
	if !k.equal(r.local) {
		r.mtx.Lock()
		r.removeKey(k)
		r.mtx.Unlock()
	}
}

func (r *routingTable) suspect(k key) {
	// TODO: For now, we simply unregister the address.
	// As an improvement, the address could be stored
	// for a retry in some point of the future.
	r.unregister(k)
}

// returns predecessor + successors + local
func (r *routingTable) neighbors(buf keys) keys {
	neighborCount := r.successorCount + 1

	var size int
	r.mtx.RLock()
	if nkeys := r.keys.length(); neighborCount > nkeys {
		buf = makeKeys(nkeys+1, buf)
		size = copy(buf, r.keys)
	} else {
		idx := r.succIdx - 1
		if idx < 0 {
			idx = nkeys - 1
		}
		buf = makeKeys(neighborCount+1, buf)
		p := buf.slice(0, neighborCount)
		size = copy(p, r.keys.slice(idx, nkeys))
		size += copy(p[size:], r.keys)
	}
	r.mtx.RUnlock()

	copy(buf[size:], r.local)
	return buf
}

// returns succcessor and stabilizers
func (r *routingTable) stabilizers(buf keys) keys {
	stabilizerCount := 1 + r.stabilizerCount

	r.mtx.RLock()
	if nkeys := r.keys.length(); stabilizerCount > nkeys {
		buf = makeKeys(nkeys, buf)
		copy(buf, r.keys)
	} else {
		r.stabIdx %= nkeys
		buf = makeKeys(stabilizerCount, buf)
		copy(buf, r.keys.at(r.succIdx))
		n := copy(buf[KeySize:], r.keys.slice(r.stabIdx, nkeys))
		copy(buf[KeySize+n:], r.keys)
		r.stabIdx += r.stabilizerCount
	}

	r.mtx.RUnlock()
	return buf
}

func (r *routingTable) successor(k key, buf key) key {
	buf = buf[:0]

	r.mtx.RLock()
	if idx := r.keys.successor(k); idx >= 0 {
		prevIdx := idx - 1
		if prevIdx < 0 {
			prevIdx = r.keys.length() - 1
		}

		curr := r.keys.at(idx)
		prev := r.keys.at(prevIdx)
		if (prevIdx != idx && !r.local.between(prev, curr)) || !k.between(prev, r.local) {
			buf = curr.clone(buf)
		}
	}
	r.mtx.RUnlock()
	return buf
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
