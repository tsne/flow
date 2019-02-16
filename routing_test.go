package flow

import "testing"

func TestNewRoutingTable(t *testing.T) {
	opts := options{
		nodeKey: 7,
		stabilization: Stabilization{
			Successors:  13,
			Stabilizers: 14,
		},
	}
	r := newRoutingTable(opts)

	switch {
	case r.local != opts.nodeKey:
		t.Fatalf("unexpected local key: %s", r.local)
	case r.successorCount != opts.stabilization.Successors:
		t.Fatalf("unexpected successor count: %d", r.successorCount)
	case r.stabilizerCount != opts.stabilization.Stabilizers:
		t.Fatalf("unexpected stabilizer count: %d", r.stabilizerCount)
	}
}

func TestRoutingTableRegister(t *testing.T) {
	r := routingTable{local: 7}

	r.registerKey(7)
	switch {
	case len(r.keys) != 0:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	}

	r.registerKey(5)
	switch {
	case len(r.keys) != 1:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 5:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.registerKey(3)
	switch {
	case len(r.keys) != 2:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 3 || r.keys[1] != 5:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.registerKey(9)
	switch {
	case len(r.keys) != 3:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 3 || r.keys[1] != 5 || r.keys[2] != 9:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 2:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.registerKey(5)
	switch {
	case len(r.keys) != 3:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 3 || r.keys[1] != 5 || r.keys[2] != 9:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 2:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}
}

func TestRoutingTableUnregister(t *testing.T) {
	r := routingTable{local: 7}
	r.registerKeys(keys{
		0, 0, 0, 0, 0, 0, 0, 3,
		0, 0, 0, 0, 0, 0, 0, 5,
		0, 0, 0, 0, 0, 0, 0, 7,
		0, 0, 0, 0, 0, 0, 0, 9,
	})

	r.unregister(7)
	switch {
	case len(r.keys) != 3:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 3 || r.keys[1] != 5 || r.keys[2] != 9:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 2:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.unregister(5)
	switch {
	case len(r.keys) != 2:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 3 || r.keys[1] != 9:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 1:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.unregister(9)
	switch {
	case len(r.keys) != 1:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 3:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.unregister(3)
	if len(r.keys) != 0 {
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	}
}

func TestRoutingTableSuspect(t *testing.T) {
	r := routingTable{local: 7}
	r.registerKeys(keys{
		0, 0, 0, 0, 0, 0, 0, 5,
		0, 0, 0, 0, 0, 0, 0, 7,
	})

	r.suspect(7)
	switch {
	case len(r.keys) != 1:
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	case r.keys[0] != 5:
		t.Fatalf("unexpected keys: %v", r.keys)
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.suspect(5)
	if len(r.keys) != 0 {
		t.Fatalf("unexpected number of keys: %d", len(r.keys))
	}
}

func TestRoutingTableNeighbors(t *testing.T) {
	r := routingTable{
		local:          7,
		successorCount: 2,
	}

	neighbors := r.neighbors()
	switch {
	case neighbors.length() != 1:
		t.Fatalf("unexpected number of neighbors: %d", neighbors.length())
	case neighbors.at(0) != r.local:
		t.Fatalf("unexpected neighbors: %v", printableKeys(neighbors))
	}

	r.registerKey(3)
	neighbors = r.neighbors()
	switch {
	case neighbors.length() != 2:
		t.Fatalf("unexpected number of neighbors: %d", neighbors.length())
	case neighbors.at(0) != 3 || neighbors.at(1) != r.local:
		t.Fatalf("unexpected neighbors: %v", printableKeys(neighbors))
	}

	r.registerKeys(keys{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 3,
		0, 0, 0, 0, 0, 0, 0, 5,
		0, 0, 0, 0, 0, 0, 0, 7,
		0, 0, 0, 0, 0, 0, 0, 9,
		0, 0, 0, 0, 0, 0, 0, 11,
		0, 0, 0, 0, 0, 0, 0, 13,
	})
	neighbors = r.neighbors()
	switch {
	case neighbors.length() != 4:
		t.Fatalf("unexpected number of neighbors: %d", neighbors.length())
	case neighbors.at(0) != 5 || neighbors.at(1) != 9 || neighbors.at(2) != 11 || neighbors.at(3) != r.local:
		t.Fatalf("unexpected neighbors: %v", printableKeys(neighbors))
	}
}

func TestRoutingTableStabilizers(t *testing.T) {
	r := routingTable{
		keys:            keyRing{1},
		stabilizerCount: 5,
	}

	stabilizers := make([]Key, 3)
	n := r.stabilizers(stabilizers)
	switch {
	case n != 1:
		t.Fatalf("unexpected number of stabilizers: %d", n)
	case stabilizers[0] != 1:
		t.Fatalf("unexpected stabilizers: %v", stabilizers)
	}

	r = routingTable{
		keys:            keyRing{1, 3, 5, 9},
		stabilizerCount: 2,
	}

	n = r.stabilizers(stabilizers)
	switch {
	case n != 3:
		t.Fatalf("unexpected number of stabilizers: %d", n)
	case stabilizers[0] != r.keys[r.succIdx] || stabilizers[1] != 1 || stabilizers[2] != 3:
		t.Fatalf("unexpected stabilizers: %v", stabilizers)
	case r.stabIdx != 2:
		t.Fatalf("unexpected stabilizer index: %d", r.stabIdx)
	}

	n = r.stabilizers(stabilizers)
	switch {
	case n != 3:
		t.Fatalf("unexpected number of stabilizers: %d", n)
	case stabilizers[0] != r.keys[r.succIdx] || stabilizers[1] != 5 || stabilizers[2] != 9:
		t.Fatalf("unexpected stabilizers: %v", stabilizers)
	case r.stabIdx != 4:
		t.Fatalf("unexpected stabilizer index: %d", r.stabIdx)
	}
}

func TestRoutingTableSuccessor(t *testing.T) {
	r := routingTable{}
	succ := r.successor(5)
	if succ != r.local {
		t.Fatalf("unexpected successor: %s", succ)
	}

	r = routingTable{
		local: 5,
		keys:  keyRing{1, 3, 7, 9},
	}
	succ = r.successor(r.keys[0])
	switch {
	case succ == r.local:
		t.Fatal("non-local successor expected")
	case succ != 1:
		t.Fatalf("unexpected successor: %s", succ)
	}
}
