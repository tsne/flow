package flow

import "testing"

func TestNewRoutingTable(t *testing.T) {
	opts := options{
		nodeKey:         intKey(7),
		successorCount:  7,
		stabilizerCount: 8,
	}
	r := newRoutingTable(opts)

	switch {
	case !r.local.equal(opts.nodeKey):
		t.Fatalf("unexpected local key: %s", r.local)
	case r.successorCount != opts.successorCount:
		t.Fatalf("unexpected successor count: %d", r.successorCount)
	case r.stabilizerCount != opts.stabilizerCount:
		t.Fatalf("unexpected stabilizer count: %d", r.stabilizerCount)
	}
}

func TestRoutingTableRegister(t *testing.T) {
	keybuf := intKeys(3, 5, 7, 9)
	key3 := keybuf.at(0)
	key5 := keybuf.at(1)
	key7 := keybuf.at(2)
	key9 := keybuf.at(3)

	r := routingTable{local: key7}

	r.register(keys(key7))
	switch {
	case r.keys.length() != 0:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	}

	r.register(keys(key5))
	switch {
	case r.keys.length() != 1:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key5):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.register(keys(key3))
	switch {
	case r.keys.length() != 2:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key3) || !r.keys.at(1).equal(key5):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.register(keys(key9))
	switch {
	case r.keys.length() != 3:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key3) || !r.keys.at(1).equal(key5) || !r.keys.at(2).equal(key9):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 2:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.register(keys(key5))
	switch {
	case r.keys.length() != 3:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key3) || !r.keys.at(1).equal(key5) || !r.keys.at(2).equal(key9):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 2:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}
}

func TestRoutingTableUnregister(t *testing.T) {
	keybuf := intKeys(3, 5, 7, 9)
	key3 := keybuf.at(0)
	key5 := keybuf.at(1)
	key7 := keybuf.at(2)
	key9 := keybuf.at(3)

	r := routingTable{local: key7}
	r.register(keybuf)

	r.unregister(key7)
	switch {
	case r.keys.length() != 3:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key3) || !r.keys.at(1).equal(key5) || !r.keys.at(2).equal(key9):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 2:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.unregister(key5)
	switch {
	case r.keys.length() != 2:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key3) || !r.keys.at(1).equal(key9):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 1:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.unregister(key9)
	switch {
	case r.keys.length() != 1:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key3):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.unregister(key3)
	if r.keys.length() != 0 {
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	}
}

func TestRoutingTableSuspect(t *testing.T) {
	keybuf := intKeys(5, 7)
	key5 := keybuf.at(0)
	key7 := keybuf.at(1)

	r := routingTable{local: key7}
	r.register(keybuf)

	r.suspect(key7)
	switch {
	case r.keys.length() != 1:
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	case !r.keys.at(0).equal(key5):
		t.Fatalf("unexpected keys: %v", printableKeys(r.keys))
	case r.succIdx != 0:
		t.Fatalf("unexpected successor index: %d", r.succIdx)
	}

	r.suspect(key5)
	if r.keys.length() != 0 {
		t.Fatalf("unexpected number of keys: %d", r.keys.length())
	}
}

func TestRoutingTableNeighbors(t *testing.T) {
	keybuf := intKeys(1, 3, 5, 7, 9, 11, 13)
	key3 := keybuf.at(1)
	key5 := keybuf.at(2)
	key7 := keybuf.at(3)
	key9 := keybuf.at(4)
	key11 := keybuf.at(5)

	r := routingTable{
		local:          key7,
		successorCount: 2,
	}
	neighbors := r.neighbors()
	switch {
	case neighbors.length() != 1:
		t.Fatalf("unexpected number of neighbors: %d", neighbors.length())
	case !neighbors.at(0).equal(r.local):
		t.Fatalf("unexpected neighbors: %v", printableKeys(neighbors))
	}

	r.register(keys(key3))
	neighbors = r.neighbors()
	switch {
	case neighbors.length() != 2:
		t.Fatalf("unexpected number of neighbors: %d", neighbors.length())
	case !neighbors.at(0).equal(key3) || !neighbors.at(1).equal(r.local):
		t.Fatalf("unexpected neighbors: %v", printableKeys(neighbors))
	}

	r.register(keybuf)
	neighbors = r.neighbors()
	switch {
	case neighbors.length() != 4:
		t.Fatalf("unexpected number of neighbors: %d", neighbors.length())
	case !neighbors.at(0).equal(key5) || !neighbors.at(1).equal(key9) || !neighbors.at(2).equal(key11) || !neighbors.at(3).equal(r.local):
		t.Fatalf("unexpected neighbors: %v", printableKeys(neighbors))
	}
}

func TestRoutingTableStabilizers(t *testing.T) {
	keys := intKeys(1, 3, 5, 9)
	r := routingTable{
		keys:            ring(keys.at(0)),
		stabilizerCount: 5,
	}

	stabilizers := r.stabilizers()
	switch {
	case stabilizers.length() != 1:
		t.Fatalf("unexpected number of stabilizers: %d", stabilizers.length())
	case !stabilizers.at(0).equal(keys.at(0)):
		t.Fatalf("unexpected stabilizers: %v", printableKeys(stabilizers))
	}

	r = routingTable{
		keys:            ring(keys),
		stabilizerCount: 2,
	}

	stabilizers = r.stabilizers()
	switch {
	case stabilizers.length() != 3:
		t.Fatalf("unexpected number of stabilizers: %d", stabilizers.length())
	case !stabilizers.at(0).equal(r.keys.at(r.succIdx)) || !stabilizers.at(1).equal(keys.at(0)) || !stabilizers.at(2).equal(keys.at(1)):
		t.Fatalf("unexpected stabilizers: %v", printableKeys(stabilizers))
	case r.stabIdx != 2:
		t.Fatalf("unexpected stabilizer index: %d", r.stabIdx)
	}

	stabilizers = r.stabilizers()
	switch {
	case stabilizers.length() != 3:
		t.Fatalf("unexpected number of stabilizers: %d", stabilizers.length())
	case !stabilizers.at(0).equal(r.keys.at(r.succIdx)) || !stabilizers.at(1).equal(keys.at(2)) || !stabilizers.at(2).equal(keys.at(3)):
		t.Fatalf("unexpected stabilizers: %v", printableKeys(stabilizers))
	case r.stabIdx != 0:
		t.Fatalf("unexpected stabilizer index: %d", r.stabIdx)
	}
}

func TestRoutingTableSuccessor(t *testing.T) {
	key5 := intKey(5)

	r := routingTable{}
	succ := r.successor(key5)
	if len(succ) != 0 {
		t.Fatalf("unexpected successor: %s", succ.String())
	}

	r = routingTable{
		local: key5,
		keys:  ring(intKeys(1, 3, 7, 9)),
	}
	succ = r.successor(r.keys.at(0))
	switch {
	case len(succ) == 0:
		t.Fatal("non-local successor expected")
	case !succ.equal(r.keys.at(0)):
		t.Fatalf("unexpected successor: %s", succ.String())
	}
}
