package flow

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestKeyString(t *testing.T) {
	k := intKey(7)
	if s := k.String(); s != "0000000000000000000000000000000000000007" {
		t.Fatalf("unexpected key representation: %s", s)
	}

	key := Key(k.array())
	if s := key.String(); s != "0000000000000000000000000000000000000007" {
		t.Fatalf("unexpected key representation: %s", s)
	}
}

func TestKeyFromBytes(t *testing.T) {
	_, err := keyFromBytes(nil)
	if err != errMalformedKey {
		t.Fatalf("unexpected error: %v", err)
	}

	key7 := intKey(7)
	k, err := keyFromBytes(key7)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !k.equal(key7):
		t.Fatalf("unexpected key: %s", k.String())
	}
}

func TestKeyEqual(t *testing.T) {
	keys := intKeys(1, 2)
	k1 := keys.at(0)
	k2 := keys.at(1)
	switch {
	case !k1.equal(k1):
		t.Fatalf("expected to be equal: %s and %s", k1, k1)
	case k1.equal(k2):
		t.Fatalf("expected to be inequal: %s and %s", k1, k2)
	}
}

func TestKeyClone(t *testing.T) {
	k1 := intKey(1)
	k2 := k1.clone(nil)
	switch {
	case &k1[0] == &k2[0]:
		t.Fatalf("expected to be different: %p and %p", k1, k2)
	case !k1.equal(k2):
		t.Fatalf("expected to be equal: %s and %s", k1.String(), k2.String())
	}
}

func TestKeyBetween(t *testing.T) {
	keys := intKeys(0x1, 0x2, 0x3, 0x100, 0x200, 0x300, 0x199, 0x255, 0x311)

	k1 := keys.at(0)
	k2 := keys.at(1)
	k3 := keys.at(2)
	switch {
	case k1.between(k2, k3):
		t.Fatalf("%s in (%s,%s]", k1.String(), k2.String(), k3.String())
	case k3.between(k1, k2):
		t.Fatalf("%s in (%s,%s]", k3.String(), k1.String(), k2.String())
	case k2.between(k3, k1):
		t.Fatalf("%s in (%s,%s]", k2.String(), k3.String(), k1.String())
	case !k3.between(k2, k1):
		t.Fatalf("%s not in (%s,%s]", k3.String(), k2.String(), k1.String())
	case !k1.between(k3, k2):
		t.Fatalf("%s not in (%s,%s]", k3.String(), k2.String(), k1.String())
	case !k2.between(k1, k3):
		t.Fatalf("%s not in (%s,%s]", k2.String(), k1.String(), k3.String())
	}

	k1 = keys.at(3)
	k2 = keys.at(4)
	k3 = keys.at(5)
	switch {
	case k1.between(k2, k3):
		t.Fatalf("%s in (%s,%s]", k1.String(), k2.String(), k3.String())
	case k3.between(k1, k2):
		t.Fatalf("%s in (%s,%s]", k3.String(), k1.String(), k2.String())
	case k2.between(k3, k1):
		t.Fatalf("%s in (%s,%s]", k2.String(), k3.String(), k1.String())
	case !k3.between(k2, k1):
		t.Fatalf("%s not in (%s,%s]", k3.String(), k2.String(), k1.String())
	case !k1.between(k3, k2):
		t.Fatalf("%s not in (%s,%s]", k3.String(), k2.String(), k1.String())
	case !k2.between(k1, k3):
		t.Fatalf("%s not in (%s,%s]", k2.String(), k1.String(), k3.String())
	}

	k1 = keys.at(6)
	k2 = keys.at(7)
	k3 = keys.at(8)
	switch {
	case k1.between(k2, k3):
		t.Fatalf("%s in (%s,%s]", k1.String(), k2.String(), k3.String())
	case k3.between(k1, k2):
		t.Fatalf("%s in (%s,%s]", k3.String(), k1.String(), k2.String())
	case k2.between(k3, k1):
		t.Fatalf("%s in (%s,%s]", k2.String(), k3.String(), k1.String())
	case !k3.between(k2, k1):
		t.Fatalf("%s not in (%s,%s]", k3.String(), k2.String(), k1.String())
	case !k1.between(k3, k2):
		t.Fatalf("%s not in (%s,%s]", k3.String(), k2.String(), k1.String())
	case !k2.between(k1, k3):
		t.Fatalf("%s not in (%s,%s]", k2.String(), k1.String(), k3.String())
	}
}

func TestMakeKeys(t *testing.T) {
	keys := makeKeys(7, nil)
	if len(keys) != 7*KeySize {
		t.Fatalf("unexpected length: %d", len(keys))
	}
}

func TestKeysFromBytes(t *testing.T) {
	_, err := keysFromBytes([]byte{1, 2, 3})
	if err != errMalformedKeys {
		t.Fatalf("unexpected error: %v", err)
	}

	k, err := keysFromBytes(nil)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(k) != 0:
		t.Fatalf("unexpected length: %d", len(k))
	}

	keys := intKeys(1, 2, 3)
	k, err = keysFromBytes(keys)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !bytes.Equal(k, keys):
		t.Fatalf("unexpected keys: %v", printableKeys(k))
	}
}

func TestKeysLength(t *testing.T) {
	var keys keys
	if keys.length() != 0 {
		t.Fatalf("unexpected length: %d", keys.length())
	}

	keys = intKeys(1, 2, 3)
	if keys.length() != 3 {
		t.Fatalf("unexpected length: %d", keys.length())
	}
}

func TestKeysAt(t *testing.T) {
	keys := intKeys(1, 2, 3)
	for i := 0; i < 3; i++ {
		if k := keys.at(i); !k.equal(intKey(i + 1)) {
			t.Fatalf("unexpected key at %d: %s", i, k.String())
		}
	}
}

func TestKeysSlice(t *testing.T) {
	keys := intKeys(1, 2, 3, 4, 5, 6, 7)
	sliced := keys.slice(2, 5)
	if !bytes.Equal(sliced, keys[2*KeySize:5*KeySize]) {
		t.Fatalf("unexpected slice: %v", printableKeys(sliced))
	}
}

func TestRingLength(t *testing.T) {
	var r ring
	if r.length() != 0 {
		t.Fatalf("unexpected length: %d", r.length())
	}

	r = ring(intKeys(1, 2, 3))
	if r.length() != 3 {
		t.Fatalf("unexpected length: %d", r.length())
	}
}

func TestRingAt(t *testing.T) {
	r := ring(intKeys(1, 2, 3))
	for i := 0; i < 3; i++ {
		if k := r.at(i); !k.equal(intKey(i + 1)) {
			t.Fatalf("unexpected key at %d: %s", i, k.String())
		}
	}
}

func TestRingSlice(t *testing.T) {
	r := ring(intKeys(1, 2, 3, 4, 5, 6, 7))
	sliced := r.slice(2, 5)
	if !bytes.Equal(sliced, r[2*KeySize:5*KeySize]) {
		t.Fatalf("unexpected slice: %v", printableKeys(sliced))
	}
}

func TestRingReserve(t *testing.T) {
	r := ring(intKeys(1, 2, 3))
	r.reserve(2)
	switch {
	case r.length() != 3:
		t.Fatalf("unexpected length: %d", r.length())
	case cap(r) != 3*KeySize:
		t.Fatalf("unexpected capacity: %d", cap(r))
	}

	r.reserve(3)
	switch {
	case r.length() != 3:
		t.Fatalf("unexpected length: %d", r.length())
	case cap(r) != 3*KeySize:
		t.Fatalf("unexpected capacity: %d", cap(r))
	}

	r.reserve(12)
	switch {
	case r.length() != 3:
		t.Fatalf("unexpected length: %d", r.length())
	case cap(r) != 12*KeySize:
		t.Fatalf("unexpected capacity: %d", cap(r))
	}
}

func TestRingAdd(t *testing.T) {
	keys := intKeys(1, 2, 3, 4)

	var r ring
	idx := r.add(keys.at(0))
	switch {
	case idx != 0:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, keys.at(0)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.add(keys.at(2))
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case !r.at(0).equal(keys.at(0)) || !r.at(1).equal(keys.at(2)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.add(keys.at(1))
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, keys.slice(0, 3)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.add(keys.at(3))
	switch {
	case idx != 3:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, keys):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.add(keys.at(0))
	switch {
	case idx >= 0:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, keys):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}
}

func TestRingRemove(t *testing.T) {
	r := ring(intKeys(1, 3, 4, 6))

	idx := r.remove(intKey(5))
	switch {
	case idx >= 0:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, intKeys(1, 3, 4, 6)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.remove(intKey(1))
	switch {
	case idx != 0:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, intKeys(3, 4, 6)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.remove(intKey(4))
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, intKeys(3, 6)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.remove(intKey(6))
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, intKeys(3)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.remove(intKey(9))
	switch {
	case idx >= 0:
		t.Fatalf("unexpected index: %d", idx)
	case !bytes.Equal(r, intKeys(3)):
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}

	idx = r.remove(intKey(3))
	switch {
	case idx != 0:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 0:
		t.Fatalf("unexpected ring: %v", printableKeys(r))
	}
}

func TestRingSuccessor(t *testing.T) {
	keys := intKeys(3, 5, 7)

	var r ring
	idx := r.successor(keys.at(0))
	if idx >= 0 {
		t.Fatalf("unexpected index: %d", idx)
	}

	r = ring(keys)
	for i := 0; i < keys.length(); i++ {
		idx = r.successor(keys.at(i))
		if idx != i {
			t.Fatalf("unexpected index: %d", idx)
		}
	}

	k := intKeys(2, 4, 6)
	for i := 0; i < k.length(); i++ {
		idx = r.successor(k.at(i))
		if idx != i {
			t.Fatalf("unexpected index: %d", idx)
		}
	}

	idx = r.successor(intKey(9))
	if idx != 0 {
		t.Fatalf("unexpected index: %d", idx)
	}
}

func intKey(n int) key {
	return key(intKeys(n))
}

func intKeys(ns ...int) keys {
	k := makeKeys(len(ns), nil)
	for i := 0; i < len(ns); i++ {
		binary.BigEndian.PutUint64(k.at(i)[KeySize-8:], uint64(ns[i]))
	}
	return k
}

type printableKeys keys

func (k printableKeys) String() string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	keys := keys(k)
	if nkeys := keys.length(); nkeys != 0 {
		for i := 0; i < nkeys; i++ {
			buf.WriteString(keys.at(i).String())
			buf.WriteString(", ")
		}
		buf.Truncate(buf.Len() - 2) // trim last comma + whitespace
	}
	buf.WriteByte(']')
	return buf.String()
}

func (k printableKeys) GoString() string {
	return k.String()
}
