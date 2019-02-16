package flow

import (
	"bytes"
	"strings"
	"testing"
)

func TestKeyFromBytesAndString(t *testing.T) {
	k1 := KeyFromBytes([]byte("bytes"))
	k2 := KeyFromBytes([]byte("bytes"))
	if k1 != k2 {
		t.Fatalf("unexpected keys: %s and %s", k1, k2)
	}
}

func TestMarshalKey(t *testing.T) {
	var buf [keySize]byte
	marshalKey(7, buf[:])
	if !bytes.Equal(buf[:], []byte{0, 0, 0, 0, 0, 0, 0, 7}) {
		t.Fatalf("unexpected bytes: %+v", buf)
	}
}

func TestUnmarshalKey(t *testing.T) {
	_, err := unmarshalKey(nil)
	if err != errMalformedKey {
		t.Fatalf("unexpected error: %v", err)
	}

	k, err := unmarshalKey([]byte{0, 0, 0, 0, 0, 0, 0, 7})
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case k != 7:
		t.Fatalf("unexpected key: %s", k)
	}
}

func TestKeyString(t *testing.T) {
	k := Key(0x1234567890abcdef)
	if s := k.String(); s != "1234567890abcdef" {
		t.Fatalf("unexpected string representation: %q", s)
	}
}

func TestKeyBetween(t *testing.T) {
	var k1, k2, k3 Key

	k1, k2, k3 = 0x1, 0x2, 0x3
	switch {
	case k1.between(k2, k3):
		t.Fatalf("%s in (%s,%s]", k1, k2, k3)
	case k3.between(k1, k2):
		t.Fatalf("%s in (%s,%s]", k3, k1, k2)
	case k2.between(k3, k1):
		t.Fatalf("%s in (%s,%s]", k2, k3, k1)
	case !k3.between(k2, k1):
		t.Fatalf("%s not in (%s,%s]", k3, k2, k1)
	case !k1.between(k3, k2):
		t.Fatalf("%s not in (%s,%s]", k3, k2, k1)
	case !k2.between(k1, k3):
		t.Fatalf("%s not in (%s,%s]", k2, k1, k3)
	}

	k1, k2, k3 = 0x100, 0x200, 0x300
	switch {
	case k1.between(k2, k3):
		t.Fatalf("%s in (%s,%s]", k1, k2, k3)
	case k3.between(k1, k2):
		t.Fatalf("%s in (%s,%s]", k3, k1, k2)
	case k2.between(k3, k1):
		t.Fatalf("%s in (%s,%s]", k2, k3, k1)
	case !k3.between(k2, k1):
		t.Fatalf("%s not in (%s,%s]", k3, k2, k1)
	case !k1.between(k3, k2):
		t.Fatalf("%s not in (%s,%s]", k3, k2, k1)
	case !k2.between(k1, k3):
		t.Fatalf("%s not in (%s,%s]", k2, k1, k3)
	}

	k1, k2, k3 = 0x199, 0x255, 0x311
	switch {
	case k1.between(k2, k3):
		t.Fatalf("%s in (%s,%s]", k1, k2, k3)
	case k3.between(k1, k2):
		t.Fatalf("%s in (%s,%s]", k3, k1, k2)
	case k2.between(k3, k1):
		t.Fatalf("%s in (%s,%s]", k2, k3, k1)
	case !k3.between(k2, k1):
		t.Fatalf("%s not in (%s,%s]", k3, k2, k1)
	case !k1.between(k3, k2):
		t.Fatalf("%s not in (%s,%s]", k3, k2, k1)
	case !k2.between(k1, k3):
		t.Fatalf("%s not in (%s,%s]", k2, k1, k3)
	}
}

func TestKeyRingReserve(t *testing.T) {
	r := keyRing{1, 2, 3}
	r.reserve(2)
	switch {
	case len(r) != 3:
		t.Fatalf("unexpected length: %d", len(r))
	case cap(r) != 3:
		t.Fatalf("unexpected capacity: %d", cap(r))
	}

	r.reserve(3)
	switch {
	case len(r) != 3:
		t.Fatalf("unexpected length: %d", len(r))
	case cap(r) != 3:
		t.Fatalf("unexpected capacity: %d", cap(r))
	}

	r.reserve(12)
	switch {
	case len(r) != 3:
		t.Fatalf("unexpected length: %d", len(r))
	case cap(r) != 12:
		t.Fatalf("unexpected capacity: %d", cap(r))
	}
}

func TestKeyRingAdd(t *testing.T) {
	var r keyRing
	idx := r.add(1)
	switch {
	case idx != 0:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 1 || r[0] != 1:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.add(3)
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 2 || r[0] != 1 || r[1] != 3:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.add(2)
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 3 || r[0] != 1 || r[1] != 2 || r[2] != 3:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.add(4)
	switch {
	case idx != 3:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 4 || r[0] != 1 || r[1] != 2 || r[2] != 3 || r[3] != 4:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.add(1) // add already existing key
	switch {
	case idx >= 0:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 4 || r[0] != 1 || r[1] != 2 || r[2] != 3 || r[3] != 4:
		t.Fatalf("unexpected ring: %+v", r)
	}
}

func TestKeyRingRemove(t *testing.T) {
	r := keyRing{1, 3, 4, 6}

	idx := r.remove(5)
	switch {
	case idx >= 0:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 4 || r[0] != 1 || r[1] != 3 || r[2] != 4 || r[3] != 6:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.remove(1)
	switch {
	case idx != 0:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 3 || r[0] != 3 || r[1] != 4 || r[2] != 6:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.remove(4)
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 2 || r[0] != 3 || r[1] != 6:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.remove(6)
	switch {
	case idx != 1:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 1 || r[0] != 3:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.remove(9)
	switch {
	case idx >= 0:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 1 || r[0] != 3:
		t.Fatalf("unexpected ring: %+v", r)
	}

	idx = r.remove(3)
	switch {
	case idx != 0:
		t.Fatalf("unexpected index: %d", idx)
	case len(r) != 0:
		t.Fatalf("unexpected ring: %+v", r)
	}
}

func TestKeyRingSuccessor(t *testing.T) {
	var r keyRing
	idx := r.successor(3)
	if idx >= 0 {
		t.Fatalf("unexpected index: %d", idx)
	}

	r = keyRing{3, 5, 7}
	for i := 0; i < len(r); i++ {
		if idx = r.successor(r[i]); idx != i {
			t.Fatalf("unexpected index: %d", idx)
		}
	}

	k := []Key{2, 4, 6}
	for i := 0; i < len(k); i++ {
		if idx = r.successor(k[i]); idx != i {
			t.Fatalf("unexpected index: %d", idx)
		}
	}

	idx = r.successor(9)
	if idx != 0 {
		t.Fatalf("unexpected index: %d", idx)
	}
}

func TestMakeKeys(t *testing.T) {
	keys := makeKeys(7)
	if len(keys) != 7*keySize {
		t.Fatalf("unexpected length: %d", len(keys))
	}
}

func TestUnmarshalKeys(t *testing.T) {
	_, err := unmarshalKeys([]byte{1, 2, 3})
	if err != errMalformedKeys {
		t.Fatalf("unexpected error: %v", err)
	}

	keys, err := unmarshalKeys(nil)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(keys) != 0:
		t.Fatalf("unexpected length: %+v", keys)
	}

	blob := []byte{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 0, 0, 0, 0, 3,
	}
	keys, err = unmarshalKeys(blob)
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !bytes.Equal(keys, blob):
		t.Fatalf("unexpected keys: %s", printableKeys(keys))
	}
}

func TestKeysLength(t *testing.T) {
	var k keys
	if k.length() != 0 {
		t.Fatalf("unexpected length: %d", k.length())
	}

	k = keys{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 0, 0, 0, 0, 3,
	}
	if k.length() != 3 {
		t.Fatalf("unexpected length: %d", k.length())
	}
}

func TestKeysAt(t *testing.T) {
	keys := keys{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 0, 0, 0, 0, 3,
	}

	for i := 0; i < 3; i++ {
		if k := keys.at(i); k != Key(i+1) {
			t.Fatalf("unexpected key at %d: %s", i, k)
		}
	}
}

func TestKeysSet(t *testing.T) {
	keys := keys{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 0, 0, 0, 0, 3,
	}

	keys.set(1, 7)
	if k := keys.at(1); k != 7 {
		t.Fatalf("unexpected key: %s", k)
	}
}

func TestKeysSlice(t *testing.T) {
	keys := keys{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 0, 0, 0, 0, 3,
		0, 0, 0, 0, 0, 0, 0, 4,
		0, 0, 0, 0, 0, 0, 0, 5,
		0, 0, 0, 0, 0, 0, 0, 6,
		0, 0, 0, 0, 0, 0, 0, 7,
	}

	sliced := keys.slice(2, 5)
	if !bytes.Equal(sliced, keys[2*keySize:5*keySize]) {
		t.Fatalf("unexpected slice: %v", printableKeys(sliced))
	}
}

func containsKey(haystack keyRing, needle Key) bool {
	for i, n := 0, len(haystack); i < n; i++ {
		if haystack[i] == needle {
			return true
		}
	}
	return false
}

type printableKeys keys

func (k printableKeys) String() string {
	var b strings.Builder
	b.WriteByte('[')
	keys := keys(k)
	if nkeys := keys.length(); nkeys != 0 {
		b.WriteString(keys.at(0).String())
		for i := 1; i < nkeys; i++ {
			b.WriteString(", ")
			b.WriteString(keys.at(i).String())
		}
	}
	b.WriteByte(']')
	return b.String()
}

func (k printableKeys) GoString() string {
	return k.String()
}
