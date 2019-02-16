package flow

import (
	"crypto/rand"
	"encoding/binary"
)

const keySize = 8

// Key represents a key within a broker clique and is used
// for partitioning.
type Key uint64

// KeyFromBytes returns a key for the given bytes.
func KeyFromBytes(p []byte) Key {
	return Key(murmur3(p))
}

// KeyFromString returns a key for the given string.
func KeyFromString(s string) Key {
	return KeyFromBytes([]byte(s))
}

// RandomKey returns a randomly generated key.
func RandomKey() (Key, error) {
	var buf [keySize]byte
	_, err := rand.Read(buf[:])
	return Key(binary.BigEndian.Uint64(buf[:])), err
}

func marshalKey(k Key, p []byte) {
	binary.BigEndian.PutUint64(p, uint64(k))
}

func unmarshalKey(p []byte) (Key, error) {
	if len(p) != keySize {
		return 0, errMalformedKey
	}
	return Key(binary.BigEndian.Uint64(p)), nil
}

// String returns a string representation of the key.
func (k Key) String() string {
	var buf [2 * keySize]byte
	k.writeString(buf[:])
	return string(buf[:])
}

func (k Key) writeString(p []byte) {
	const digits = "0123456789abcdef"

	shift := uint(64)
	for i := 0; shift != 0; i++ {
		shift -= 4
		p[i] = digits[(k>>shift)&0xf]
	}
}

// check if k is in (lower,upper]
func (k Key) between(lower, upper Key) bool {
	if lower > upper {
		return k > lower || k <= upper
	}
	return lower < k && k <= upper
}

type keyRing []Key

func (r *keyRing) reserve(newcap int) {
	if cap(*r) < newcap {
		res := make(keyRing, len(*r), newcap)
		copy(res, *r)
		*r = res
	}
}

func (r *keyRing) add(newKey Key) int {
	for i := 0; i < len(*r); i++ {
		k := (*r)[i]
		switch {
		case k == newKey:
			// already exists
			return -1
		case k > newKey:
			// insert at index i
			*r = append(*r, 0)
			copy((*r)[i+1:], (*r)[i:])
			(*r)[i] = newKey
			return i
		}
	}
	*r = append(*r, newKey)
	return len(*r) - 1
}

func (r *keyRing) remove(rmKey Key) int {
	for i := 0; i < len(*r); i++ {
		k := (*r)[i]
		switch {
		case k < rmKey:
			continue
		case k > rmKey:
			return -1
		default:
			copy((*r)[i:], (*r)[i+1:])
			*r = (*r)[:len(*r)-1]
			return i
		}
	}
	return -1
}

// returns the index of k's successor
func (r keyRing) successor(k Key) int {
	if len(r) == 0 {
		return -1
	}
	for i := 0; i < len(r); i++ {
		if k <= r[i] {
			return i
		}
	}
	return 0
}

type keys []byte

func makeKeys(n int) keys {
	return alloc(n*keySize, nil)
}

func unmarshalKeys(p []byte) (keys, error) {
	if nkeys := len(p) / keySize; len(p) != nkeys*keySize {
		return nil, errMalformedKeys
	}
	return keys(p), nil
}

func (k keys) length() int {
	return len(k) / keySize
}

func (k keys) at(idx int) Key {
	return Key(binary.BigEndian.Uint64(k[idx*keySize:]))
}

func (k keys) set(idx int, key Key) {
	binary.BigEndian.PutUint64(k[idx*keySize:], uint64(key))
}

func (k keys) slice(start, end int) keys {
	return k[start*keySize : end*keySize]
}

// murmur3 implements the MurmurHash3 algorithm for 128 bits and takes
// the upper 64 bits for the hash value.
func murmur3(p []byte) uint64 {
	const (
		c1 = uint64(0x87c37b91114253d5)
		c2 = uint64(0x4cf5ad432745937f)
	)

	var h1, h2, k1, k2 uint64

	// body
	nbytes := uint64(len(p))
	for i, n := uint64(0), nbytes/16; i < n; i++ {
		k1 = binary.BigEndian.Uint64(p[8*i:])
		k2 = binary.BigEndian.Uint64(p[8*i+8:])

		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	// tail
	k1, k2 = 0, 0
	p = p[(nbytes/16)*16:]
	switch len(p) {
	case 15:
		k2 ^= uint64(p[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(p[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(p[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(p[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(p[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(p[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(p[8]) << 0
		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough

	case 8:
		k1 ^= uint64(p[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(p[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(p[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(p[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(p[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(p[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(p[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(p[0]) << 0
		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	// finalization
	h1 ^= nbytes
	h2 ^= nbytes

	h1 += h2
	h2 += h1

	h1 = fmix64(h1)
	h2 = fmix64(h2)

	h1 += h2
	//h2 += h1

	return h1
}

func rotl64(x uint64, r uint) uint64 {
	return (x << r) | (x >> (64 - r))
}

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= uint64(0xff51afd7ed558ccd)
	k ^= k >> 33
	k *= uint64(0xc4ceb9fe1a85ec53)
	k ^= k >> 33
	return k
}
