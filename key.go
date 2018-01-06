package flow

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
)

// KeySize defines the size of a key.
const KeySize = sha1.Size

// Key represents a key within a group. A key is used for partitioning.
type Key [KeySize]byte

// BytesKey returns a key for the given bytes.
func BytesKey(p []byte) Key {
	return Key(sha1.Sum(p))
}

// StringKey returns a key for the given string.
func StringKey(s string) Key {
	return BytesKey([]byte(s))
}

// String returns a string representation of the key.
func (k Key) String() string {
	return key(k[:]).String()
}

type key []byte

func keyFromBytes(p []byte) (key, error) {
	if len(p) != KeySize {
		return nil, errMalformedKey
	}
	return key(p), nil
}

func (k key) String() string {
	var buf [2 * KeySize]byte
	hex.Encode(buf[:], k[:])
	return string(buf[:])
}

func (k key) equal(other key) bool {
	return bytes.Equal(k, other)
}

func (k key) array() [KeySize]byte {
	var res [KeySize]byte
	copy(res[:], k)
	return res
}

func (k key) clone() key {
	res := make(key, KeySize)
	copy(res, k)
	return res
}

// check if k is in (lower,upper]
func (k key) between(lower, upper key) bool {
	if bytes.Compare(lower, upper) > 0 {
		return bytes.Compare(k, lower) > 0 || bytes.Compare(k, upper) <= 0
	}
	return bytes.Compare(k, lower) > 0 && bytes.Compare(k, upper) <= 0
}

type keys []byte

func makeKeys(n int) keys {
	return make(keys, n*KeySize)
}

func keysFromBytes(p []byte) (keys, error) {
	if nkeys := len(p) / KeySize; len(p) != nkeys*KeySize {
		return nil, errMalformedKeys
	}
	return keys(p), nil
}

func (k keys) length() int {
	return len(k) / KeySize
}

func (k keys) at(idx int) key {
	i := idx * KeySize
	return key(k[i : i+KeySize])
}

func (k keys) slice(start, end int) keys {
	return k[start*KeySize : end*KeySize]
}

type ring []byte

func (r ring) length() int {
	return len(r) / KeySize
}

func (r ring) at(idx int) key {
	i := idx * KeySize
	return key(r[i : i+KeySize])
}

func (r ring) slice(start, end int) ring {
	return r[start*KeySize : end*KeySize]
}

func (r *ring) reserve(newcap int) {
	if cap(*r) >= newcap*KeySize {
		return
	}
	res := make(ring, len(*r), newcap*KeySize)
	copy(res, *r)
	*r = res
}

func (r *ring) add(newKey key) int {
	for i := 0; i < len(*r); i += KeySize {
		cmp := bytes.Compare((*r)[i:i+KeySize], newKey)
		switch {
		case cmp == 0:
			// already exists
			return -1
		case cmp > 0:
			// insert at index i
			*r = append(*r, newKey...)
			copy((*r)[i+KeySize:], (*r)[i:len(*r)-KeySize])
			copy((*r)[i:i+KeySize], newKey)
			return i / KeySize
		}
	}
	*r = append(*r, newKey...)
	return r.length() - 1
}

func (r *ring) remove(rmKey key) int {
	for i := 0; i < len(*r); i += KeySize {
		cmp := bytes.Compare((*r)[i:i+KeySize], rmKey)
		switch {
		case cmp < 0:
			continue
		case cmp > 0:
			return -1
		default:
			copy((*r)[i:], (*r)[i+KeySize:])
			*r = (*r)[:len(*r)-KeySize]
			return i / KeySize
		}
	}
	return -1
}

// returns the index of k's successor
func (r ring) successor(k key) int {
	if len(r) == 0 {
		return -1
	}
	for i := 0; i < len(r); i += KeySize {
		if bytes.Compare(k, r[i:i+KeySize]) <= 0 {
			return i / KeySize
		}
	}
	return 0
}
