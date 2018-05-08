package flow

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestAlloc(t *testing.T) {
	p := alloc(16, nil)
	if len(p) != 16 {
		t.Errorf("unexpected memory size: %d", len(p))
	}

	// shrink
	q := alloc(8, p)
	switch {
	case len(q) != 8:
		t.Errorf("unexpected memory size: %d", len(q))
	case !equalAddress(p, q):
		t.Errorf("unexpected memory address: %p", q)
	}

	// grow
	q = alloc(16, q)
	switch {
	case len(q) != 16:
		t.Errorf("unexpected memory size: %d", len(q))
	case !equalAddress(p, q):
		t.Errorf("unexpected memory address: %p", q)
	}

	// exceed
	q = alloc(32, q)
	switch {
	case len(q) != 32:
		t.Errorf("unexpected memory size: %d", len(q))
	case equalAddress(p, q):
		t.Errorf("unexpected memory address: %p", q)
	}
}

func equalAddress(p, q []byte) bool {
	ph := (*reflect.SliceHeader)(unsafe.Pointer(&p))
	qh := (*reflect.SliceHeader)(unsafe.Pointer(&q))
	return ph.Data == qh.Data
}
