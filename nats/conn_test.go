package nats

import (
	"reflect"
	"testing"
)

func TestSplitAddresses(t *testing.T) {
	tests := []struct {
		addr     string
		expected []string
	}{
		{
			addr:     "foo",
			expected: []string{"foo"},
		},
		{
			addr:     "foo,bar",
			expected: []string{"foo", "bar"},
		},
		{
			addr:     " foo, bar ",
			expected: []string{"foo", "bar"},
		},
		{
			addr:     " foo, bar ,, ",
			expected: []string{"foo", "bar"},
		},
		{
			addr:     " ,, foo, bar ",
			expected: []string{"foo", "bar"},
		},
		{
			addr:     " ,,, foo, bar ,,, ",
			expected: []string{"foo", "bar"},
		},
	}

	for _, test := range tests {
		addrs := splitAddresses(test.addr)
		if !reflect.DeepEqual(addrs, test.expected) {
			t.Errorf("unexpected addresses for %q: %+v", test.addr, addrs)
		}
	}
}
