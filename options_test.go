package flow

import (
	"bytes"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()
	switch {
	case opts.codec != binaryCodec{}:
		t.Fatalf("unexpected codec: %T", opts.codec)
	case opts.groupName == "":
		t.Fatal("unexpected empty group name")
	case opts.store == nil:
		t.Fatal("unexpected empty store")
	case opts.successorCount == 0:
		t.Fatal("unexpected successor count: 0")
	case opts.stabilizerCount == 0:
		t.Fatal("unexpected stabilizer count: 0")
	case opts.stabilizationInterval <= 0:
		t.Fatalf("unexpected stabilization interval: %v", opts.stabilizationInterval)
	case opts.ackTimeout <= 0:
		t.Fatalf("unexpected ack timeout: %v", opts.ackTimeout)
	}
}

func TestOptionsApplyWithoutUserOptions(t *testing.T) {
	defaultOpts := defaultOptions()
	opts := defaultOpts
	if err := opts.apply(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	switch {
	case opts.codec != defaultOpts.codec:
		t.Fatalf("unexpected codec: %T", opts.codec)
	case opts.groupName != defaultOpts.groupName:
		t.Fatalf("unexpected group name: %s", opts.groupName)
	case len(opts.nodeKey) == 0:
		t.Fatal("expected node id to be not empty")
	case opts.store != defaultOpts.store:
		t.Fatalf("unexpected store: %+v", opts.store)
	case opts.storeFilter == nil:
		t.Fatal("expected store filter to be set")
	case opts.successorCount != defaultOpts.successorCount:
		t.Fatalf("unexpected successor count: %d", opts.successorCount)
	case opts.stabilizerCount != defaultOpts.stabilizerCount:
		t.Fatalf("unexpected stabilizer count: %d", opts.stabilizerCount)
	case opts.stabilizationInterval != defaultOpts.stabilizationInterval:
		t.Fatalf("unexpected stabilization interval: %v", opts.stabilizationInterval)
	case opts.ackTimeout != defaultOpts.ackTimeout:
		t.Fatalf("unexpected ack timeout: %v", opts.ackTimeout)
	}
}

func TestOptionMessageCodec(t *testing.T) {
	var opts options

	err := opts.apply(MessageCodec(nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(MessageCodec(binaryCodec{}))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.codec != binaryCodec{}:
		t.Fatalf("unexpected codec: %T", opts.codec)
	}
}

func TestOptionGroup(t *testing.T) {
	var opts options

	err := opts.apply(Group(""))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(Group("group name"))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.groupName != "group name":
		t.Fatalf("unexpected group name: %s", opts.groupName)
	}
}

func TestOptionNodeKey(t *testing.T) {
	var opts options

	key := StringKey("node key")
	err := opts.apply(NodeKey(key))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !bytes.Equal(opts.nodeKey, key[:]):
		t.Fatalf("unexpected node key", printableKey(opts.nodeKey))
	}
}

func TestOptionStorage(t *testing.T) {
	var opts options

	err := opts.apply(Storage(nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	store := newStoreRecorder()
	err = opts.apply(Storage(store))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s, ok := opts.store.(*storeRecorder); !ok || s != store {
		t.Fatalf("unexpected store: %+v", opts.store)
	}
}

func TestOptionStorageFilter(t *testing.T) {
	var opts options

	err := opts.apply(StorageFilter(nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(StorageFilter(func(string) bool { return true }))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.storeFilter == nil:
		t.Fatal("expected store filter to be set")
	}
}

func TestOptionSuccessors(t *testing.T) {
	var opts options

	// negative
	err := opts.apply(Successors(-1))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// zero
	err = opts.apply(Successors(0))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// positive
	err = opts.apply(Successors(7))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.successorCount != 7:
		t.Fatalf("unexpected successor count: %d", opts.successorCount)
	}
}

func TestOptionStabilizers(t *testing.T) {
	var opts options

	// negative
	err := opts.apply(Stabilizers(-1))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// zero
	err = opts.apply(Stabilizers(0))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// positive
	err = opts.apply(Stabilizers(7))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.stabilizerCount != 7:
		t.Fatalf("unexpected successor count: %d", opts.stabilizerCount)
	}
}

func TestOptionStabilizationInterval(t *testing.T) {
	var opts options

	// negative
	err := opts.apply(StabilizationInterval(-1))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// zero
	err = opts.apply(StabilizationInterval(0))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// positive
	err = opts.apply(StabilizationInterval(time.Second))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.stabilizationInterval != time.Second:
		t.Fatalf("unexpected successor count: %d", opts.stabilizationInterval)
	}
}

func TestOptionAckTimeout(t *testing.T) {
	var opts options

	// negative
	err := opts.apply(AckTimeout(-1))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// zero
	err = opts.apply(AckTimeout(0))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// positive
	err = opts.apply(AckTimeout(time.Second))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.ackTimeout != time.Second:
		t.Fatalf("unexpected successor count: %d", opts.ackTimeout)
	}
}
