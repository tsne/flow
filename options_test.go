package flow

import (
	"bytes"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()
	switch {
	case opts.codec != DefaultCodec{}:
		t.Fatalf("unexpected codec: %T", opts.codec)
	case opts.errorHandler == nil:
		t.Fatal("expected error handler, got none")
	case opts.groupName == "":
		t.Fatal("unexpected empty group name")
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

	err = opts.apply(MessageCodec(DefaultCodec{}))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.codec != DefaultCodec{}:
		t.Fatalf("unexpected codec: %T", opts.codec)
	}
}

func TestOptionErrorHandler(t *testing.T) {
	var opts options

	err := opts.apply(ErrorHandler(nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	errorHandlerCalled := false
	errorHandler := func(error) { errorHandlerCalled = true }

	err = opts.apply(ErrorHandler(errorHandler))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	opts.errorHandler(errorString("something went wrong"))
	if !errorHandlerCalled {
		t.Fatal("error handler not called")
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

	key := KeyFromString("node key")
	err := opts.apply(NodeKey(key))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case !bytes.Equal(opts.nodeKey, key[:]):
		t.Fatalf("unexpected node key: %s", printableKey(opts.nodeKey))
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
