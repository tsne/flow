package flow

import (
	"context"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()
	switch {
	case len(opts.messageHandlers) != 0:
		t.Fatalf("unexpected number of message handlers: %d", len(opts.messageHandlers))
	case len(opts.requestHandlers) != 0:
		t.Fatalf("unexpected number of request handlers: %d", len(opts.requestHandlers))
	case opts.codec != DefaultCodec{}:
		t.Fatalf("unexpected codec: %T", opts.codec)
	case opts.errorHandler == nil:
		t.Fatal("expected error handler, got none")
	case opts.clique == "":
		t.Fatal("unexpected empty clique")
	case opts.stabilization.Successors <= 0:
		t.Fatalf("unexpected stabilization successor count: %d", opts.stabilization.Successors)
	case opts.stabilization.Stabilizers <= 0:
		t.Fatalf("unexpected stabilizer count: %d", opts.stabilization.Stabilizers)
	case opts.stabilization.Interval <= 0:
		t.Fatalf("unexpected stabilization interval: %v", opts.stabilization.Interval)
	case opts.ackTimeout <= 0:
		t.Fatalf("unexpected ack timeout: %v", opts.ackTimeout)
	case opts.reqTimeout <= 0:
		t.Fatalf("unexpected req timeout: %v", opts.reqTimeout)
	}
}

func TestOptionsApplyWithoutUserOptions(t *testing.T) {
	defaultOpts := defaultOptions()
	opts := defaultOpts
	if err := opts.apply(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	switch {
	case len(opts.messageHandlers) != len(defaultOpts.messageHandlers):
		t.Fatalf("unexpected message handlers: %v", opts.messageHandlers)
	case len(opts.requestHandlers) != len(defaultOpts.requestHandlers):
		t.Fatalf("unexpected request handlers: %v", opts.requestHandlers)
	case opts.codec != defaultOpts.codec:
		t.Fatalf("unexpected codec: %T", opts.codec)
	case opts.clique != defaultOpts.clique:
		t.Fatalf("unexpected clique: %s", opts.clique)
	case opts.nodeKey == defaultOpts.nodeKey:
		t.Fatalf("unexpected node key: %s", opts.nodeKey)
	case opts.stabilization.Successors != defaultOpts.stabilization.Successors:
		t.Fatalf("unexpected successor count: %d", opts.stabilization.Successors)
	case opts.stabilization.Stabilizers != defaultOpts.stabilization.Stabilizers:
		t.Fatalf("unexpected stabilizer count: %d", opts.stabilization.Stabilizers)
	case opts.stabilization.Interval != defaultOpts.stabilization.Interval:
		t.Fatalf("unexpected stabilization interval: %v", opts.stabilization.Interval)
	case opts.ackTimeout != defaultOpts.ackTimeout:
		t.Fatalf("unexpected ack timeout: %v", opts.ackTimeout)
	case opts.reqTimeout != defaultOpts.reqTimeout:
		t.Fatalf("unexpected req timeout: %v", opts.reqTimeout)
	}
}

func TestOptionWithMessageHandler(t *testing.T) {
	opts := options{
		messageHandlers: make(map[string]MessageHandler),
	}

	h := func(context.Context, Message) {}
	err := opts.apply(WithMessageHandler("", h))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithMessageHandler("stream", nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithMessageHandler("stream", h))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(opts.messageHandlers) != 1:
		t.Fatalf("unexpected number of message handlers: %d", len(opts.messageHandlers))
	case opts.messageHandlers["stream"] == nil:
		t.Fatal("missing message handler for stream")
	}

	err = opts.apply(WithMessageHandler("stream", h))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithRequestHandler("stream", func(context.Context, Message) Message { return Message{} }))
	if err == nil {
		t.Fatal("error expected, got none")
	}
}

func TestOptionWithRequestHandler(t *testing.T) {
	opts := options{
		requestHandlers: make(map[string]RequestHandler),
	}

	h := func(context.Context, Message) Message { return Message{} }
	err := opts.apply(WithRequestHandler("", h))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithRequestHandler("stream", nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithRequestHandler("stream", h))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(opts.requestHandlers) != 1:
		t.Fatalf("unexpected number of request handlers: %d", len(opts.requestHandlers))
	case opts.requestHandlers["stream"] == nil:
		t.Fatal("missing request handler for stream")
	}

	err = opts.apply(WithRequestHandler("stream", h))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithMessageHandler("stream", func(context.Context, Message) {}))
	if err == nil {
		t.Fatal("error expected, got none")
	}
}

func TestOptionWithErrorHandler(t *testing.T) {
	var opts options

	err := opts.apply(WithErrorHandler(nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	errorHandlerCalled := false
	errorHandler := func(error) { errorHandlerCalled = true }

	err = opts.apply(WithErrorHandler(errorHandler))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	opts.errorHandler(errorString("something went wrong"))
	if !errorHandlerCalled {
		t.Fatal("error handler not called")
	}
}

func TestOptionWithPartition(t *testing.T) {
	var opts options

	key := KeyFromString("key one")
	err := opts.apply(WithPartition("", key))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.clique != defaultClique:
		t.Fatalf("unexpected clique: %s", opts.clique)
	case opts.nodeKey != key:
		t.Fatalf("unexpected node key: %s", opts.nodeKey)
	}

	key = KeyFromString("key two")
	err = opts.apply(WithPartition("not-the-default-clique", key))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.clique != "not-the-default-clique":
		t.Fatalf("unexpected clique: %s", opts.clique)
	case opts.nodeKey != key:
		t.Fatalf("unexpected node key: %s", opts.nodeKey)
	}
}

func TestOptionWithCodec(t *testing.T) {
	var opts options

	err := opts.apply(WithCodec(nil))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithCodec(DefaultCodec{}))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.codec != DefaultCodec{}:
		t.Fatalf("unexpected codec: %T", opts.codec)
	}
}

func TestOptionWithStabilization(t *testing.T) {
	opts := options{
		stabilization: Stabilization{
			Successors:  3,
			Stabilizers: 4,
			Interval:    time.Second,
		},
	}

	// negative
	err := opts.apply(WithStabilization(Stabilization{Successors: -1}))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithStabilization(Stabilization{Stabilizers: -1}))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	err = opts.apply(WithStabilization(Stabilization{Interval: -time.Minute}))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// zero
	err = opts.apply(WithStabilization(Stabilization{}))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.stabilization.Successors != 3:
		t.Fatalf("unexpected number of successors: %d", opts.stabilization.Successors)
	case opts.stabilization.Stabilizers != 4:
		t.Fatalf("unexpected number of stabilizers: %d", opts.stabilization.Stabilizers)
	case opts.stabilization.Interval != time.Second:
		t.Fatalf("unexpected stabilization interval: %v", opts.stabilization.Interval)
	}

	// positive
	err = opts.apply(WithStabilization(Stabilization{
		Successors:  5,
		Stabilizers: 6,
		Interval:    time.Minute,
	}))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.stabilization.Successors != 5:
		t.Fatalf("unexpected number of successors: %d", opts.stabilization.Successors)
	case opts.stabilization.Stabilizers != 6:
		t.Fatalf("unexpected number of stabilizers: %d", opts.stabilization.Stabilizers)
	case opts.stabilization.Interval != time.Minute:
		t.Fatalf("unexpected stabilization interval: %v", opts.stabilization.Interval)
	}
}

func TestOptionWithAckTimeout(t *testing.T) {
	var opts options

	// negative
	err := opts.apply(WithAckTimeout(-1))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// zero
	err = opts.apply(WithAckTimeout(0))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// positive
	err = opts.apply(WithAckTimeout(time.Second))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.ackTimeout != time.Second:
		t.Fatalf("unexpected successor count: %d", opts.ackTimeout)
	}
}

func TestOptionWithRequestTimeout(t *testing.T) {
	var opts options

	// negative
	err := opts.apply(WithRequestTimeout(-1))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// zero
	err = opts.apply(WithRequestTimeout(0))
	if err == nil {
		t.Fatal("error expected, got none")
	}

	// positive
	err = opts.apply(WithRequestTimeout(time.Second))
	switch {
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	case opts.reqTimeout != time.Second:
		t.Fatalf("unexpected successor count: %d", opts.reqTimeout)
	}
}
