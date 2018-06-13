package flow

import "fmt"

const (
	// ErrClosed is reported for a pending acknowledgement after
	// the broker was closed.
	ErrClosed = errorString("closed")

	// ErrTimeout is reported when a request times out while waiting
	// for a response.
	ErrTimeout = errorString("timeout")

	errMalformedKey  = errorString("malformed key")
	errMalformedKeys = errorString("malformed keys")

	errMalformedFrame   = protocolError("malformed frame")
	errMalformedJoin    = protocolError("malformed join")
	errMalformedLeave   = protocolError("malformed leave")
	errMalformedInfo    = protocolError("malformed info")
	errMalformedPing    = protocolError("malformed ping")
	errMalformedFwd     = protocolError("malformed fwd")
	errMalformedAck     = protocolError("malformed ack")
	errMalformedReq     = protocolError("malformed req")
	errMalformedResp    = protocolError("malformed resp")
	errMalformedMessage = protocolError("malformed message")
)

type errorString string

func errorf(format string, args ...interface{}) error {
	return errorString(fmt.Sprintf(format, args...))
}

func (e errorString) Error() string {
	return string(e)
}

type optionError string

func (e optionError) Error() string {
	return "invalid option: " + string(e)
}

type protocolError string

func (e protocolError) Error() string {
	return "protocol error: " + string(e)
}
