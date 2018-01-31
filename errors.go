package flow

import "fmt"

const (
	errClosing       = errorString("closing")
	errRespTimeout   = errorString("response timeout")
	errMalformedKey  = errorString("malformed key")
	errMalformedKeys = errorString("malformed keys")

	errMalformedFrame = protocolError("malformed frame")
	errMalformedJoin  = protocolError("malformed join")
	errMalformedLeave = protocolError("malformed leave")
	errMalformedInfo  = protocolError("malformed info")
	errMalformedPing  = protocolError("malformed ping")
	errMalformedAck   = protocolError("malformed ack")
	errMalformedPub   = protocolError("malformed pub")
	errMalformedFwd   = protocolError("malformed fwd")
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

type ackError string

func (e ackError) Error() string {
	return string(e)
}
