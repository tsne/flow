package flow

const (
	errTimeout       = errorString("timeout")
	errMalformedKey  = errorString("malformed key")
	errMalformedKeys = errorString("malformed keys")

	errMalformedMessage  = protocolError("malformed message")
	errMalformedJoinMsg  = protocolError("malformed join message")
	errMalformedLeaveMsg = protocolError("malformed leave message")
	errMalformedInfoMsg  = protocolError("malformed info message")
	errMalformedPingMsg  = protocolError("malformed ping message")
	errMalformedAckMsg   = protocolError("malformed ack message")
	errMalformedPubMsg   = protocolError("malformed pub message")
	errMalformedFwdMsg   = protocolError("malformed fwd message")
)

type errorString string

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
