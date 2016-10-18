package flow

const (
	errTimeout = errorString("timeout")

	errMalformedMessage = protocolError("malformed message")
	errMalformedKey     = protocolError("malformed key")
	errMalformedKeys    = protocolError("malformed keys")
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
	return string(e)
}

type ackError string

func (e ackError) Error() string {
	return string(e)
}
