package flow

import "testing"

func TestErrorString(t *testing.T) {
	err := errorString("error text")
	if err.Error() != "error text" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}

func TestOptionError(t *testing.T) {
	err := optionError("error text")
	if err.Error() != "invalid option: error text" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}

func TestProtocolError(t *testing.T) {
	err := protocolError("error text")
	if err.Error() != "error text" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}

func TestAckError(t *testing.T) {
	err := ackError("error text")
	if err.Error() != "error text" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}
