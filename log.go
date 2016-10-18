package flow

import (
	"fmt"
	"log"
)

var logger Logger = stdLogger{}

// Logger defines an interface which is used for logging messages
// in this package. The default logger uses the standard log package
// to log messages.
type Logger interface {
	Print(msg string)
}

// SetLogger sets a custom logger which will be used for logging.
// This function should be called before the broker is used, because
// it is not concurrency safe to set a logger.
func SetLogger(l Logger) {
	if l == nil {
		panic("logger is nil")
	}
	logger = l
}

// DevNullLogger returns a logger which discards all logging messages.
func DevNullLogger() Logger {
	return devNullLogger{}
}

func logf(s string, args ...interface{}) {
	logger.Print(fmt.Sprintf(s, args...))
}

type stdLogger struct{}

func (l stdLogger) Print(msg string) {
	log.Print(msg)
}

type devNullLogger struct{}

func (l devNullLogger) Print(msg string) {
}
