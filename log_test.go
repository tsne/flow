package flow

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestSetLogger(t *testing.T) {
	newLogger := newLogRecorder()
	SetLogger(newLogger)

	if l, ok := logger.(*logRecorder); !ok || l != newLogger {
		t.Fatalf("unexpected logger %+v", logger)
	}
}

func TestSetLoggerWithNilLogger(t *testing.T) {
	panicked := false

	func() {
		defer func() {
			panicked = recover() != nil
		}()
		SetLogger(nil)
	}()

	if !panicked {
		t.Fatal("panic expected")
	}
}

func TestDevNullLogger(t *testing.T) {
	logger := DevNullLogger()
	if _, ok := logger.(devNullLogger); !ok {
		t.Fatalf("unexpected logger type: %T", logger)
	}
}

func TestLogf(t *testing.T) {
	rec := newLogRecorder()
	logger = rec
	logf("log message with number %d", 7)

	switch {
	case rec.countMsgs() != 1:
		t.Fatalf("unexpected number of log messages: %d", rec.countMsgs())
	case rec.message(0) != "log message with number 7":
		t.Fatalf("unexpected log message: %s", rec.message(0))
	}
}

func TestStdLogger(t *testing.T) {
	var buf bytes.Buffer

	log.SetOutput(&buf)
	log.SetPrefix("")
	log.SetFlags(0)

	var stdlog stdLogger
	stdlog.Print("log message")
	if s := strings.TrimSpace(buf.String()); s != "log message" {
		t.Fatalf("unexpected log message: %v", s)
	}
}

type logRecorder struct {
	messages []string
}

func newLogRecorder() *logRecorder {
	return &logRecorder{}
}

func (r *logRecorder) Print(msg string) {
	r.messages = append(r.messages, msg)
}

func (r *logRecorder) countMsgs() int {
	return len(r.messages)
}

func (r *logRecorder) message(idx int) string {
	return r.messages[idx]
}
