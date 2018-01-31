package nats

import (
	"crypto/tls"
	"strings"
	"time"

	nats "github.com/nats-io/go-nats"
)

// Option represents an option to configure a NATS connection.
type Option func(*nats.Options) error

// TLS enables secure connections to the NATS servers.
func TLS(conf *tls.Config) Option {
	return func(o *nats.Options) error {
		if conf == nil {
			return optionError("no tls config specified")
		}
		o.Secure = true
		o.TLSConfig = conf
		return nil
	}
}

// TLSFromFiles enables secure connection by loading a key pair from
// the given files.
func TLSFromFiles(certFile, keyFile string) Option {
	return func(o *nats.Options) error {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}

		opt := TLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		return opt(o)
	}
}

// Reconnects enables or disables reconnections to the NATS servers.
// The count parameter specifies the maximum number of reconnect
// attempty, while the interval specifies the wait time between those
// attempts. If count is less than or equal to zero, reconnections
// will be disabled. Otherwise, the reconnections will be enabled.
func Reconnects(count int, interval time.Duration) Option {
	return func(o *nats.Options) error {
		switch {
		case count < 0:
			return optionError("negative number of reconnections")
		case interval < 0:
			return optionError("negative reconnection interval")
		}

		o.AllowReconnect = count > 0
		o.MaxReconnect = count
		o.ReconnectWait = interval
		return nil
	}
}

// Credentials sets the credentials which should be used for a server
// connection. If the given credentials are of the form "user:password",
// an user/password pair will be configured. Otherwise a security token
// is set. This option can be used twice, for the user/password pair
// and for the token.
func Credentials(cred string) Option {
	return func(o *nats.Options) error {
		if cred == "" {
			return optionError("no credentials specified")
		}

		if idx := strings.IndexByte(cred, ':'); idx < 0 {
			o.Token = cred
		} else {
			o.User = cred[:idx]
			o.Password = cred[idx+1:]
		}
		return nil
	}
}
