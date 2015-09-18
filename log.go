/*
 * portal - marshal
 *
 * this package exists for the simple reason that go vet complains bitterly
 * that the go-logging package we use named its error printer 'Error' without
 * the trailing 'f', and yet it accepts a format string. blah.
 *
 */

package marshal

import "github.com/op/go-logging"

type logger struct {
	l *logging.Logger
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.l.Error(format, args...)
}

func (l *logger) Warningf(format string, args ...interface{}) {
	l.l.Warning(format, args...)
}

func (l *logger) Infof(format string, args ...interface{}) {
	l.l.Info(format, args...)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.l.Debug(format, args...)
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	l.l.Fatalf(format, args...)
}

var log *logger

func init() {
	log = &logger{l: logging.MustGetLogger("PortalMarshal")}
	logging.SetLevel(logging.WARNING, "PortalMarshal")
}
