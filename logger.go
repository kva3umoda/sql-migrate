package migrate

import (
	`fmt`
)

// Logger is the type that gorp uses to log SQL statements.
// See DbMap.TraceOn.
type Logger interface {
	Tracef(format string, v ...any)
	Infof(format string, v ...any)
	Errorf(format string, v ...any)
}

var _ Logger = (*defaultLogger)(nil)

type defaultLogger struct {
}

func DefaultLogger() *defaultLogger {
	return &defaultLogger{}
}

func (d defaultLogger) Tracef(format string, v ...any) {
	fmt.Printf("[MIGRATE-TRACE]\t"+format, v...)
}

func (d defaultLogger) Infof(format string, v ...any) {
	fmt.Printf("[MIGRATE-INFO]\t"+format, v...)
}

func (d defaultLogger) Errorf(format string, v ...any) {
	fmt.Printf("[MIGRATE-ERROR]\t"+format, v...)
}
