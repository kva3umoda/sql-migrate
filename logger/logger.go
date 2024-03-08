package logger

import "fmt"

// Logger is the type that gorp uses to log SQL statements.
// See DbMap.TraceOn.
type Logger interface {
	Printf(format string, v ...any)
}

type defaultLogger struct {
}

func DefaultLogger() *defaultLogger {
	return &defaultLogger{}
}

func (*defaultLogger) Printf(format string, v ...any) {
	fmt.Printf(format, v...)
}
