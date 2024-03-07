package logger

// Logger is the type that gorp uses to log SQL statements.
// See DbMap.TraceOn.
type Logger interface {
	Printf(format string, v ...interface{})
}
