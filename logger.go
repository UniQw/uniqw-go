package uniqw

import (
	"fmt"
	"os"
)

// Logger defines logging methods used by the library. Implementations should be cheap.
// Default is FmtLogger which writes to stdout/stderr using fmt.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// FmtLogger is a minimal logger that prints messages with level prefixes.
// Debug/Info go to stdout; Warn/Error go to stderr.
type FmtLogger struct{}

// NewFmtLogger creates a new FmtLogger.
func NewFmtLogger() *FmtLogger { return &FmtLogger{} }

func (FmtLogger) Debugf(format string, args ...any) { fmt.Printf("[DEBUG] "+format+"\n", args...) }
func (FmtLogger) Infof(format string, args ...any)  { fmt.Printf("[INFO]  "+format+"\n", args...) }
func (FmtLogger) Warnf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[WARN]  "+format+"\n", args...)
}
func (FmtLogger) Errorf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[ERROR] "+format+"\n", args...)
}
