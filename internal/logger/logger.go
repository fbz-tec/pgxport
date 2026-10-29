package logger

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/term"
)

// ConsoleLogger writes log messages to stdout, and warnings and errors to stderr.
type ConsoleLogger struct {
	output      io.Writer
	errOut      io.Writer
	verboseMode bool
	quietMode   bool
	mu          sync.Mutex
}

var (
	// std is the logger behind the package functions (logger.Debug, logger.Warn...).
	std = &ConsoleLogger{output: os.Stdout, errOut: os.Stderr}
	// Enable colors only if stdout is a terminal
	isTTY = term.IsTerminal(int(os.Stdout.Fd()))
)

// SetVerbose enables or disables verbose mode globally
func SetVerbose(verbose bool) { std.SetVerbose(verbose) }

func IsVerbose() bool { return std.IsVerbose() }

func SetQuiet(quiet bool) { std.SetQuiet(quiet) }

// Global helper functions for convenience
func Info(format string, args ...any)    { std.Info(format, args...) }
func Debug(format string, args ...any)   { std.Debug(format, args...) }
func Success(format string, args ...any) { std.Success(format, args...) }
func Warn(format string, args ...any)    { std.Warn(format, args...) }
func Error(format string, args ...any)   { std.Error(format, args...) }

// -------------------- Implementation --------------------

func (l *ConsoleLogger) SetVerbose(enabled bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.verboseMode = enabled
}

func (l *ConsoleLogger) IsVerbose() bool {
	return l.verboseMode
}

func (l *ConsoleLogger) SetQuiet(enabled bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.quietMode = enabled
}

func (l *ConsoleLogger) timestamp() string {
	return time.Now().Format("2006-01-02 15:04:05.000")
}

func (l *ConsoleLogger) log(out io.Writer, prefix, color, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	if isTTY {
		fmt.Fprintf(out, "%s%s %s%s\n", color, prefix, msg, resetColor)
	} else {
		fmt.Fprintf(out, "%s %s\n", prefix, msg)
	}

}

const (
	blueColor   = "\033[34m"
	greenColor  = "\033[32m"
	yellowColor = "\033[33m"
	redColor    = "\033[31m"
	grayColor   = "\033[90m"
	resetColor  = "\033[0m"
)

func (l *ConsoleLogger) Info(format string, args ...any) {
	if l.quietMode {
		return
	}
	icon := "ℹ️"
	if !isTTY {
		icon = "INFO"
	}
	l.log(l.output, icon, blueColor, format, args...)
}

func (l *ConsoleLogger) Debug(format string, args ...any) {
	if !l.verboseMode {
		return
	}

	icon := "🔍"
	if !isTTY {
		icon = "DEBUG"
	}

	l.log(l.output, fmt.Sprintf("[%s] %s", l.timestamp(), icon), grayColor, format, args...)
}

func (l *ConsoleLogger) Success(format string, args ...any) {
	if l.quietMode {
		return
	}
	icon := "✓"
	if !isTTY {
		icon = "SUCCESS"
	}
	l.log(l.output, icon, greenColor, format, args...)
}

func (l *ConsoleLogger) Warn(format string, args ...any) {
	if l.quietMode {
		return
	}
	icon := "⚠"
	if !isTTY {
		icon = "WARN"
	}
	l.log(l.output, icon, yellowColor, format, args...)
}

func (l *ConsoleLogger) Error(format string, args ...any) {
	icon := "✗"
	if !isTTY {
		icon = "ERROR"
	}
	l.log(l.errOut, icon, redColor, format, args...)
}
