package output

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/fbz-tec/pgxport/internal/logger"
)

// Finish closes the output writer and reports the close error through err,
// unless err already holds an earlier error. Closing writes the last buffered
// bytes and the compression trailer, so its error must not be dropped.
// If the export failed, the partial file is removed.
// Use it as: defer output.Finish(w, &err)
func Finish(w io.WriteCloser, err *error) {
	if cerr := w.Close(); cerr != nil && *err == nil {
		*err = fmt.Errorf("error closing output file: %w", cerr)
	}
	if *err == nil {
		return
	}
	var path string
	switch v := w.(type) {
	case *bufferedWriteCloser:
		path = v.path
	case *compositeWriteCloser:
		path = v.path
	}
	// Only regular files: never delete a device or symlink such as -o /dev/stdout.
	if info, lerr := os.Lstat(path); path == "" || lerr != nil || !info.Mode().IsRegular() {
		return
	}
	if rerr := os.Remove(path); rerr != nil {
		logger.Warn("Could not remove partial output file %s: %v", path, rerr)
		return
	}
	logger.Debug("Removed partial output file: %s", path)
}

// bufferedWriteCloser wraps a WriteCloser with buffered I/O
type bufferedWriteCloser struct {
	*bufio.Writer
	underlying io.WriteCloser
	path       string
}

// Close flushes the buffer and closes the underlying writer
func (bwc *bufferedWriteCloser) Close() error {
	if err := bwc.Writer.Flush(); err != nil {
		bwc.underlying.Close() // Attempt to close even if flush fails
		return fmt.Errorf("error flushing buffer: %w", err)
	}
	return bwc.underlying.Close()
}

// newBufferedWriteCloser creates a buffered writer with specified buffer size
func newBufferedWriteCloser(wc io.WriteCloser, size int, path string) io.WriteCloser {
	return &bufferedWriteCloser{
		Writer:     bufio.NewWriterSize(wc, size),
		underlying: wc,
		path:       path,
	}
}

type compositeWriteCloser struct {
	io.Writer
	closeFunc func() error
	path      string
}

// Close implements io.WriteCloser.
func (c *compositeWriteCloser) Close() error {
	if c.closeFunc == nil {
		return nil
	}
	return c.closeFunc()
}
