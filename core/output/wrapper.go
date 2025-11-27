package output

import (
	"bufio"
	"fmt"
	"io"
)

// bufferedWriteCloser wraps a WriteCloser with buffered I/O
type bufferedWriteCloser struct {
	*bufio.Writer
	underlying io.WriteCloser
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
func newBufferedWriteCloser(wc io.WriteCloser, size int) io.WriteCloser {
	return &bufferedWriteCloser{
		Writer:     bufio.NewWriterSize(wc, size),
		underlying: wc,
	}
}

type compositeWriteCloser struct {
	io.Writer
	closeFunc func() error
}

// Close implements io.WriteCloser.
func (c *compositeWriteCloser) Close() error {
	if c.closeFunc == nil {
		return nil
	}
	return c.closeFunc()
}
