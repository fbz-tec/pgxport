package output

import (
	"fmt"
	"io"
	"os"

	"github.com/fbz-tec/pgxport/internal/logger"
)

func newFileWriter(path string) (io.WriteCloser, error) {
	logger.Debug("Creating uncompressed output file: %s", path)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %w", err)
	}
	// Using 256KB buffer provides optimal throughput for large exports
	return newBufferedWriteCloser(file, 256*1024), nil
}
