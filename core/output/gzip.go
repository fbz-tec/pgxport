package output

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/internal/logger"
)

func newGzipWriter(path string) (io.WriteCloser, error) {
	start := time.Now()
	if !strings.HasSuffix(strings.ToLower(path), ".gz") {
		path += ".gz"
	}
	logger.Debug("Creating gzip-compressed output file: %s", path)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %w", err)
	}
	gzipWriter := gzip.NewWriter(file)
	return &compositeWriteCloser{
		Writer: gzipWriter,
		closeFunc: func() error {
			logger.Debug("Finalizing gzip compression for: %s", path)
			var err error
			if cerr := gzipWriter.Close(); cerr != nil {
				err = cerr
			}
			if ferr := file.Close(); ferr != nil && err == nil {
				err = ferr
			}
			logger.Debug("GZIP file closed successfully in %v", time.Since(start))
			return err
		},
	}, nil
}
