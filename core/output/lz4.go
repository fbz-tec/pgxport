package output

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/pierrec/lz4/v4"
)

func newLz4Writer(path string) (io.WriteCloser, error) {
	start := time.Now()
	if !strings.HasSuffix(strings.ToLower(path), ".lz4") {
		path += ".lz4"
	}
	logger.Debug("Creating lz4-compressed output file: %s", path)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %w", err)
	}
	lz4Writer := lz4.NewWriter(file)
	return &compositeWriteCloser{
		Writer: lz4Writer,
		closeFunc: func() error {
			logger.Debug("Finalizing lz4 compression for: %s", path)
			var err error
			if cerr := lz4Writer.Close(); cerr != nil {
				err = cerr
			}
			if ferr := file.Close(); ferr != nil && err == nil {
				err = ferr
			}
			logger.Debug("lz4 file closed successfully in %v", time.Since(start))
			return err
		},
	}, nil
}
