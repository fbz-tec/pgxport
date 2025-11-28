package output

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/klauspost/compress/zstd"
)

func newZstdWriter(path string) (io.WriteCloser, error) {
	start := time.Now()
	if !strings.HasSuffix(strings.ToLower(path), ".zst") {
		path += ".zst"
	}
	logger.Debug("Creating Zstandard-compressed output file: %s", path)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %w", err)
	}
	zstdWriter, err := zstd.NewWriter(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("error creating zstd writer: %w", err)
	}
	return &compositeWriteCloser{
		Writer: zstdWriter,
		closeFunc: func() error {
			logger.Debug("Finalizing zstd compression for: %s", path)
			var err error
			if cerr := zstdWriter.Close(); cerr != nil {
				err = cerr
			}
			if ferr := file.Close(); ferr != nil && err == nil {
				err = ferr
			}
			logger.Debug("zstd file closed successfully in %v", time.Since(start))
			return err
		},
	}, nil
}
