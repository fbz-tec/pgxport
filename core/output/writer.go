package output

import (
	"fmt"
	"io"
	"strings"
)

const (
	None = "none"
	GZIP = "gzip"
	ZIP  = "zip"
	ZSTD = "zstd"
	LZ4  = "lz4"
)

// OutputConfig holds configuration for output file creation.
type OutputConfig struct {
	Path        string
	Compression string
	Format      string
}

// CreateWriter creates a new writer based on the output configuration.
// Supports various compression formats: none, gzip, zip, zstd, lz4.
// Returns an error if the compression type is unsupported or file creation fails.
func CreateWriter(cfg OutputConfig) (io.WriteCloser, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Compression)) {
	case None:
		return newFileWriter(cfg.Path)
	case GZIP:
		return newGzipWriter(cfg.Path)
	case ZIP:
		return newZipWriter(cfg.Path, cfg.Format)
	case ZSTD:
		return newZstdWriter(cfg.Path)
	case LZ4:
		return newLz4Writer(cfg.Path)
	default:
		return nil, fmt.Errorf("unsupported compression type %q", cfg.Compression)
	}
}
