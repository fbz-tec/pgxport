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
)

type OutputConfig struct {
	Path        string
	Compression string
	Format      string
}

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
	default:
		return nil, fmt.Errorf("unsupported compression type %q", cfg.Compression)
	}
}
