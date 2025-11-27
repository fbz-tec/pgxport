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
	default:
		return nil, fmt.Errorf("unsupported compression type %q", cfg.Compression)
	}
}
