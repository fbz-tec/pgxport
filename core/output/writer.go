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
	path := FinalPath(cfg.Path, cfg.Compression)
	switch strings.ToLower(strings.TrimSpace(cfg.Compression)) {
	case None:
		return newFileWriter(path)
	case GZIP:
		return newGzipWriter(path)
	case ZIP:
		return newZipWriter(path, determineZipEntryName(cfg.Path, cfg.Format))
	case ZSTD:
		return newZstdWriter(path)
	case LZ4:
		return newLz4Writer(path)
	default:
		return nil, fmt.Errorf("unsupported compression type %q", cfg.Compression)
	}
}

// FinalPath returns the name of the file actually written for path with this compression:
// gzip, zstd and lz4 add their extension when missing (users.csv.gz), zip replaces the
// extension (users.zip). Other values return path unchanged.
func FinalPath(path, compression string) string {
	switch strings.ToLower(strings.TrimSpace(compression)) {
	case GZIP:
		return withSuffix(path, ".gz")
	case ZSTD:
		return withSuffix(path, ".zst")
	case LZ4:
		return withSuffix(path, ".lz4")
	case ZIP:
		return fixExtension(path, ".zip")
	}
	return path
}

func withSuffix(path, ext string) string {
	if strings.HasSuffix(strings.ToLower(path), ext) {
		return path
	}
	return path + ext
}
