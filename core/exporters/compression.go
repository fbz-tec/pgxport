package exporters

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/internal/logger"
)

const (
	None = "none"
	GZIP = "gzip"
	ZIP  = "zip"
)

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

func createOutputWriter(path string, options ExportOptions, format string) (io.WriteCloser, error) {
	start := time.Now()
	compression := strings.ToLower(strings.TrimSpace(options.Compression))
	switch compression {
	case None:
		logger.Debug("Creating uncompressed output file: %s", path)
		file, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("error creating file: %w", err)
		}
		return file, nil

	case GZIP:
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

	case ZIP:
		fixedPath := fixExtension(path, ".zip")
		logger.Debug("Creating zip-compressed output file: %s", fixedPath)
		file, err := os.Create(fixedPath)
		if err != nil {
			return nil, fmt.Errorf("error creating file: %w", err)
		}
		zipWriter := zip.NewWriter(file)
		entryName := determineZipEntryName(path, format)
		logger.Debug("Creating zip entry: %s", entryName)
		entryWriter, err := zipWriter.Create(entryName)
		if err != nil {
			zipWriter.Close()
			file.Close()
			return nil, fmt.Errorf("error creating zip entry: %w", err)
		}
		return &compositeWriteCloser{
			Writer: entryWriter,
			closeFunc: func() error {
				logger.Debug("Finalizing zip archive: %s", fixedPath)
				var err error
				if cerr := zipWriter.Close(); cerr != nil {
					err = cerr
				}
				if ferr := file.Close(); ferr != nil && err == nil {
					err = ferr
				}
				logger.Debug("ZIP file closed successfully in %v", time.Since(start))
				return err
			},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported compression: %s", options.Compression)
	}
}

func determineZipEntryName(outputPath, format string) string {
	base := filepath.Base(outputPath)
	lowerBase := strings.ToLower(base)

	name := strings.TrimSuffix(lowerBase, ".zip")

	if name == "" {
		name = "export"
	}

	if !strings.HasSuffix(name, "."+format) && format != FormatTemplate {
		name = fmt.Sprintf("%s.%s", name, format)
	}

	return name
}

func fixExtension(path, extention string) string {
	ext := filepath.Ext(path)

	if strings.ToLower(ext) != extention {
		path = path[:len(path)-len(ext)] + extention
	}
	return path
}
