package output

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/internal/logger"
)

func newZipWriter(path, format string) (io.WriteCloser, error) {
	start := time.Now()
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
}

func determineZipEntryName(outputPath, format string) string {
	base := filepath.Base(outputPath)
	lowerBase := strings.ToLower(base)

	name := strings.TrimSuffix(lowerBase, ".zip")

	if name == "" {
		name = "export"
	}

	if !strings.HasSuffix(name, "."+format) && format != "template" {
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
