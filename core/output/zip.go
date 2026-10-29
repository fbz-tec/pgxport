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

// newZipWriter creates the archive at path (already ending in .zip, see FinalPath)
// with a single file named entryName inside.
func newZipWriter(path, entryName string) (io.WriteCloser, error) {
	start := time.Now()
	logger.Debug("Creating zip-compressed output file: %s", path)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %w", err)
	}
	zipWriter := zip.NewWriter(file)
	logger.Debug("Creating zip entry: %s", entryName)
	entryWriter, err := zipWriter.Create(entryName)
	if err != nil {
		zipWriter.Close()
		file.Close()
		return nil, fmt.Errorf("error creating zip entry: %w", err)
	}
	return &compositeWriteCloser{
		path:   path,
		Writer: entryWriter,
		closeFunc: func() error {
			logger.Debug("Finalizing zip archive: %s", path)
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

// determineZipEntryName returns the name of the file inside the archive: the output file
// name as the user wrote it, without .zip and with the format extension. Extensions are
// compared case-insensitively, but the name keeps its case (Report_Q1.csv).
func determineZipEntryName(outputPath, format string) string {
	name := filepath.Base(outputPath)
	if strings.HasSuffix(strings.ToLower(name), ".zip") {
		name = name[:len(name)-len(".zip")]
	}

	if name == "" {
		name = "export"
	}

	if !strings.HasSuffix(strings.ToLower(name), "."+format) && format != "template" {
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
