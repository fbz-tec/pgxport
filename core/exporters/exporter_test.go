package exporters

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestExportFailureRemovesPartialFile checks that every format reports a failure
// that happens mid-export and leaves no partial file behind.
func TestExportFailureRemovesPartialFile(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	rowTpl := filepath.Join(t.TempDir(), "row.tpl")
	if err := os.WriteFile(rowTpl, []byte("{{get . \"g\"}}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Row 3 divides by zero, after rows 1 and 2 were already written.
	query := "SELECT g, 1/(3-g) AS x FROM generate_series(1, 5) g"

	for _, format := range []string{FormatCSV, FormatJSON, FormatXML, FormatYAML, FormatSQL, FormatXLSX, FormatTemplate} {
		t.Run(format, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "out."+format)

			rows, err := conn.Query(context.Background(), query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(format)
			if err != nil {
				t.Fatalf("Failed to get exporter: %v", err)
			}
			_, err = exporter.Export(rows, ExportOptions{
				Format:            format,
				OutputPath:        outputPath,
				Compression:       "none",
				Delimiter:         ',',
				TableName:         "t",
				RowPerStatement:   1,
				XmlRootElement:    "results",
				XmlRowElement:     "row",
				TimeFormat:        "yyyy-MM-dd HH:mm:ss",
				TemplateRow:       rowTpl,
				TemplateStreaming: true,
			})
			if err == nil {
				t.Fatal("Export() should fail on division by zero")
			}
			if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
				t.Errorf("partial file %s should have been removed", outputPath)
			}
		})
	}
}

// TestExportCloseErrorIsReported writes a small export to /dev/full, where every write
// fails with "no space left on device". The rows fit in the write buffer, so the only
// failing write is the final flush in Close: it must still fail the export.
func TestExportCloseErrorIsReported(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/dev/full only exists on Linux")
	}
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	query := "SELECT g AS id FROM generate_series(1, 10) g"
	options := ExportOptions{Format: FormatCSV, OutputPath: "/dev/full", Compression: "none", Delimiter: ','}

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, _ := Get(FormatCSV)
	if _, err := exporter.Export(rows, options); err == nil {
		t.Error("Export() to a full disk should fail")
	}
	rows.Close()

	if _, err := exporter.(CopyCapable).ExportCopy(conn, query, options); err == nil {
		t.Error("ExportCopy() to a full disk should fail")
	}

	if _, err := os.Stat("/dev/full"); err != nil {
		t.Errorf("/dev/full must not be removed: %v", err)
	}
}
