package exporters

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
)

func TestExportXLSX(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name        string
		query       string
		compression string
		noHeader    bool
		wantErr     bool
		checkFunc   func(t *testing.T, path string)
	}{
		{
			name:        "basic XLSX export",
			query:       "SELECT 1 as id, 'test' as name, true as active",
			compression: "none",
			noHeader:    false,
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				f, err := excelize.OpenFile(path)
				if err != nil {
					t.Fatalf("Failed to open XLSX file: %v", err)
				}
				defer f.Close()

				rows, err := f.GetRows("Sheet1")
				if err != nil {
					t.Fatalf("Failed to get rows: %v", err)
				}

				// Should have header + 1 data row
				if len(rows) != 2 {
					t.Errorf("Expected 2 rows (header + data), got %d", len(rows))
				}

				// Check header
				if len(rows) > 0 {
					if !slices.Contains(rows[0], "id") || !slices.Contains(rows[0], "name") || !slices.Contains(rows[0], "active") {
						t.Errorf("Header missing expected columns: %v", rows[0])
					}
				}

				// Check data
				if len(rows) > 1 {
					if len(rows[1]) != 3 {
						t.Errorf("Expected 3 columns in data row, got %d", len(rows[1]))
					}
				}
			},
		},
		{
			name:        "XLSX without header",
			query:       "SELECT 1 as id, 'test' as name, true as active",
			compression: "none",
			noHeader:    true,
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				f, err := excelize.OpenFile(path)
				if err != nil {
					t.Fatalf("Failed to open XLSX file: %v", err)
				}
				defer f.Close()

				rows, err := f.GetRows("Sheet1")
				if err != nil {
					t.Fatalf("Failed to get rows: %v", err)
				}

				// Should have only 1 data row (no header)
				if len(rows) != 1 {
					t.Errorf("Expected 1 row (data only), got %d", len(rows))
				}

				// First row should be data, not header
				if len(rows) > 0 {
					if rows[0][0] != "1" {
						t.Errorf("First cell should be '1', got %q", rows[0][0])
					}
				}
			},
		},
		{
			name:        "XLSX with NULL values",
			query:       "SELECT 1 as id, NULL as description, 'test' as name",
			compression: "none",
			noHeader:    false,
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				f, err := excelize.OpenFile(path)
				if err != nil {
					t.Fatalf("Failed to open XLSX file: %v", err)
				}
				defer f.Close()

				rows, err := f.GetRows("Sheet1")
				if err != nil {
					t.Fatalf("Failed to get rows: %v", err)
				}

				// Check NULL value (should be empty string)
				if len(rows) > 1 && len(rows[1]) > 1 {
					if rows[1][1] != "" {
						t.Errorf("NULL value should be empty string, got %q", rows[1][1])
					}
				}
			},
		},
		{
			name:        "empty result set",
			query:       "SELECT 1 as id WHERE 1=0",
			compression: "none",
			noHeader:    false,
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				f, err := excelize.OpenFile(path)
				if err != nil {
					t.Fatalf("Failed to open XLSX file: %v", err)
				}
				defer f.Close()

				rows, err := f.GetRows("Sheet1")
				if err != nil {
					t.Fatalf("Failed to get rows: %v", err)
				}

				// Should have only header row
				if len(rows) != 1 {
					t.Errorf("Expected 1 row (header only), got %d", len(rows))
				}
			},
		},
		{
			name:        "XLSX with multiple rows",
			query:       "SELECT generate_series(1, 10) as id, 'test' || generate_series(1, 10) as name",
			compression: "none",
			noHeader:    false,
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				f, err := excelize.OpenFile(path)
				if err != nil {
					t.Fatalf("Failed to open XLSX file: %v", err)
				}
				defer f.Close()

				rows, err := f.GetRows("Sheet1")
				if err != nil {
					t.Fatalf("Failed to get rows: %v", err)
				}

				// Should have header + 10 data rows
				if len(rows) != 11 {
					t.Errorf("Expected 11 rows (header + 10 data), got %d", len(rows))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.xlsx")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatXLSX)
			if err != nil {
				t.Fatalf("Failed to get xlsx exporter: %v", err)
			}

			options := ExportOptions{
				Format:      FormatXLSX,
				Compression: tt.compression,
				TimeFormat:  "yyyy-MM-dd HH:mm:ss",
				TimeZone:    "",
				NoHeader:    tt.noHeader,
			}

			_, err = exporter.Export(rows, outputPath, options)

			if (err != nil) != tt.wantErr {
				t.Errorf("Export() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkFunc != nil {
				tt.checkFunc(t, outputPath)
			}
		})
	}
}

func TestWriteXLSXTimeFormatting(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name       string
		timeFormat string
		timeZone   string
		checkFunc  func(t *testing.T, path string)
	}{
		{
			name:       "default time format",
			timeFormat: "yyyy-MM-dd HH:mm:ss",
			timeZone:   "",
			checkFunc: func(t *testing.T, path string) {
				f, err := excelize.OpenFile(path)
				if err != nil {
					t.Fatalf("Failed to open XLSX: %v", err)
				}
				defer f.Close()

				rows, err := f.GetRows("Sheet1")
				if err != nil {
					t.Fatalf("Failed to get rows: %v", err)
				}

				// Excel should have date as formatted time.Time value
				if len(rows) < 2 {
					t.Fatal("Expected at least 2 rows")
				}

				// Check that timestamp is present (Excel stores as serial number)
				if len(rows[1]) < 1 {
					t.Error("Expected timestamp value")
				}
			},
		},
		{
			name:       "UTC timezone",
			timeFormat: "yyyy-MM-dd HH:mm:ss",
			timeZone:   "UTC",
			checkFunc: func(t *testing.T, path string) {
				f, err := excelize.OpenFile(path)
				if err != nil {
					t.Fatalf("Failed to open XLSX: %v", err)
				}
				defer f.Close()

				// Just verify it doesn't crash and produces valid output
				rows, err := f.GetRows("Sheet1")
				if err != nil {
					t.Fatalf("Failed to get rows: %v", err)
				}

				if len(rows) == 0 {
					t.Error("Expected non-empty output")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.xlsx")

			query := "SELECT NOW() as created_at"
			ctx := context.Background()
			rows, err := conn.Query(ctx, query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatXLSX)
			if err != nil {
				t.Fatalf("Failed to get xlsx exporter: %v", err)
			}

			options := ExportOptions{
				Format:      FormatXLSX,
				Compression: "none",
				TimeFormat:  tt.timeFormat,
				TimeZone:    tt.timeZone,
			}

			_, err = exporter.Export(rows, outputPath, options)
			if err != nil {
				t.Fatalf("Export() error: %v", err)
			}

			tt.checkFunc(t, outputPath)
		})
	}
}

func TestWriteXLSXDataTypes(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.xlsx")

	query := `
		SELECT 
			1::integer as int_col,
			3.14::numeric as numeric_col,
			'text value'::text as text_col,
			true::boolean as bool_col,
			NULL as null_col,
			NOW() as timestamp_col,
			'2024-01-15'::date as date_col
	`

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXLSX)
	if err != nil {
		t.Fatalf("Failed to get xlsx exporter: %v", err)
	}

	options := ExportOptions{
		Format:      FormatXLSX,
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
	}

	rowCount, err := exporter.Export(rows, outputPath, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	if rowCount != 1 {
		t.Errorf("Expected 1 row, got %d", rowCount)
	}

	// Verify file can be opened
	f, err := excelize.OpenFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to open XLSX: %v", err)
	}
	defer f.Close()

	xlsxRows, err := f.GetRows("Sheet1")
	if err != nil {
		t.Fatalf("Failed to get rows: %v", err)
	}

	if len(xlsxRows) != 2 {
		t.Errorf("Expected 2 rows (header + data), got %d", len(xlsxRows))
	}

	// Verify column count
	if len(xlsxRows[0]) != 7 {
		t.Errorf("Expected 7 columns, got %d", len(xlsxRows[0]))
	}
}

func TestWriteXLSXLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "large.xlsx")

	// Generate 1,000 rows
	query := "SELECT i, 'data_' || i as name FROM generate_series(1, 1000) AS s(i)"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXLSX)
	if err != nil {
		t.Fatalf("Failed to get xlsx exporter: %v", err)
	}

	options := ExportOptions{
		Format:      FormatXLSX,
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
	}

	start := time.Now()
	rowCount, err := exporter.Export(rows, outputPath, options)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	if rowCount != 1000 {
		t.Errorf("Expected 1000 rows, got %d", rowCount)
	}

	t.Logf("Exported 1,000 rows in %v", duration)

	// Verify file exists
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Output file is empty")
	}

	// Verify row count
	f, err := excelize.OpenFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to open XLSX: %v", err)
	}
	defer f.Close()

	xlsxRows, err := f.GetRows("Sheet1")
	if err != nil {
		t.Fatalf("Failed to get rows: %v", err)
	}

	// Header + 1000 rows = 1001
	if len(xlsxRows) != 1001 {
		t.Errorf("Expected 1001 rows, got %d", len(xlsxRows))
	}
}

func TestWriteXLSXColumnOrder(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.xlsx")

	query := "SELECT 3 as col_c, 1 as col_a, 2 as col_b"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXLSX)
	if err != nil {
		t.Fatalf("Failed to get xlsx exporter: %v", err)
	}

	options := ExportOptions{
		Format:      FormatXLSX,
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
	}

	_, err = exporter.Export(rows, outputPath, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	// Verify column order is preserved
	f, err := excelize.OpenFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to open XLSX: %v", err)
	}
	defer f.Close()

	xlsxRows, err := f.GetRows("Sheet1")
	if err != nil {
		t.Fatalf("Failed to get rows: %v", err)
	}

	// Check header order
	if len(xlsxRows) < 1 {
		t.Fatal("Expected header row")
	}

	header := xlsxRows[0]
	expectedOrder := []string{"col_c", "col_a", "col_b"}

	for i, expected := range expectedOrder {
		if i >= len(header) || header[i] != expected {
			t.Errorf("Column %d: expected %q, got %q", i, expected, header[i])
		}
	}

	// Check data order
	if len(xlsxRows) < 2 {
		t.Fatal("Expected data row")
	}

	dataRow := xlsxRows[1]
	expectedValues := []string{"3", "1", "2"}

	for i, expected := range expectedValues {
		if i >= len(dataRow) || dataRow[i] != expected {
			t.Errorf("Data column %d: expected %q, got %q", i, expected, dataRow[i])
		}
	}
}

func BenchmarkExportXLSX(b *testing.B) {
	testURL := os.Getenv("DB_TEST_URL")
	if testURL == "" {
		b.Skip("Skipping benchmark: DB_TEST_URL not set")
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, testURL)
	if err != nil {
		b.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	tmpDir := b.TempDir()
	exporter, err := Get(FormatXLSX)
	if err != nil {
		b.Fatalf("Failed to get xlsx exporter: %v", err)
	}

	options := ExportOptions{
		Format:      FormatXLSX,
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(tmpDir, "bench.xlsx")
		query := "SELECT generate_series(1, 100) as id, md5(random()::text) as data"
		rows, err := conn.Query(ctx, query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		_, err = exporter.Export(rows, outputPath, options)
		if err != nil {
			b.Fatalf("Export failed: %v", err)
		}
		rows.Close()
		os.Remove(outputPath)
	}
}
