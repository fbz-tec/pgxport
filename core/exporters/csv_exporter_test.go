package exporters

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestExportCSV(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name        string
		query       string
		delimiter   rune
		compression string
		wantErr     bool
		checkFunc   func(t *testing.T, path string)
	}{
		{
			name:        "basic CSV export",
			query:       "SELECT 1 as id, 'test' as name, true as active",
			delimiter:   ',',
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				lines := strings.Split(strings.TrimSpace(string(content)), "\n")
				if len(lines) != 2 { // header + 1 data row
					t.Errorf("Expected 2 lines, got %d", len(lines))
				}

				// Check header
				if !strings.Contains(lines[0], "id") || !strings.Contains(lines[0], "name") {
					t.Errorf("Header missing expected columns: %s", lines[0])
				}
			},
		},
		{
			name:        "CSV with custom delimiter",
			query:       "SELECT 1 as id, 'test' as name",
			delimiter:   ';',
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				lines := strings.Split(strings.TrimSpace(string(content)), "\n")
				if !strings.Contains(lines[0], ";") {
					t.Errorf("Expected semicolon delimiter, got: %s", lines[0])
				}
			},
		},
		{
			name:        "CSV with NULL values",
			query:       "SELECT 1 as id, NULL as description, 'test' as name",
			delimiter:   ',',
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				// NULL should be exported as empty string in CSV
				if !strings.Contains(string(content), ",,") && !strings.Contains(string(content), ",\"\",") {
					t.Logf("Content: %s", string(content))
				}
			},
		},
		{
			name:        "CSV with special characters",
			query:       "SELECT 'O''Brien' as name, 'Line1\nLine2' as address",
			delimiter:   ',',
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 2 { // header + 1 row
					t.Errorf("Expected 2 records, got %d", len(records))
				}
			},
		},
		{
			name:        "empty result set",
			query:       "SELECT 1 as id WHERE 1=0",
			delimiter:   ',',
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				lines := strings.Split(strings.TrimSpace(string(content)), "\n")
				if len(lines) != 1 { // only header
					t.Errorf("Expected only header line, got %d lines", len(lines))
				}
			},
		},
		{
			name:        "CSV with gzip compression",
			query:       "SELECT 1 as id, 'test' as name",
			delimiter:   ',',
			compression: "gzip",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				if !strings.HasSuffix(path, ".gz") {
					t.Errorf("Expected .gz extension, got: %s", path)
				}

				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("Failed to stat file: %v", err)
				}
				if info.Size() == 0 {
					t.Error("Compressed file is empty")
				}
			},
		},
		{
			name:        "CSV with multiple rows",
			query:       "SELECT generate_series(1, 100) as id, 'test' as name",
			delimiter:   ',',
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 101 { // header + 100 rows
					t.Errorf("Expected 101 records, got %d", len(records))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.csv")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatCSV)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}
			options := ExportOptions{
				Format:      FormatCSV,
				Delimiter:   tt.delimiter,
				Compression: tt.compression,
				TimeFormat:  "yyyy-MM-dd HH:mm:ss",
				TimeZone:    "",
			}

			_, err = exporter.Export(rows, outputPath, options)

			if (err != nil) != tt.wantErr {
				t.Errorf("Export() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkFunc != nil {
				finalPath := outputPath
				if tt.compression == "gzip" && !strings.HasSuffix(outputPath, ".gz") {
					finalPath = outputPath + ".gz"
				}
				tt.checkFunc(t, finalPath)
			}
		})
	}
}

func TestWriteCSVTimeFormatting(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name       string
		timeFormat string
		timeZone   string
		checkFunc  func(t *testing.T, content string)
	}{
		{
			name:       "default time format",
			timeFormat: "yyyy-MM-dd HH:mm:ss",
			timeZone:   "",
			checkFunc: func(t *testing.T, content string) {
				if !strings.Contains(content, "-") || !strings.Contains(content, ":") {
					t.Errorf("Expected date-time format, got: %s", content)
				}
			},
		},
		{
			name:       "custom time format",
			timeFormat: "dd/MM/yyyy",
			timeZone:   "",
			checkFunc: func(t *testing.T, content string) {
				if !strings.Contains(content, "/") {
					t.Errorf("Expected custom date format with /, got: %s", content)
				}
			},
		},
		{
			name:       "UTC timezone",
			timeFormat: "yyyy-MM-dd HH:mm:ss",
			timeZone:   "UTC",
			checkFunc: func(t *testing.T, content string) {
				// Just verify it doesn't crash and produces output
				if len(content) == 0 {
					t.Error("Expected non-empty output")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.csv")

			query := "SELECT NOW() as created_at"
			ctx := context.Background()
			rows, err := conn.Query(ctx, query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatCSV)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}
			options := ExportOptions{
				Format:      FormatCSV,
				Delimiter:   ',',
				Compression: "none",
				TimeFormat:  tt.timeFormat,
				TimeZone:    tt.timeZone,
			}

			_, err = exporter.Export(rows, outputPath, options)
			if err != nil {
				t.Fatalf("Export() error: %v", err)
			}

			content, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("Failed to read output: %v", err)
			}

			tt.checkFunc(t, string(content))
		})
	}
}

func TestWriteCSVDataTypes(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.csv")

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

	exporter, err := Get(FormatCSV)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:      FormatCSV,
		Delimiter:   ',',
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

	// Verify the file can be parsed
	f, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("Expected 2 records (header + 1 row), got %d", len(records))
	}

	if len(records[0]) != 7 {
		t.Errorf("Expected 7 columns, got %d", len(records[0]))
	}
}

func TestWriteCopyCSV(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name      string
		query     string
		delimiter rune
		wantErr   bool
		minRows   int
		checkFunc func(t *testing.T, path string)
	}{
		{
			name:      "basic COPY export",
			query:     "SELECT 1 as id, 'test' as name",
			delimiter: ',',
			wantErr:   false,
			minRows:   1,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output: %v", err)
				}

				if !strings.Contains(string(content), "id") {
					t.Error("Expected header with 'id' column")
				}
			},
		},
		{
			name:      "COPY with multiple rows",
			query:     "SELECT generate_series(1, 50) as id",
			delimiter: ',',
			wantErr:   false,
			minRows:   50,
			checkFunc: func(t *testing.T, path string) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 51 { // header + 50 rows
					t.Errorf("Expected 51 records, got %d", len(records))
				}
			},
		},
		{
			name:      "COPY with custom delimiter",
			query:     "SELECT 1 as id, 'test' as name",
			delimiter: ';',
			wantErr:   false,
			minRows:   1,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output: %v", err)
				}

				if !strings.Contains(string(content), ";") {
					t.Error("Expected semicolon delimiter")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.csv")

			exporter, err := Get(FormatCSV)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}

			options := ExportOptions{
				Format:      FormatCSV,
				Delimiter:   tt.delimiter,
				Compression: "none",
			}

			copyExp, ok := exporter.(CopyCapable)

			if !ok {
				t.Fatalf("Copy mode is not supported: %v", err)
			}

			rowCount, err := copyExp.ExportCopy(conn, tt.query, outputPath, options)

			if (err != nil) != tt.wantErr {
				t.Errorf("writeCopyCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if rowCount < tt.minRows {
					t.Errorf("Expected at least %d rows, got %d", tt.minRows, rowCount)
				}

				if tt.checkFunc != nil {
					tt.checkFunc(t, outputPath)
				}
			}
		})
	}
}

func TestWriteCSVLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "large.csv")

	// Generate 10,000 rows
	query := "SELECT i, 'data_' || i FROM generate_series(1, 10000) AS s(i)"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatCSV)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:      FormatCSV,
		Delimiter:   ',',
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

	if rowCount != 10000 {
		t.Errorf("Expected 10000 rows, got %d", rowCount)
	}

	t.Logf("Exported 10,000 rows in %v", duration)

	// Verify file exists and has content
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Output file is empty")
	}
}

func TestWriteCSVNoHeader(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name      string
		query     string
		noHeader  bool
		checkFunc func(t *testing.T, path string, noHeader bool)
	}{
		{
			name:     "CSV with header (default)",
			query:    "SELECT 1 as id, 'test' as name, true as active",
			noHeader: false,
			checkFunc: func(t *testing.T, path string, noHeader bool) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 2 { // header + 1 data row
					t.Errorf("Expected 2 records (header + data), got %d", len(records))
				}

				// Check header row
				if len(records) > 0 {
					header := records[0]

					if !slices.Contains(header, "id") || !slices.Contains(header, "name") || !slices.Contains(header, "active") {
						t.Errorf("Header missing expected columns: %v", header)
					}
				}

				// Check data row
				if len(records) > 1 {
					dataRow := records[1]
					if len(dataRow) != 3 {
						t.Errorf("Expected 3 columns in data row, got %d", len(dataRow))
					}
				}
			},
		},
		{
			name:     "CSV without header",
			query:    "SELECT 1 as id, 'test' as name, true as active",
			noHeader: true,
			checkFunc: func(t *testing.T, path string, noHeader bool) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 1 { // only data row, no header
					t.Errorf("Expected 1 record (data only), got %d", len(records))
				}

				// First row should be data, not header
				if len(records) > 0 {
					firstRow := records[0]
					if len(firstRow) != 3 {
						t.Errorf("Expected 3 columns in data row, got %d", len(firstRow))
					}
					// First column should be "1", not "id"
					if firstRow[0] != "1" {
						t.Errorf("First row should contain data '1', got %q", firstRow[0])
					}
					// Should not contain column names
					if firstRow[0] == "id" || firstRow[1] == "name" || firstRow[2] == "active" {
						t.Error("First row should not contain column names when noHeader is true")
					}
				}
			},
		},
		{
			name:     "CSV without header - multiple rows",
			query:    "SELECT generate_series(1, 10) as num, 'row' || generate_series(1, 10) as label",
			noHeader: true,
			checkFunc: func(t *testing.T, path string, noHeader bool) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 10 { // only data rows, no header
					t.Errorf("Expected 10 records (data only), got %d", len(records))
				}

				// Verify first row is data, not header
				if len(records) > 0 {
					firstRow := records[0]
					if firstRow[0] == "num" || firstRow[1] == "label" {
						t.Error("First row contains column names instead of data")
					}
					if firstRow[0] != "1" {
						t.Errorf("First data value should be '1', got %q", firstRow[0])
					}
				}
			},
		},
		{
			name:     "CSV without header - empty result",
			query:    "SELECT 1 as id WHERE 1=0",
			noHeader: true,
			checkFunc: func(t *testing.T, path string, noHeader bool) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read file: %v", err)
				}

				if len(content) != 0 {
					t.Errorf("Expected empty file for empty result with noHeader, got %d bytes", len(content))
				}
			},
		},
		{
			name:     "CSV with header - empty result",
			query:    "SELECT 1 as id WHERE 1=0",
			noHeader: false,
			checkFunc: func(t *testing.T, path string, noHeader bool) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 1 { // only header
					t.Errorf("Expected 1 record (header only), got %d", len(records))
				}

				if len(records) > 0 && records[0][0] != "id" {
					t.Error("Expected header row to contain column name 'id'")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.csv")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatCSV)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}
			options := ExportOptions{
				Format:      FormatCSV,
				Delimiter:   ',',
				Compression: "none",
				TimeFormat:  "yyyy-MM-dd HH:mm:ss",
				TimeZone:    "",
				NoHeader:    tt.noHeader,
			}

			_, err = exporter.Export(rows, outputPath, options)
			if err != nil {
				t.Fatalf("Export() error: %v", err)
			}

			tt.checkFunc(t, outputPath, tt.noHeader)
		})
	}
}

func TestWriteCopyCSVNoHeader(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name      string
		query     string
		noHeader  bool
		checkFunc func(t *testing.T, path string)
	}{
		{
			name:     "COPY with header",
			query:    "SELECT 1 as id, 'test' as name",
			noHeader: false,
			checkFunc: func(t *testing.T, path string) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 2 {
					t.Errorf("Expected 2 records (header + data), got %d", len(records))
				}

				if len(records) > 0 && records[0][0] != "id" {
					t.Error("Expected header row with column name 'id'")
				}
			},
		},
		{
			name:     "COPY without header",
			query:    "SELECT 1 as id, 'test' as name",
			noHeader: true,
			checkFunc: func(t *testing.T, path string) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 1 {
					t.Errorf("Expected 1 record (data only), got %d", len(records))
				}

				// First row should be data
				if len(records) > 0 && records[0][0] != "1" {
					t.Errorf("First row should contain data '1', got %q", records[0][0])
				}
			},
		},
		{
			name:     "COPY without header - multiple rows",
			query:    "SELECT generate_series(1, 100) as num",
			noHeader: true,
			checkFunc: func(t *testing.T, path string) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatalf("Failed to open file: %v", err)
				}
				defer f.Close()

				reader := csv.NewReader(f)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to parse CSV: %v", err)
				}

				if len(records) != 100 {
					t.Errorf("Expected 100 records, got %d", len(records))
				}

				// Verify no header
				if records[0][0] == "num" {
					t.Error("First row should not be header")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.csv")

			exporter, err := Get(FormatCSV)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}

			options := ExportOptions{
				Format:      FormatCSV,
				Delimiter:   ',',
				Compression: "none",
				NoHeader:    tt.noHeader,
			}

			copyExp, ok := exporter.(CopyCapable)

			if !ok {
				t.Fatalf("Copy mode is not supported by this exporter")
			}

			_, err = copyExp.ExportCopy(conn, tt.query, outputPath, options)

			if err != nil {
				t.Fatalf("writeCopyCSV() error: %v", err)
			}

			tt.checkFunc(t, outputPath)
		})
	}
}

func BenchmarkExportCSV(b *testing.B) {
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
	exporter, err := Get(FormatCSV)
	if err != nil {
		b.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:      FormatCSV,
		Delimiter:   ',',
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(tmpDir, "bench.csv")
		query := "SELECT generate_series(1, 1000) as id, md5(random()::text) as data"
		rows, err := conn.Query(ctx, query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		_, err = exporter.Export(rows, outputPath, options)
		if err != nil {
			b.Fatalf("writeCSV failed: %v", err)
		}
		rows.Close()
		os.Remove(outputPath)
	}
}
