package exporters

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestExportJSON(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name        string
		query       string
		compression string
		wantErr     bool
		checkFunc   func(t *testing.T, path string)
	}{
		{
			name:        "basic JSON export",
			query:       "SELECT 1 as id, 'test' as name, true as active",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				var result []map[string]interface{}
				if err := json.Unmarshal(content, &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}

				if len(result) != 1 {
					t.Errorf("Expected 1 record, got %d", len(result))
				}

				if result[0]["id"] == nil {
					t.Error("Missing 'id' field")
				}
			},
		},
		{
			name:        "JSON with NULL values",
			query:       "SELECT 1 as id, NULL as description, 'test' as name",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				var result []map[string]interface{}
				if err := json.Unmarshal(content, &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}

				if len(result) != 1 {
					t.Errorf("Expected 1 record, got %d", len(result))
				}

				// NULL should be preserved as nil in JSON
				if result[0]["description"] != nil {
					t.Errorf("Expected nil for NULL value, got %v", result[0]["description"])
				}
			},
		},
		{
			name:        "JSON with special characters",
			query:       "SELECT 'O''Brien' as name, 'Line1\nLine2' as address, '<tag>' as html",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				var result []map[string]interface{}
				if err := json.Unmarshal(content, &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}

				if len(result) != 1 {
					t.Errorf("Expected 1 record, got %d", len(result))
				}

				// Check that special characters are properly encoded
				name, ok := result[0]["name"].(string)
				if !ok || !strings.Contains(name, "O'Brien") {
					t.Errorf("Expected O'Brien in name, got %v", result[0]["name"])
				}
			},
		},
		{
			name:        "empty result set",
			query:       "SELECT 1 as id WHERE 1=0",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				var result []map[string]interface{}
				if err := json.Unmarshal(content, &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}

				if len(result) != 0 {
					t.Errorf("Expected empty array, got %d records", len(result))
				}
			},
		},
		{
			name:        "JSON with gzip compression",
			query:       "SELECT 1 as id, 'test' as name",
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
			name:        "JSON with multiple rows",
			query:       "SELECT generate_series(1, 10) as id, 'test' || generate_series(1, 10) as name",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				var result []map[string]interface{}
				if err := json.Unmarshal(content, &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}

				if len(result) != 10 {
					t.Errorf("Expected 10 records, got %d", len(result))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.json")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatJSON)
			if err != nil {
				t.Fatalf("Failed to get json exporter: %v", err)
			}
			options := ExportOptions{
				Format:      FormatJSON,
				Compression: tt.compression,
				TimeFormat:  "yyyy-MM-dd HH:mm:ss",
				TimeZone:    "",
				OutputPath:  outputPath,
			}

			_, err = exporter.Export(rows, options)

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

func TestWriteJSONTimeFormatting(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name       string
		timeFormat string
		timeZone   string
		checkFunc  func(t *testing.T, result []map[string]interface{})
	}{
		{
			name:       "default time format",
			timeFormat: "yyyy-MM-dd HH:mm:ss",
			timeZone:   "",
			checkFunc: func(t *testing.T, result []map[string]interface{}) {
				if len(result) != 1 {
					t.Fatalf("Expected 1 record, got %d", len(result))
				}

				createdAt, ok := result[0]["created_at"].(string)
				if !ok {
					t.Errorf("created_at should be string, got %T", result[0]["created_at"])
				}

				if !strings.Contains(createdAt, "-") || !strings.Contains(createdAt, ":") {
					t.Errorf("Expected date-time format, got: %s", createdAt)
				}
			},
		},
		{
			name:       "custom time format",
			timeFormat: "dd/MM/yyyy HH:mm:ss",
			timeZone:   "",
			checkFunc: func(t *testing.T, result []map[string]interface{}) {
				if len(result) != 1 {
					t.Fatalf("Expected 1 record, got %d", len(result))
				}

				createdAt, ok := result[0]["created_at"].(string)
				if !ok {
					t.Errorf("created_at should be string, got %T", result[0]["created_at"])
				}

				if !strings.Contains(createdAt, "/") {
					t.Errorf("Expected custom date format with /, got: %s", createdAt)
				}
			},
		},
		{
			name:       "UTC timezone",
			timeFormat: "yyyy-MM-dd HH:mm:ss",
			timeZone:   "UTC",
			checkFunc: func(t *testing.T, result []map[string]interface{}) {
				if len(result) != 1 {
					t.Fatalf("Expected 1 record, got %d", len(result))
				}

				// Just verify it doesn't crash and produces valid output
				if result[0]["created_at"] == nil {
					t.Error("created_at should not be nil")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.json")

			query := "SELECT NOW() as created_at"
			ctx := context.Background()
			rows, err := conn.Query(ctx, query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatJSON)
			if err != nil {
				t.Fatalf("Failed to get json exporter: %v", err)
			}
			options := ExportOptions{
				Format:      FormatJSON,
				Compression: "none",
				TimeFormat:  tt.timeFormat,
				TimeZone:    tt.timeZone,
				OutputPath:  outputPath,
			}

			_, err = exporter.Export(rows, options)
			if err != nil {
				t.Fatalf("Export() error: %v", err)
			}

			content, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("Failed to read output: %v", err)
			}

			var result []map[string]interface{}
			if err := json.Unmarshal(content, &result); err != nil {
				t.Fatalf("Failed to parse JSON: %v", err)
			}

			tt.checkFunc(t, result)
		})
	}
}

func TestWriteJSONDataTypes(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	query := `
		SELECT 
			1::integer as int_col,
			3.14::numeric as numeric_col,
			'text value'::text as text_col,
			true::boolean as bool_col,
			NULL as null_col,
			NOW() as timestamp_col,
			'2024-01-15'::date as date_col,
			ARRAY[1,2,3] as array_col
	`

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatJSON)
	if err != nil {
		t.Fatalf("Failed to get json exporter: %v", err)
	}
	options := ExportOptions{
		Format:      FormatJSON,
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
		OutputPath:  outputPath,
	}

	rowCount, err := exporter.Export(rows, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	if rowCount != 1 {
		t.Errorf("Expected 1 row, got %d", rowCount)
	}

	// Verify the file can be parsed
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(content, &result); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 record, got %d", len(result))
	}

	record := result[0]

	// Verify field existence
	expectedFields := []string{"int_col", "numeric_col", "text_col", "bool_col", "null_col", "timestamp_col", "date_col", "array_col"}
	for _, field := range expectedFields {
		if _, ok := record[field]; !ok {
			t.Errorf("Missing field: %s", field)
		}
	}

	// Verify NULL is preserved
	if record["null_col"] != nil {
		t.Errorf("Expected nil for null_col, got %v", record["null_col"])
	}

	// Verify boolean
	if boolVal, ok := record["bool_col"].(bool); !ok || !boolVal {
		t.Errorf("Expected true for bool_col, got %v", record["bool_col"])
	}
}

func TestWriteJSONPrettyPrint(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	query := "SELECT 1 as id, 'test' as name"
	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatJSON)
	if err != nil {
		t.Fatalf("Failed to get json exporter: %v", err)
	}
	options := ExportOptions{
		Format:      FormatJSON,
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
		OutputPath:  outputPath,
	}

	_, err = exporter.Export(rows, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// Check for indentation (pretty print)
	contentStr := string(content)
	if !strings.Contains(contentStr, "  ") { // 2-space indentation
		t.Error("Expected pretty-printed JSON with indentation")
	}

	// Should start with [ and end with ]
	contentStr = strings.TrimSpace(contentStr)
	if !strings.HasPrefix(contentStr, "[") {
		t.Error("Expected JSON to start with [")
	}
	if !strings.HasSuffix(contentStr, "]") {
		t.Error("Expected JSON to end with ]")
	}
}

func TestWriteJSONLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "large.json")

	// Generate 1,000 rows
	query := "SELECT i, 'data_' || i FROM generate_series(1, 1000) AS s(i)"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatJSON)
	if err != nil {
		t.Fatalf("Failed to get json exporter: %v", err)
	}
	options := ExportOptions{
		Format:      FormatJSON,
		Compression: "none",
		TimeFormat:  "yyyy-MM-dd HH:mm:ss",
		TimeZone:    "",
		OutputPath:  outputPath,
	}

	start := time.Now()
	rowCount, err := exporter.Export(rows, options)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	if rowCount != 1000 {
		t.Errorf("Expected 1000 rows, got %d", rowCount)
	}

	t.Logf("Exported 1,000 rows in %v", duration)

	// Verify file exists and has content
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Output file is empty")
	}

	// Verify it's valid JSON
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(content, &result); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(result) != 1000 {
		t.Errorf("Expected 1000 records in parsed JSON, got %d", len(result))
	}
}

func BenchmarkExportJSON(b *testing.B) {
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
	exporter, err := Get(FormatJSON)
	if err != nil {
		b.Fatalf("Failed to get json exporter: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(tmpDir, "bench.json")
		query := "SELECT generate_series(1, 100) as id, md5(random()::text) as data"
		rows, err := conn.Query(ctx, query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		options := ExportOptions{
			Format:      FormatJSON,
			Compression: "none",
			TimeFormat:  "yyyy-MM-dd HH:mm:ss",
			TimeZone:    "",
			OutputPath:  outputPath,
		}

		_, err = exporter.Export(rows, options)
		if err != nil {
			b.Fatalf("writeJSON failed: %v", err)
		}
		rows.Close()
		os.Remove(outputPath)
	}
}
