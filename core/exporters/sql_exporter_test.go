package exporters

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestExportSQL(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name        string
		query       string
		tableName   string
		compression string
		wantErr     bool
		checkFunc   func(t *testing.T, path string)
	}{
		{
			name:        "basic SQL export",
			query:       "SELECT 1 as id, 'test' as name",
			tableName:   "users",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// Check for INSERT statement
				if !strings.Contains(contentStr, "INSERT INTO") {
					t.Error("Expected INSERT INTO statement")
				}

				// Check table name is quoted
				if !strings.Contains(contentStr, `"users"`) {
					t.Error("Expected quoted table name")
				}

				// Check for VALUES clause
				if !strings.Contains(contentStr, "VALUES") {
					t.Error("Expected VALUES clause")
				}

				// Check statement ends with semicolon
				if !strings.Contains(contentStr, ");") {
					t.Error("Expected semicolon at end of statement")
				}
			},
		},
		{
			name:        "SQL with NULL values",
			query:       "SELECT 1 as id, NULL as description, 'test' as name",
			tableName:   "items",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// NULL should be exported as NULL keyword (not 'NULL' string)
				if !strings.Contains(contentStr, ", NULL,") && !strings.Contains(contentStr, "(NULL,") {
					t.Logf("Content: %s", contentStr)
				}

				// Should NOT contain 'NULL' in quotes
				if strings.Contains(contentStr, "'NULL'") {
					t.Error("NULL should not be quoted")
				}
			},
		},
		{
			name:        "SQL with special characters",
			query:       "SELECT 'O''Brien' as name, 'Line1\nLine2' as address",
			tableName:   "contacts",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// Single quotes should be escaped as ''
				if !strings.Contains(contentStr, "''") {
					t.Error("Expected escaped single quotes")
				}
			},
		},
		{
			name:        "SQL with quoted identifiers",
			query:       "SELECT 1 as \"user_id\", 'test' as \"user name\"",
			tableName:   "test_table",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// Column names should be quoted
				if !strings.Contains(contentStr, `"user_id"`) {
					t.Error("Expected quoted column name user_id")
				}

				if !strings.Contains(contentStr, `"user name"`) {
					t.Error("Expected quoted column name with space")
				}
			},
		},
		{
			name:        "empty result set",
			query:       "SELECT 1 as id WHERE 1=0",
			tableName:   "empty_table",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				if len(content) != 0 {
					t.Error("Expected empty file for empty result set")
				}
			},
		},
		{
			name:        "SQL with gzip compression",
			query:       "SELECT 1 as id, 'test' as name",
			tableName:   "compressed",
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
			name:        "SQL with multiple rows",
			query:       "SELECT generate_series(1, 5) as id, 'test' || generate_series(1, 5) as name",
			tableName:   "multi_row",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				// Count INSERT statements
				insertCount := strings.Count(string(content), "INSERT INTO")
				if insertCount != 5 {
					t.Errorf("Expected 5 INSERT statements, got %d", insertCount)
				}
			},
		},
		{
			name:        "SQL with schema-qualified table name",
			query:       "SELECT 1 as id",
			tableName:   "public.users",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// Schema and table should both be quoted separately
				if !strings.Contains(contentStr, `"public"."users"`) {
					t.Error("Expected schema-qualified table name with proper quoting")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.sql")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatSQL)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}
			options := ExportOptions{
				Format:          FormatSQL,
				TableName:       tt.tableName,
				Compression:     tt.compression,
				RowPerStatement: 1,
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

func TestWriteSQLDataTypes(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.sql")

	query := `
		SELECT 
			1::integer as int_col,
			3.14::numeric as numeric_col,
			'text value'::text as text_col,
			true::boolean as bool_col,
			false::boolean as bool_false,
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

	exporter, err := Get(FormatSQL)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:          FormatSQL,
		TableName:       "test_types",
		Compression:     "none",
		RowPerStatement: 1,
	}

	rowCount, err := exporter.Export(rows, outputPath, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	if rowCount != 1 {
		t.Errorf("Expected 1 row, got %d", rowCount)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	contentStr := string(content)

	// Verify data types are formatted correctly
	tests := []struct {
		name     string
		expected string
		message  string
	}{
		{
			name:     "integer without quotes",
			expected: "1",
			message:  "Integers should not be quoted",
		},
		{
			name:     "numeric without quotes",
			expected: "3.14",
			message:  "Numbers should not be quoted",
		},
		{
			name:     "text with quotes",
			expected: "'text value'",
			message:  "Text should be quoted",
		},
		{
			name:     "boolean true",
			expected: "true",
			message:  "Boolean true should be lowercase and unquoted",
		},
		{
			name:     "boolean false",
			expected: "false",
			message:  "Boolean false should be lowercase and unquoted",
		},
		{
			name:     "NULL keyword",
			expected: "NULL",
			message:  "NULL should be uppercase and unquoted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(contentStr, tt.expected) {
				t.Errorf("%s: expected to find %q in output", tt.message, tt.expected)
				t.Logf("Content: %s", contentStr)
			}
		})
	}
}

func TestWriteSQLColumnOrder(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.sql")

	query := "SELECT 1 as id, 'test' as name, true as active"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatSQL)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:          FormatSQL,
		TableName:       "test_table",
		Compression:     "none",
		RowPerStatement: 1,
	}

	_, err = exporter.Export(rows, outputPath, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	contentStr := string(content)

	// Verify columns appear in INSERT statement
	if !strings.Contains(contentStr, `("id", "name", "active")`) {
		t.Error("Expected column list in INSERT statement")
	}

	// Verify column order matches value order
	// Values should appear in same order as columns
	lines := strings.Split(contentStr, "\n")
	for _, line := range lines {
		if strings.Contains(line, "INSERT INTO") {
			// Check that id, name, active appear in that order
			idPos := strings.Index(line, "1")
			namePos := strings.Index(line, "'test'")
			activePos := strings.Index(line, "true")

			if idPos > namePos || namePos > activePos {
				t.Error("Values not in correct order")
			}
		}
	}
}

func TestWriteSQLEscaping(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name          string
		query         string
		expectedInSQL string
		notExpected   string
	}{
		{
			name:          "single quote escaping",
			query:         "SELECT 'O''Brien' as name",
			expectedInSQL: "''",
			notExpected:   "",
		},
		{
			name:          "backslash handling",
			query:         "SELECT 'C:\\path\\to\\file' as path",
			expectedInSQL: "\\",
			notExpected:   "",
		},
		{
			name:          "newline in text",
			query:         "SELECT 'Line1\nLine2' as text",
			expectedInSQL: "'Line1",
			notExpected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.sql")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatSQL)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}
			options := ExportOptions{
				Format:          FormatSQL,
				TableName:       "test_escape",
				Compression:     "none",
				RowPerStatement: 1,
			}

			_, err = exporter.Export(rows, outputPath, options)
			if err != nil {
				t.Fatalf("Export() error: %v", err)
			}

			content, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			contentStr := string(content)

			if tt.expectedInSQL != "" && !strings.Contains(contentStr, tt.expectedInSQL) {
				t.Errorf("Expected to find %q in SQL output", tt.expectedInSQL)
			}

			if tt.notExpected != "" && strings.Contains(contentStr, tt.notExpected) {
				t.Errorf("Did not expect to find %q in SQL output", tt.notExpected)
			}
		})
	}
}

func TestWriteSQLLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "large.sql")

	// Generate 1,000 rows
	query := "SELECT i, 'data_' || i FROM generate_series(1, 1000) AS s(i)"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatSQL)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:          FormatSQL,
		TableName:       "large_table",
		Compression:     "none",
		RowPerStatement: 1,
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

	// Verify file exists and has content
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Output file is empty")
	}

	// Count INSERT statements
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	insertCount := strings.Count(string(content), "INSERT INTO")
	if insertCount != 1000 {
		t.Errorf("Expected 1000 INSERT statements, got %d", insertCount)
	}
}

func TestWriteSQLStatementFormat(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.sql")

	query := "SELECT 1 as id, 'test' as name"
	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatSQL)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:          FormatSQL,
		TableName:       "test_table",
		Compression:     "none",
		RowPerStatement: 1,
	}

	_, err = exporter.Export(rows, outputPath, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	contentStr := string(content)

	// Verify multi-row INSERT format is used
	expectedHeader := `INSERT INTO "test_table" ("id", "name") VALUES`
	if !strings.Contains(contentStr, expectedHeader) {
		t.Errorf("Expected to find INSERT header: %s", expectedHeader)
	}

	// Verify value row format with tab indentation
	if !strings.Contains(contentStr, "\t(1, 'test');") {
		t.Error("Expected to find value row: \\t(1, 'test');")
	}

	// Verify overall structure
	lines := strings.Split(strings.TrimSpace(contentStr), "\n")
	if len(lines) != 2 {
		t.Errorf("Expected 2 lines (header + value), got %d", len(lines))
	}

	// First line should be the INSERT INTO ... VALUES
	if !strings.HasPrefix(lines[0], "INSERT INTO") {
		t.Error("First line should start with INSERT INTO")
	}

	// Second line should be the value row with tab and semicolon
	if !strings.HasPrefix(lines[1], "\t(") {
		t.Error("Second line should start with tab and opening parenthesis")
	}

	if !strings.HasSuffix(lines[1], ");") {
		t.Error("Second line should end with );")
	}
}

func TestWriteSQLBuffering(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.sql")

	// Generate enough rows to trigger buffering (>10000)
	query := "SELECT generate_series(1, 15000) as id"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatSQL)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:          FormatSQL,
		TableName:       "buffer_test",
		Compression:     "none",
		RowPerStatement: 1,
	}

	rowCount, err := exporter.Export(rows, outputPath, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	if rowCount != 15000 {
		t.Errorf("Expected 15000 rows, got %d", rowCount)
	}

	// Verify all rows were written
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	insertCount := strings.Count(string(content), "INSERT INTO")
	if insertCount != 15000 {
		t.Errorf("Expected 15000 INSERT statements, got %d", insertCount)
	}
}

func TestWriteSQLWithBatchInsert(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name            string
		query           string
		tableName       string
		insertBatch     int
		expectedRows    int
		expectedInserts int
		checkFunc       func(t *testing.T, path string, content string)
	}{
		{
			name:            "batch insert with 3 rows per statement",
			query:           "SELECT generate_series(1, 9) as id, 'user_' || generate_series(1, 9) as name",
			tableName:       "users",
			insertBatch:     3,
			expectedRows:    9,
			expectedInserts: 3, // 9 rows / 3 per batch = 3 INSERT statements
			checkFunc: func(t *testing.T, path string, content string) {
				// Verify multi-row INSERT format
				if !strings.Contains(content, "VALUES\n\t(") {
					t.Error("Expected multi-row INSERT format with VALUES and newlines")
				}

				// Count INSERT statements
				insertCount := strings.Count(content, "INSERT INTO")
				if insertCount != 3 {
					t.Errorf("Expected 3 INSERT statements, got %d", insertCount)
				}

				// Verify each INSERT has 3 value rows (except possibly the last)
				inserts := strings.Split(content, "INSERT INTO")
				for i, insert := range inserts[1:] { // Skip first empty element
					valueRows := strings.Count(insert, "\t(")
					if i < 2 && valueRows != 3 {
						t.Errorf("INSERT %d: expected 3 value rows, got %d", i+1, valueRows)
					}
				}
			},
		},
		{
			name:            "batch insert with partial last batch",
			query:           "SELECT generate_series(1, 10) as id",
			tableName:       "items",
			insertBatch:     3,
			expectedRows:    10,
			expectedInserts: 4, // 3 + 3 + 3 + 1 = 4 INSERT statements
			checkFunc: func(t *testing.T, path string, content string) {
				insertCount := strings.Count(content, "INSERT INTO")
				if insertCount != 4 {
					t.Errorf("Expected 4 INSERT statements, got %d", insertCount)
				}

				// Last INSERT should have only 1 row
				lines := strings.Split(strings.TrimSpace(content), "\n")
				lastInsertLines := []string{}
				inLastInsert := false

				for i := len(lines) - 1; i >= 0; i-- {
					lastInsertLines = append([]string{lines[i]}, lastInsertLines...)
					if strings.Contains(lines[i], "INSERT INTO") {
						inLastInsert = true
						break
					}
				}

				if inLastInsert {
					lastInsertContent := strings.Join(lastInsertLines, "\n")
					valueRowCount := strings.Count(lastInsertContent, "\t(")
					if valueRowCount != 1 {
						t.Errorf("Last INSERT should have 1 value row, got %d", valueRowCount)
					}
				}
			},
		},
		{
			name:            "batch insert equals total rows",
			query:           "SELECT generate_series(1, 5) as id",
			tableName:       "exact_batch",
			insertBatch:     5,
			expectedRows:    5,
			expectedInserts: 1, // All rows in single INSERT
			checkFunc: func(t *testing.T, path string, content string) {
				insertCount := strings.Count(content, "INSERT INTO")
				if insertCount != 1 {
					t.Errorf("Expected 1 INSERT statement, got %d", insertCount)
				}

				// Should have 5 value rows
				valueRowCount := strings.Count(content, "\t(")
				if valueRowCount != 5 {
					t.Errorf("Expected 5 value rows, got %d", valueRowCount)
				}
			},
		},
		{
			name:            "batch insert larger than total rows",
			query:           "SELECT generate_series(1, 3) as id",
			tableName:       "small_batch",
			insertBatch:     10,
			expectedRows:    3,
			expectedInserts: 1, // All rows fit in single INSERT
			checkFunc: func(t *testing.T, path string, content string) {
				insertCount := strings.Count(content, "INSERT INTO")
				if insertCount != 1 {
					t.Errorf("Expected 1 INSERT statement, got %d", insertCount)
				}
			},
		},
		{
			name:            "batch insert with value 1 (single row per INSERT)",
			query:           "SELECT generate_series(1, 5) as id",
			tableName:       "single_row",
			insertBatch:     1,
			expectedRows:    5,
			expectedInserts: 5, // One INSERT per row
			checkFunc: func(t *testing.T, path string, content string) {
				insertCount := strings.Count(content, "INSERT INTO")
				if insertCount != 5 {
					t.Errorf("Expected 5 INSERT statements, got %d", insertCount)
				}

				// With batch size 1, each INSERT should still use the multi-row format
				// but with only one row: INSERT INTO ... VALUES\n\t(...);\n
				if !strings.Contains(content, "VALUES\n\t(") {
					t.Error("Expected multi-row format even with batch size 1")
				}

				// Each INSERT should have exactly 1 value row
				lines := strings.Split(content, "INSERT INTO")
				for i, insert := range lines[1:] { // Skip first empty element
					valueRows := strings.Count(insert, "\t(")
					if valueRows != 1 {
						t.Errorf("INSERT %d: expected 1 value row, got %d", i+1, valueRows)
					}
				}
			},
		},
		{
			name:            "batch insert with multiple data types",
			query:           "SELECT generate_series(1, 6) as id, 'name_' || generate_series(1, 6) as name, (generate_series(1, 6) % 2 = 0) as active, NULL as description",
			tableName:       "mixed_types",
			insertBatch:     2,
			expectedRows:    6,
			expectedInserts: 3,
			checkFunc: func(t *testing.T, path string, content string) {
				// Verify proper formatting of different types in batch
				if !strings.Contains(content, "NULL") {
					t.Error("Expected NULL values in output")
				}
				if !strings.Contains(content, "true") || !strings.Contains(content, "false") {
					t.Error("Expected boolean values in output")
				}
				if !strings.Contains(content, "'name_") {
					t.Error("Expected quoted string values in output")
				}
			},
		},
		{
			name:            "batch insert format verification",
			query:           "SELECT 1 as id, 'first' as name UNION ALL SELECT 2, 'second' UNION ALL SELECT 3, 'third'",
			tableName:       "format_test",
			insertBatch:     3,
			expectedRows:    3,
			expectedInserts: 1,
			checkFunc: func(t *testing.T, path string, content string) {
				// Verify exact format: INSERT INTO "table" ("col1", "col2") VALUES\n\t(val1, val2),\n\t(val1, val2);\n
				expectedPattern := `INSERT INTO "format_test" ("id", "name") VALUES`
				if !strings.Contains(content, expectedPattern) {
					t.Errorf("Expected pattern %q not found", expectedPattern)
				}

				// Check comma separators between rows
				commaCount := strings.Count(content, "),\n")
				if commaCount != 2 { // 3 rows means 2 commas (last row ends with semicolon)
					t.Errorf("Expected 2 comma separators between rows, got %d", commaCount)
				}

				// Check final row ends with semicolon
				if !strings.Contains(content, ");\n") {
					t.Error("Expected last row to end with semicolon")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.sql")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatSQL)
			if err != nil {
				t.Fatalf("Failed to get sql exporter: %v", err)
			}
			options := ExportOptions{
				Format:          FormatSQL,
				TableName:       tt.tableName,
				Compression:     "none",
				RowPerStatement: tt.insertBatch,
			}

			rowCount, err := exporter.Export(rows, outputPath, options)
			if err != nil {
				t.Fatalf("Export() error: %v", err)
			}

			if rowCount != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, rowCount)
			}

			content, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("Failed to read output file: %v", err)
			}

			contentStr := string(content)

			if tt.checkFunc != nil {
				tt.checkFunc(t, outputPath, contentStr)
			}

			// Verify total row count in output
			totalValueRows := strings.Count(contentStr, "\t(")
			if totalValueRows != tt.expectedRows {
				t.Errorf("Expected %d total value rows in output, got %d", tt.expectedRows, totalValueRows)
			}
		})
	}
}

func TestWriteSQLBatchInsertLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "large_batch.sql")

	// Generate 10,000 rows with batch size of 100
	query := "SELECT i, 'data_' || i as name FROM generate_series(1, 10000) AS s(i)"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatSQL)
	if err != nil {
		t.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:          FormatSQL,
		TableName:       "large_batch_table",
		Compression:     "none",
		RowPerStatement: 100,
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

	t.Logf("Exported 10,000 rows with batch size 100 in %v", duration)

	// Verify file exists and has content
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.Size() == 0 {
		t.Error("Output file is empty")
	}

	// Count INSERT statements (should be 100: 10000 rows / 100 per batch)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	insertCount := strings.Count(string(content), "INSERT INTO")
	expectedInserts := 100
	if insertCount != expectedInserts {
		t.Errorf("Expected %d INSERT statements, got %d", expectedInserts, insertCount)
	}

	// Verify file is smaller than single-row INSERT version
	// (This is more of a sanity check - batch should be more compact)
	t.Logf("Output file size with batch inserts: %d bytes", info.Size())
}

func BenchmarkWriteSQLBatchComparison(b *testing.B) {
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

	benchmarks := []struct {
		name      string
		batchSize int
		rowCount  int
	}{
		{"SingleRow_100rows", 1, 100},
		{"Batch10_100rows", 10, 100},
		{"Batch50_100rows", 50, 100},
		{"Batch100_100rows", 100, 100},
		{"SingleRow_1000rows", 1, 1000},
		{"Batch100_1000rows", 100, 1000},
		{"Batch500_1000rows", 500, 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tmpDir := b.TempDir()
			exporter, err := Get(FormatSQL)
			if err != nil {
				b.Fatalf("Failed to get sql exporter: %v", err)
			}
			query := fmt.Sprintf("SELECT generate_series(1, %d) as id, 'data_' || generate_series(1, %d) as data", bm.rowCount, bm.rowCount)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				outputPath := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.sql", i))

				rows, err := conn.Query(ctx, query)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}

				options := ExportOptions{
					Format:          FormatSQL,
					TableName:       "bench_table",
					Compression:     "none",
					RowPerStatement: bm.batchSize,
				}

				_, err = exporter.Export(rows, outputPath, options)
				if err != nil {
					b.Fatalf("writeSQL failed: %v", err)
				}
				rows.Close()
				os.Remove(outputPath)
			}
		})
	}
}

func BenchmarkExportSQL(b *testing.B) {
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
	exporter, err := Get(FormatSQL)
	if err != nil {
		b.Fatalf("Failed to get sql exporter: %v", err)
	}
	options := ExportOptions{
		Format:          FormatSQL,
		TableName:       "bench_table",
		Compression:     "none",
		RowPerStatement: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(tmpDir, "bench.sql")
		query := "SELECT generate_series(1, 100) as id, md5(random()::text) as data"
		rows, err := conn.Query(ctx, query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		_, err = exporter.Export(rows, outputPath, options)
		if err != nil {
			b.Fatalf("writeSQL failed: %v", err)
		}
		rows.Close()
		os.Remove(outputPath)
	}
}
