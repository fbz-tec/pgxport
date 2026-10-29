package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadSQLFromFile(t *testing.T) {
	tests := []struct {
		name        string
		fileContent string
		wantErr     bool
		expected    string
	}{
		{
			name:        "valid SQL file",
			fileContent: "SELECT * FROM users WHERE id = 1;",
			wantErr:     false,
			expected:    "SELECT * FROM users WHERE id = 1;",
		},
		{
			name:        "empty file",
			fileContent: "",
			wantErr:     false,
			expected:    "",
		},
		{
			name: "multiline SQL",
			fileContent: `SELECT id, name, email
FROM users
WHERE active = true
ORDER BY name;`,
			wantErr: false,
			expected: `SELECT id, name, email
FROM users
WHERE active = true
ORDER BY name;`,
		},
		{
			name: "SQL with comments",
			fileContent: `-- Get all active users
SELECT * FROM users
WHERE active = true; -- filter by status`,
			wantErr: false,
			expected: `-- Get all active users
SELECT * FROM users
WHERE active = true; -- filter by status`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary file
			tmpFile, err := os.CreateTemp("", "test_*.sql")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			tmpPath := tmpFile.Name()
			defer os.Remove(tmpPath)

			// Write content and close
			if _, err := tmpFile.Write([]byte(tt.fileContent)); err != nil {
				tmpFile.Close()
				t.Fatalf("Failed to write to temp file: %v", err)
			}
			tmpFile.Close()

			// Test reading the file
			result, err := readSQLFromFile(tmpPath)

			if (err != nil) != tt.wantErr {
				t.Errorf("readSQLFromFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expected {
				t.Errorf("readSQLFromFile() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestReadSQLFromFileErrors(t *testing.T) {
	tests := []struct {
		name     string
		filepath string
	}{
		{
			name:     "non-existent file",
			filepath: "/tmp/this_file_does_not_exist_12345.sql",
		},
		{
			name:     "directory instead of file",
			filepath: os.TempDir(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readSQLFromFile(tt.filepath)
			if err == nil {
				t.Error("readSQLFromFile() expected error for invalid path, got nil")
			}
		})
	}
}

func TestReadSQLFromFilePermissions(t *testing.T) {
	// Create a file with no read permissions
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "noperm.sql")

	err := os.WriteFile(tmpFile, []byte("SELECT 1;"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Change permissions to no-read (write-only)
	err = os.Chmod(tmpFile, 0200)
	if err != nil {
		t.Skipf("Cannot change file permissions: %v", err)
	}

	// Restore permissions in cleanup
	defer os.Chmod(tmpFile, 0644)

	_, err = readSQLFromFile(tmpFile)
	if err == nil {
		t.Error("readSQLFromFile() should fail with no-read permissions")
	}
}

func TestReadSQLFromFileLargeFile(t *testing.T) {
	// Test reading a larger SQL file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "large.sql")

	// Create a large SQL content
	var content string
	for i := 0; i < 1000; i++ {
		content += "SELECT * FROM users WHERE id = 1;\n"
	}

	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	result, err := readSQLFromFile(tmpFile)
	if err != nil {
		t.Errorf("readSQLFromFile() unexpected error: %v", err)
	}

	if result != content {
		t.Error("readSQLFromFile() did not return expected content for large file")
	}
}

func TestReadSQLFromFileUTF8(t *testing.T) {
	// Test reading file with UTF-8 characters
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "utf8.sql")

	content := "SELECT * FROM users WHERE name = 'José García éàç';"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	result, err := readSQLFromFile(tmpFile)
	if err != nil {
		t.Errorf("readSQLFromFile() unexpected error: %v", err)
	}

	if result != content {
		t.Errorf("readSQLFromFile() = %q, want %q", result, content)
	}
}

func TestParseDelimiter(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  rune
		wantError bool
	}{
		{
			name:      "comma delimiter",
			input:     ",",
			expected:  ',',
			wantError: false,
		},
		{
			name:      "semicolon delimiter",
			input:     ";",
			expected:  ';',
			wantError: false,
		},
		{
			name:      "pipe delimiter",
			input:     "|",
			expected:  '|',
			wantError: false,
		},
		{
			name:      "tab delimiter",
			input:     `\t`,
			expected:  '\t',
			wantError: false,
		},
		{
			name:      "tab with spaces",
			input:     `  \t  `,
			expected:  '\t',
			wantError: false,
		},
		{
			name:      "space delimiter",
			input:     " ",
			expected:  ' ',
			wantError: true,
		},
		{
			name:      "empty string",
			input:     "",
			wantError: true,
		},
		{
			name:      "multiple characters",
			input:     ",,",
			wantError: true,
		},
		{
			name:      "word instead of delimiter",
			input:     "comma",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseDelimiter(tt.input)

			if tt.wantError {
				if err == nil {
					t.Errorf("parseDelimiter(%q) expected error, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("parseDelimiter(%q) unexpected error: %v", tt.input, err)
				return
			}

			if result != tt.expected {
				t.Errorf("parseDelimiter(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValidateExportParams(t *testing.T) {
	// Save original values
	originalSqlQuery := sqlQuery
	originalSqlFile := sqlFile
	originalFormat := format
	originalCompression := compression
	originalTableName := tableName
	originalTimeFormat := timeFormat
	originalTimeZone := timeZone
	originalXmlRoot, originalXmlRow := xmlRootElement, xmlRowElement

	// Restore original values after test
	defer func() {
		xmlRootElement, xmlRowElement = originalXmlRoot, originalXmlRow
		sqlQuery = originalSqlQuery
		sqlFile = originalSqlFile
		format = originalFormat
		compression = originalCompression
		tableName = originalTableName
		timeFormat = originalTimeFormat
		timeZone = originalTimeZone
	}()

	tests := []struct {
		name        string
		setupFunc   func()
		wantErr     bool
		errContains string
	}{
		{
			name: "valid CSV format",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "csv"
				compression = "none"
				tableName = ""
				timeFormat = "yyyy-MM-dd HH:mm:ss"
				timeZone = ""
			},
			wantErr: false,
		},
		{
			name: "valid JSON format",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "json"
				compression = "none"
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr: false,
		},
		{
			name: "valid XML format",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "xml"
				compression = "gzip"
				tableName = ""
				timeFormat = ""
				timeZone = ""
				xmlRootElement = "results"
				xmlRowElement = "row"
			},
			wantErr: false,
		},
		{
			name: "invalid XML row tag with a space",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "xml"
				compression = "none"
				timeFormat = ""
				timeZone = ""
				xmlRootElement = "results"
				xmlRowElement = "my row"
			},
			wantErr:     true,
			errContains: "--xml-row-tag",
		},
		{
			name: "invalid XML root tag starting with a digit",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "xml"
				compression = "none"
				timeFormat = ""
				timeZone = ""
				xmlRootElement = "1results"
				xmlRowElement = "row"
			},
			wantErr:     true,
			errContains: "--xml-root-tag",
		},
		{
			name: "valid SQL format with table name",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "sql"
				compression = "none"
				tableName = "users_backup"
				timeFormat = ""
				timeZone = ""
				rowPerStatement = 1
			},
			wantErr: false,
		},
		{
			name: "no SQL query or file",
			setupFunc: func() {
				sqlQuery = ""
				sqlFile = ""
				format = "csv"
				compression = "none"
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr:     true,
			errContains: "Either --sql or --sqlfile must be provided",
		},
		{
			name: "both SQL query and file",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = "query.sql"
				format = "csv"
				compression = "none"
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr:     true,
			errContains: "Cannot use both --sql and --sqlfile",
		},
		{
			name: "invalid format",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "txt"
				compression = "none"
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr:     true,
			errContains: "Invalid format",
		},
		{
			name: "SQL format without table name",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "sql"
				compression = "none"
				tableName = ""
				timeFormat = ""
				timeZone = ""
				rowPerStatement = 1
			},
			wantErr:     true,
			errContains: "--table (-t) is required",
		},
		{
			name: "SQL format with whitespace-only table name",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "sql"
				compression = "none"
				tableName = "   "
				timeFormat = ""
				timeZone = ""
				rowPerStatement = 1
			},
			wantErr:     true,
			errContains: "--table (-t) is required",
		},
		{
			name: "SQL format with invalid insert batch",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "sql"
				compression = "none"
				tableName = "users_backup"
				timeFormat = ""
				timeZone = ""
				rowPerStatement = 0
			},
			wantErr:     true,
			errContains: "--insert-batch must be at least 1",
		},
		{
			name: "invalid compression",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "csv"
				compression = "bzip2"
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr:     true,
			errContains: "Invalid compression",
		},
		{
			name: "invalid timezone",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "csv"
				compression = "none"
				tableName = ""
				timeFormat = "yyyy-MM-dd HH:mm:ss"
				timeZone = "Invalid/Timezone"
			},
			wantErr:     true,
			errContains: "Invalid timezone",
		},
		{
			name: "valid timezone UTC",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "csv"
				compression = "none"
				tableName = ""
				timeFormat = "yyyy-MM-dd HH:mm:ss"
				timeZone = "UTC"
			},
			wantErr: false,
		},
		{
			name: "valid timezone America/New_York",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "csv"
				compression = "none"
				tableName = ""
				timeFormat = "yyyy-MM-dd HH:mm:ss"
				timeZone = "America/New_York"
			},
			wantErr: false,
		},
		{
			name: "compression with uppercase",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "csv"
				compression = "GZIP"
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr: false,
		},
		{
			name: "compression with whitespace",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "csv"
				compression = "  zip  "
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr: false,
		},
		{
			name: "format with uppercase",
			setupFunc: func() {
				sqlQuery = "SELECT * FROM users"
				sqlFile = ""
				format = "CSV"
				compression = "none"
				tableName = ""
				timeFormat = ""
				timeZone = ""
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupFunc()
			err := validateExportParams()

			if tt.wantErr {
				if err == nil {
					t.Errorf("validateExportParams() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("validateExportParams() error = %q, should contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("validateExportParams() unexpected error: %v", err)
				}
			}
		})
	}
}

// Benchmark tests
func BenchmarkReadSQLFromFile(b *testing.B) {
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "bench.sql")

	content := "SELECT * FROM users WHERE id = 1;"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = readSQLFromFile(tmpFile)
	}
}

func BenchmarkReadSQLFromFileLarge(b *testing.B) {
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "bench_large.sql")

	// Create a 10KB SQL file
	var content string
	for i := 0; i < 100; i++ {
		content += "SELECT * FROM users WHERE id = 1 AND name LIKE '%test%' ORDER BY created_at DESC LIMIT 100;\n"
	}

	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = readSQLFromFile(tmpFile)
	}
}

func BenchmarkParseDelimiter(b *testing.B) {
	delimiters := []string{",", ";", "|", "\t", " "}

	for _, delim := range delimiters {
		b.Run(delim, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = parseDelimiter(delim)
			}
		})
	}
}

func TestHandleExportResult(t *testing.T) {
	// Save original value
	originalFailOnEmpty := failOnEmpty
	defer func() { failOnEmpty = originalFailOnEmpty }()

	tests := []struct {
		name        string
		rowCount    int
		failOnEmpty bool
		wantErr     bool
		errContains string
	}{
		{
			name:        "success with rows",
			rowCount:    100,
			failOnEmpty: false,
			wantErr:     false,
		},
		{
			name:        "zero rows without fail flag",
			rowCount:    0,
			failOnEmpty: false,
			wantErr:     false,
		},
		{
			name:        "zero rows with fail flag",
			rowCount:    0,
			failOnEmpty: true,
			wantErr:     true,
			errContains: "query returned 0 rows",
		},
		{
			name:        "success with rows and fail flag",
			rowCount:    50,
			failOnEmpty: true,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failOnEmpty = tt.failOnEmpty

			err := handleExportResult(tt.rowCount, "/tmp/test.csv")

			if tt.wantErr {
				if err == nil {
					t.Errorf("handleExportResult() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("handleExportResult() error = %q, should contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("handleExportResult() unexpected error: %v", err)
				}
			}
		})
	}
}
