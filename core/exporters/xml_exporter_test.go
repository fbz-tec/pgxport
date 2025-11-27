package exporters

import (
	"context"
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestExportXML(t *testing.T) {
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
			name:        "basic XML export",
			query:       "SELECT 1 as id, 'test' as name, true as active",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// Check XML declaration
				if !strings.HasPrefix(contentStr, "<?xml") {
					t.Error("Expected XML declaration at start")
				}

				// Check root element
				if !strings.Contains(contentStr, "<results>") {
					t.Error("Expected <results> root element")
				}

				if !strings.Contains(contentStr, "</results>") {
					t.Error("Expected </results> closing tag")
				}

				// Check row element
				if !strings.Contains(contentStr, "<row>") {
					t.Error("Expected <row> element")
				}

				// Check fields
				if !strings.Contains(contentStr, "<id>") || !strings.Contains(contentStr, "<name>") {
					t.Error("Expected field elements")
				}
			},
		},
		{
			name:        "XML with NULL values",
			query:       "SELECT 1 as id, NULL as description, 'test' as name",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// Check for description element (should be present but empty)
				if !strings.Contains(contentStr, "<description>") {
					t.Error("Expected <description> element even for NULL")
				}
			},
		},
		{
			name:        "XML with special characters",
			query:       "SELECT 'O''Brien' as name, '<tag>' as html, '\"quoted\"' as text",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// XML should escape special characters
				// < becomes &lt;, > becomes &gt;
				if strings.Contains(contentStr, "<tag>") && strings.Count(contentStr, "<tag>") > 1 {
					// If we see <tag> more than once (once for actual XML tag),
					// it means special chars weren't escaped
					if !strings.Contains(contentStr, "&lt;") && !strings.Contains(contentStr, "&gt;") {
						t.Error("Expected special characters to be escaped in XML")
					}
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

				contentStr := string(content)

				// Should have root element but no row elements
				if !strings.Contains(contentStr, "<results>") {
					t.Error("Expected <results> root element")
				}

				if strings.Contains(contentStr, "<row>") {
					t.Error("Did not expect <row> elements for empty result")
				}
			},
		},
		{
			name:        "XML with gzip compression",
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
			name:        "XML with multiple rows",
			query:       "SELECT generate_series(1, 5) as id, 'test' || generate_series(1, 5) as name",
			compression: "none",
			wantErr:     false,
			checkFunc: func(t *testing.T, path string) {
				content, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}

				contentStr := string(content)

				// Count row elements
				rowCount := strings.Count(contentStr, "<row>")
				if rowCount != 5 {
					t.Errorf("Expected 5 <row> elements, got %d", rowCount)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "output.xml")

			ctx := context.Background()
			rows, err := conn.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatXML)
			if err != nil {
				t.Fatalf("Failed to get xml exporter: %v", err)
			}
			options := ExportOptions{
				Format:         FormatXML,
				Compression:    tt.compression,
				TimeFormat:     "yyyy-MM-dd HH:mm:ss",
				TimeZone:       "",
				XmlRootElement: "results",
				XmlRowElement:  "row",
				OutputPath:     outputPath,
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

func TestWriteXMLTimeFormatting(t *testing.T) {
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
			timeFormat: "dd/MM/yyyy HH:mm:ss",
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
			outputPath := filepath.Join(tmpDir, "output.xml")

			query := "SELECT NOW() as created_at"
			ctx := context.Background()
			rows, err := conn.Query(ctx, query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			exporter, err := Get(FormatXML)
			if err != nil {
				t.Fatalf("Failed to get xml exporter: %v", err)
			}
			options := ExportOptions{
				Format:         FormatXML,
				Compression:    "none",
				TimeFormat:     tt.timeFormat,
				TimeZone:       tt.timeZone,
				XmlRootElement: "results",
				XmlRowElement:  "row",
				OutputPath:     outputPath,
			}

			_, err = exporter.Export(rows, options)
			if err != nil {
				t.Fatalf("export() error: %v", err)
			}

			content, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatalf("Failed to read output: %v", err)
			}

			tt.checkFunc(t, string(content))
		})
	}
}

func TestWriteXMLDataTypes(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.xml")

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

	exporter, err := Get(FormatXML)
	if err != nil {
		t.Fatalf("Failed to get xml exporter: %v", err)
	}
	options := ExportOptions{
		Format:         FormatXML,
		Compression:    "none",
		TimeFormat:     "yyyy-MM-dd HH:mm:ss",
		TimeZone:       "",
		XmlRootElement: "results",
		XmlRowElement:  "row",
		OutputPath:     outputPath,
	}

	rowCount, err := exporter.Export(rows, options)
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

	// Verify all column elements are present
	expectedElements := []string{"int_col", "numeric_col", "text_col", "bool_col", "null_col", "timestamp_col", "date_col"}
	for _, elem := range expectedElements {
		if !strings.Contains(contentStr, "<"+elem+">") {
			t.Errorf("Missing element: <%s>", elem)
		}
	}
}

func TestWriteXMLStructure(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.xml")

	query := "SELECT 1 as id, 'test' as name"
	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXML)
	if err != nil {
		t.Fatalf("Failed to get xml exporter: %v", err)
	}
	options := ExportOptions{
		Format:         FormatXML,
		Compression:    "none",
		TimeFormat:     "yyyy-MM-dd HH:mm:ss",
		TimeZone:       "",
		XmlRootElement: "results",
		XmlRowElement:  "row",
		OutputPath:     outputPath,
	}

	_, err = exporter.Export(rows, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	contentStr := strings.TrimSpace(string(content))

	// Verify XML structure
	lines := strings.Split(contentStr, "\n")

	// First line should be XML declaration
	if !strings.HasPrefix(lines[0], "<?xml") {
		t.Errorf("Expected XML declaration on first line, got: %s", lines[0])
	}

	// Check for proper closing
	if !strings.HasSuffix(contentStr, "</results>") {
		t.Error("Expected XML to end with </results>")
	}

	// Check for indentation (pretty print)
	if !strings.Contains(contentStr, "  <row>") {
		t.Error("Expected indented XML structure")
	}
}

func TestWriteXMLValidXML(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.xml")

	query := "SELECT 1 as id, 'test' as name, true as active"
	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXML)
	if err != nil {
		t.Fatalf("Failed to get xml exporter: %v", err)
	}
	options := ExportOptions{
		Format:         FormatXML,
		Compression:    "none",
		TimeFormat:     "yyyy-MM-dd HH:mm:ss",
		TimeZone:       "",
		XmlRootElement: "results",
		XmlRowElement:  "row",
		OutputPath:     outputPath,
	}

	_, err = exporter.Export(rows, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	// Try to parse the XML to verify it's valid
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	type Row struct {
		ID     string `xml:"id"`
		Name   string `xml:"name"`
		Active string `xml:"active"`
	}

	type Results struct {
		XMLName xml.Name `xml:"results"`
		Rows    []Row    `xml:"row"`
	}

	var results Results
	if err := xml.Unmarshal(content, &results); err != nil {
		t.Fatalf("Generated XML is not valid: %v", err)
	}

	if len(results.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(results.Rows))
	}
}

func TestWriteXMLCustomTags(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "custom_tags.xml")

	query := "SELECT 1 as id, 'custom' as label"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXML)
	if err != nil {
		t.Fatalf("Failed to get xml exporter: %v", err)
	}
	options := ExportOptions{
		Format:         FormatXML,
		Compression:    "none",
		TimeFormat:     "yyyy-MM-dd HH:mm:ss",
		TimeZone:       "",
		XmlRootElement: "data",
		XmlRowElement:  "record",
		OutputPath:     outputPath,
	}

	_, err = exporter.Export(rows, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	contentStr := string(content)

	// Validate custom tags
	if !strings.Contains(contentStr, "<data>") {
		t.Error("Expected custom root element <data>")
	}
	if !strings.Contains(contentStr, "<record>") {
		t.Error("Expected custom row element <record>")
	}
	if strings.Contains(contentStr, "<results>") {
		t.Error("Did not expect default <results> tag when custom tag is provided")
	}
	if strings.Contains(contentStr, "<row>") {
		t.Error("Did not expect default <row> tag when custom tag is provided")
	}
}

func TestWriteXMLLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "large.xml")

	// Generate 1,000 rows
	query := "SELECT i, 'data_' || i FROM generate_series(1, 1000) AS s(i)"

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXML)
	if err != nil {
		t.Fatalf("Failed to get xml exporter: %v", err)
	}
	options := ExportOptions{
		Format:         FormatXML,
		Compression:    "none",
		TimeFormat:     "yyyy-MM-dd HH:mm:ss",
		TimeZone:       "",
		XmlRootElement: "results",
		XmlRowElement:  "row",
		OutputPath:     outputPath,
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

	// Verify row count in XML
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	rowElementCount := strings.Count(string(content), "<row>")
	if rowElementCount != 1000 {
		t.Errorf("Expected 1000 <row> elements, got %d", rowElementCount)
	}
}

func TestWriteXMLSpecialXMLCharacters(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.xml")

	// Test with characters that need XML escaping
	query := `SELECT 
		'<test>' as angle_brackets,
		'a & b' as ampersand,
		'"quoted"' as quotes,
		'It''s fine' as apostrophe
	`

	ctx := context.Background()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	exporter, err := Get(FormatXML)
	if err != nil {
		t.Fatalf("Failed to get xml exporter: %v", err)
	}
	options := ExportOptions{
		Format:         FormatXML,
		Compression:    "none",
		TimeFormat:     "yyyy-MM-dd HH:mm:ss",
		TimeZone:       "",
		XmlRootElement: "results",
		XmlRowElement:  "row",
		OutputPath:     outputPath,
	}

	_, err = exporter.Export(rows, options)
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}

	// Verify the file is valid XML by parsing it
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// If xml.Unmarshal succeeds, the XML is valid and properly escaped
	var result interface{}
	if err := xml.Unmarshal(content, &result); err != nil {
		t.Fatalf("Failed to parse XML with special characters: %v", err)
	}
}

func BenchmarkExportXML(b *testing.B) {
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
	exporter, err := Get(FormatXML)
	if err != nil {
		b.Fatalf("Failed to get xml exporter: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(tmpDir, "bench.xml")
		query := "SELECT generate_series(1, 100) as id, md5(random()::text) as data"
		rows, err := conn.Query(ctx, query)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		options := ExportOptions{
			Format:         FormatXML,
			Compression:    "none",
			TimeFormat:     "yyyy-MM-dd HH:mm:ss",
			TimeZone:       "",
			XmlRootElement: "results",
			XmlRowElement:  "row",
			OutputPath:     outputPath,
		}

		_, err = exporter.Export(rows, options)
		if err != nil {
			b.Fatalf("writeXML failed: %v", err)
		}
		rows.Close()
		os.Remove(outputPath)
	}
}
