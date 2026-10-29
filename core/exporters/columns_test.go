package exporters

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/yaml.v3"
)

func TestUniqueColumnNames(t *testing.T) {
	tests := []struct {
		in   []string
		want []string
	}{
		{[]string{"id", "name"}, []string{"id", "name"}},
		{[]string{"id", "total", "id", "name"}, []string{"id", "total", "id_2", "name"}},
		{[]string{"id", "id", "id"}, []string{"id", "id_2", "id_3"}},
		{[]string{"id", "id", "id_2"}, []string{"id", "id_3", "id_2"}},
	}
	for _, tt := range tests {
		fields := make([]pgconn.FieldDescription, len(tt.in))
		for i, name := range tt.in {
			fields[i].Name = name
		}
		if got := uniqueColumnNames(fields); !slices.Equal(got, tt.want) {
			t.Errorf("uniqueColumnNames(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

// TestDuplicateColumnNamesKeepAllValues exports a JOIN where two columns are named "id".
// The second "id" used to overwrite the first one in JSON, YAML and templates.
func TestDuplicateColumnNamesKeepAllValues(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	query := `SELECT o.id, o.total, c.id, c.name
		FROM (VALUES (101, 49.90, 7)) o(id, total, customer_id)
		JOIN (VALUES (7, 'Alice')) c(id, name) ON c.id = o.customer_id`
	// total is compared after reading back as a number, so 49.90 reads as 49.9
	want := map[string]string{"id": "101", "total": "49.9", "id_2": "7", "name": "Alice"}

	rowTpl := filepath.Join(t.TempDir(), "row.tpl")
	if err := os.WriteFile(rowTpl, []byte("{{json .}}"), 0o644); err != nil {
		t.Fatal(err)
	}
	fullTpl := filepath.Join(t.TempDir(), "full.tpl")
	if err := os.WriteFile(fullTpl, []byte("{{json .Rows}}"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := map[string]ExportOptions{
		"json":              {Format: FormatJSON},
		"yaml":              {Format: FormatYAML},
		"template row json": {Format: FormatTemplate, TemplateRow: rowTpl, TemplateStreaming: true},
		"template full":     {Format: FormatTemplate, TemplateFile: fullTpl},
	}
	for name, options := range tests {
		t.Run(name, func(t *testing.T) {
			rows, err := conn.Query(context.Background(), query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer rows.Close()

			options.Compression = "none"
			options.OutputPath = filepath.Join(t.TempDir(), "out")
			exporter, err := Get(options.Format)
			if err != nil {
				t.Fatalf("Failed to get exporter: %v", err)
			}
			if _, err := exporter.Export(rows, options); err != nil {
				t.Fatalf("Export() error: %v", err)
			}
			content, err := os.ReadFile(options.OutputPath)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			// All formats are read back as a list of rows; a template row is one JSON object.
			var parsed []map[string]any
			switch {
			case options.Format == FormatYAML:
				err = yaml.Unmarshal(content, &parsed)
			case options.TemplateStreaming:
				var row map[string]any
				err = json.Unmarshal(content, &row)
				parsed = []map[string]any{row}
			default:
				err = json.Unmarshal(content, &parsed)
			}
			if err != nil || len(parsed) != 1 {
				t.Fatalf("cannot read back 1 row (err %v):\n%s", err, content)
			}
			for key, value := range want {
				if got, _ := json.Marshal(parsed[0][key]); string(got) != value && string(got) != `"`+value+`"` {
					t.Errorf("%s = %s, want %s\n%s", key, got, value, content)
				}
			}
		})
	}
}
