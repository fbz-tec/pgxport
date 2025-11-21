package exporters

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExportTemplateFull(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name      string
		query     string
		template  string
		wantErr   bool
		checkFunc func(t *testing.T, out string)
	}{
		{
			name:  "basic full template export",
			query: "SELECT 1 as id, 'Alice' as name",
			template: `Total={{.Count}}
{{range .Rows}}- {{.id}}:{{.name}}
{{end}}`,
			wantErr: false,
			checkFunc: func(t *testing.T, out string) {
				content, _ := os.ReadFile(out)
				s := string(content)

				if !strings.Contains(s, "Total=1") {
					t.Errorf("Expected Total=1, got %s", s)
				}
				if !strings.Contains(s, "1:Alice") {
					t.Errorf("Expected row content 1:Alice, got %s", s)
				}
			},
		},
		{
			name:     "empty result full mode",
			query:    "SELECT 1 WHERE 1=0",
			template: `Count={{.Count}}`,
			wantErr:  false,
			checkFunc: func(t *testing.T, out string) {
				content, _ := os.ReadFile(out)
				if !strings.Contains(string(content), "Count=0") {
					t.Errorf("Expected Count=0")
				}
			},
		},
		{
			name:     "invalid full template",
			query:    "SELECT 1 as id",
			template: `{{ INVALID }}`,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			tplPath := filepath.Join(tmp, "tpl.txt")
			outPath := filepath.Join(tmp, "output.txt")

			// write template
			os.WriteFile(tplPath, []byte(tt.template), 0644)

			rows, err := conn.Query(context.Background(), tt.query)
			if err != nil {
				t.Fatalf("query err: %v", err)
			}
			defer rows.Close()

			exporter, _ := GetExporter(FormatTemplate)

			opts := ExportOptions{
				Format:            FormatTemplate,
				TemplateFile:      tplPath,
				TemplateStreaming: false,
				Compression:       "none",
			}

			_, err = exporter.Export(rows, outPath, opts)

			if (err != nil) != tt.wantErr {
				t.Fatalf("Export err=%v, wantErr=%v", err, tt.wantErr)
			}

			if !tt.wantErr && tt.checkFunc != nil {
				tt.checkFunc(t, outPath)
			}
		})
	}
}

func TestExportTemplateStreaming(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmp := t.TempDir()
	header := filepath.Join(tmp, "header.tpl")
	row := filepath.Join(tmp, "row.tpl")
	footer := filepath.Join(tmp, "footer.tpl")
	outPath := filepath.Join(tmp, "output.html")

	// Create templates
	os.WriteFile(header, []byte(`<table>{{range .Columns}}<th>{{.}}</th>{{end}}`), 0644)
	os.WriteFile(row, []byte(`<tr><td>{{.id}}</td><td>{{.name}}</td></tr>`), 0644)
	os.WriteFile(footer, []byte(`</table>`), 0644)

	query := "SELECT 1 as id, 'Alice' as name"

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("query err: %v", err)
	}
	defer rows.Close()

	exporter, _ := GetExporter(FormatTemplate)

	opts := ExportOptions{
		Format:            FormatTemplate,
		TemplateRow:       row,
		TemplateHeader:    header,
		TemplateFooter:    footer,
		TemplateStreaming: true,
		Compression:       "none",
	}

	_, err = exporter.Export(rows, outPath, opts)
	if err != nil {
		t.Fatalf("Streaming export err: %v", err)
	}

	content, _ := os.ReadFile(outPath)
	s := string(content)

	if !strings.Contains(s, "<th>id</th>") {
		t.Error("Expected id column")
	}
	if !strings.Contains(s, "<tr>") {
		t.Error("Expected row <tr>")
	}
	if !strings.Contains(s, "</table>") {
		t.Error("Expected closing table tag")
	}
}

func TestTemplateStreamingMissingRow(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmp := t.TempDir()
	header := filepath.Join(tmp, "header.tpl")
	os.WriteFile(header, []byte("header"), 0644)

	outPath := filepath.Join(tmp, "out.txt")

	rows, _ := conn.Query(context.Background(), "SELECT 1")
	defer rows.Close()

	exporter, _ := GetExporter(FormatTemplate)

	opts := ExportOptions{
		Format:            FormatTemplate,
		TemplateHeader:    header,
		TemplateStreaming: true,
		TemplateRow:       "", // missing → should error
	}

	_, err := exporter.Export(rows, outPath, opts)
	if err == nil {
		t.Fatal("Expected error when TemplateRow missing")
	}
}

func TestExportTemplateTimeFormatting(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmp := t.TempDir()
	tpl := filepath.Join(tmp, "tpl.txt")
	outPath := filepath.Join(tmp, "out.txt")

	os.WriteFile(tpl, []byte(`{{.Rows}}`), 0644)

	query := "SELECT NOW() as created_at"

	rows, _ := conn.Query(context.Background(), query)
	defer rows.Close()

	exporter, _ := GetExporter(FormatTemplate)

	opts := ExportOptions{
		Format:       FormatTemplate,
		TemplateFile: tpl,
		TimeFormat:   "yyyy-MM-dd HH:mm",
		Compression:  "none",
	}

	_, err := exporter.Export(rows, outPath, opts)
	if err != nil {
		t.Fatalf("Template time export err: %v", err)
	}

	content, _ := os.ReadFile(outPath)
	if !strings.Contains(string(content), "-") {
		t.Error("Expected formatted date")
	}
}

func TestExportTemplateDataTypes(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmp := t.TempDir()
	tpl := filepath.Join(tmp, "tpl.txt")
	outPath := filepath.Join(tmp, "out.txt")

	os.WriteFile(tpl, []byte(`{{range .Rows}}ID={{.id}}, N={{.name}}{{end}}`), 0644)

	query := `
		SELECT 
			1::integer as id,
			'text' as name
	`

	rows, _ := conn.Query(context.Background(), query)
	defer rows.Close()

	exporter, _ := GetExporter(FormatTemplate)

	opts := ExportOptions{
		Format:       FormatTemplate,
		TemplateFile: tpl,
		Compression:  "none",
	}

	_, err := exporter.Export(rows, outPath, opts)
	if err != nil {
		t.Fatalf("Template data export err: %v", err)
	}

	content, _ := os.ReadFile(outPath)
	s := string(content)

	if !strings.Contains(s, "ID=1") {
		t.Error("Expected ID=1")
	}
	if !strings.Contains(s, "N=text") {
		t.Error("Expected name=text")
	}
}

func TestExportTemplateLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test")
	}

	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmp := t.TempDir()
	tpl := filepath.Join(tmp, "tpl.txt")
	outPath := filepath.Join(tmp, "out.txt")

	os.WriteFile(tpl, []byte(`{{.Count}} rows`), 0644)

	query := "SELECT generate_series(1, 5000) as id"

	rows, _ := conn.Query(context.Background(), query)
	defer rows.Close()

	exporter, _ := GetExporter(FormatTemplate)

	opts := ExportOptions{
		Format:       FormatTemplate,
		TemplateFile: tpl,
		Compression:  "none",
	}

	count, err := exporter.Export(rows, outPath, opts)
	if err != nil {
		t.Fatalf("Large template err: %v", err)
	}

	if count != 5000 {
		t.Errorf("Expected 5000 rows, got %d", count)
	}

	content, _ := os.ReadFile(outPath)
	if !strings.Contains(string(content), "5000 rows") {
		t.Error("Expected '5000 rows'")
	}
}
