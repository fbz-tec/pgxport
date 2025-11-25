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
{{range .Rows}}- {{get . "id"}}:{{get . "name"}}
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
			name:  "multiple columns access",
			query: "SELECT 1 as id, 'Bob' as name, 25 as age",
			template: `{{range .Rows}}ID={{get . "id"}},Name={{get . "name"}},Age={{get . "age"}}
{{end}}`,
			wantErr: false,
			checkFunc: func(t *testing.T, out string) {
				content, _ := os.ReadFile(out)
				s := string(content)

				if !strings.Contains(s, "ID=1") {
					t.Errorf("Expected ID=1 in output")
				}
				if !strings.Contains(s, "Name=Bob") {
					t.Errorf("Expected Name=Bob in output")
				}
				if !strings.Contains(s, "Age=25") {
					t.Errorf("Expected Age=25 in output")
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

	// streaming templates using get helper
	os.WriteFile(header, []byte(`<table>{{range .Columns}}<th>{{.}}</th>{{end}}`), 0644)
	os.WriteFile(row, []byte(`<tr><td>{{get . "id"}}</td><td>{{get . "name"}}</td></tr>`), 0644)
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

	// use get helper to access orderedmap value
	os.WriteFile(tpl, []byte(`{{range .Rows}}{{get . "created_at"}}{{end}}`), 0644)

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
	s := string(content)

	if !strings.Contains(s, "-") { // formatted
		t.Error("Expected formatted date, got: ", s)
	}
}

func TestExportTemplateDataTypes(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmp := t.TempDir()
	tpl := filepath.Join(tmp, "tpl.txt")
	outPath := filepath.Join(tmp, "out.txt")

	os.WriteFile(tpl, []byte(`{{range .Rows}}ID={{get . "id"}}, N={{get . "name"}}{{end}}`), 0644)

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

func TestExportTemplateHelperFunctions(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	tmp := t.TempDir()
	tpl := filepath.Join(tmp, "tpl.txt")
	outPath := filepath.Join(tmp, "out.txt")

	// Test various helper functions
	template := `{{range .Rows}}
Upper: {{upper (get . "name")}}
Lower: {{lower (get . "name")}}
Title: {{title (get . "name")}}
{{end}}`

	os.WriteFile(tpl, []byte(template), 0644)

	query := "SELECT 'Alice Smith' as name"

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
		t.Fatalf("Template helpers test err: %v", err)
	}

	content, _ := os.ReadFile(outPath)
	s := string(content)

	if !strings.Contains(s, "Upper: ALICE SMITH") {
		t.Error("Expected uppercase transformation")
	}
	if !strings.Contains(s, "Lower: alice smith") {
		t.Error("Expected lowercase transformation")
	}
	if !strings.Contains(s, "Title: Alice Smith") {
		t.Error("Expected title case transformation")
	}
}
