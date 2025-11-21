package exporters

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Template exporter supporting both full and streaming mode.
type templateExporter struct{}

// Export chooses streaming or full mode based on ExportOptions.
func (e *templateExporter) Export(rows pgx.Rows, outputPath string, options ExportOptions) (int, error) {
	if options.TemplateStreaming {
		return e.exportStreaming(rows, outputPath, options)
	}
	return e.exportFull(rows, outputPath, options)
}

// full mode (load all rows)
func (e *templateExporter) exportFull(rows pgx.Rows, outputPath string, options ExportOptions) (int, error) {

	start := time.Now()
	logger.Debug("Preparing TEMPLATE (full mode) export (compression=%s)", options.Compression)

	tplBytes, err := os.ReadFile(options.TemplateFile)
	if err != nil {
		return 0, fmt.Errorf("error reading template file: %w", err)
	}

	tpl, err := template.New("pgxport-template").
		Funcs(defaultTemplateFuncs()).
		Parse(string(tplBytes))
	if err != nil {
		return 0, fmt.Errorf("error parsing template: %w", err)
	}

	fields := rows.FieldDescriptions()
	keys := make([]string, len(fields))
	for i, f := range fields {
		keys[i] = string(f.Name)
	}

	allRows := []map[string]interface{}{}
	rowCount := 0

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}

		entry := make(map[string]interface{}, len(keys))
		for i, k := range keys {
			entry[k] = formatters.FormatTemplateValue(vals[i], fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		}

		allRows = append(allRows, entry)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	writer, err := createOutputWriter(outputPath, options, FormatTemplate)
	if err != nil {
		return rowCount, err
	}
	defer writer.Close()

	data := map[string]interface{}{
		"Rows":        allRows,
		"Columns":     keys,
		"Count":       rowCount,
		"GeneratedAt": time.Now().Format(time.RFC3339),
	}

	if err := tpl.Execute(writer, data); err != nil {
		return rowCount, fmt.Errorf("error executing template: %w", err)
	}

	elapsed := time.Since(start)
	logger.Debug("TEMPLATE full export completed: %d rows in %.2fs", rowCount, elapsed.Seconds())

	return rowCount, nil
}

// Streaming mode
func (e *templateExporter) exportStreaming(rows pgx.Rows, outputPath string, options ExportOptions) (int, error) {

	start := time.Now()
	logger.Debug("Preparing TEMPLATE (streaming mode) export (compression=%s)", options.Compression)

	funcs := defaultTemplateFuncs()

	// Load header / row / footer templates
	tplHeader, err := loadTemplateIfExists(options.TemplateHeader, false, funcs)
	if err != nil {
		return 0, err
	}
	tplRow, err := loadTemplateIfExists(options.TemplateRow, true, funcs)
	if err != nil {
		return 0, err
	}
	tplFooter, err := loadTemplateIfExists(options.TemplateFooter, false, funcs)
	if err != nil {
		return 0, err
	}

	writer, err := createOutputWriter(outputPath, options, FormatTemplate)
	if err != nil {
		return 0, err
	}
	defer writer.Close()

	// Extract column names
	fields := rows.FieldDescriptions()
	keys := make([]string, len(fields))
	for i, f := range fields {
		keys[i] = string(f.Name)
	}

	// Execute header with columns
	if tplHeader != nil {
		headerData := map[string]interface{}{"Columns": keys}
		if err := tplHeader.Execute(writer, headerData); err != nil {
			return 0, fmt.Errorf("error executing header template: %w", err)
		}
	}

	rowCount := 0

	// Stream row-by-row
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}

		rowMap := make(map[string]interface{}, len(keys))
		for i, k := range keys {
			rowMap[k] = formatters.FormatTemplateValue(vals[i], fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		}

		if err := tplRow.Execute(writer, rowMap); err != nil {
			return rowCount, fmt.Errorf("error executing row template: %w", err)
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return rowCount, err
	}

	// Footer
	if tplFooter != nil {
		if err := tplFooter.Execute(writer, nil); err != nil {
			return rowCount, fmt.Errorf("error executing footer template: %w", err)
		}
	}

	elapsed := time.Since(start)
	logger.Debug("TEMPLATE streaming export completed: %d rows in %.2fs", rowCount, elapsed.Seconds())

	return rowCount, nil
}

// utilities for template exporter
func defaultTemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"upper":     strings.ToUpper,
		"lower":     strings.ToLower,
		"title":     cases.Title(language.English).String,
		"trim":      strings.TrimSpace,
		"replace":   strings.ReplaceAll,
		"join":      strings.Join,
		"split":     strings.Split,
		"contains":  strings.Contains,
		"hasPrefix": strings.HasPrefix,
		"hasSuffix": strings.HasSuffix,
		"printf":    fmt.Sprintf,
		"json": func(v interface{}) string {
			b, err := json.Marshal(v)
			if err != nil {
				return fmt.Sprintf("ERROR: %v", err)
			}
			return string(b)
		},
		"jsonPretty": func(v interface{}) string {
			b, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				return fmt.Sprintf("ERROR: %v", err)
			}
			return string(b)
		},
		"now": time.Now,
		"formatTime": func(t time.Time, layout string) string {
			newLayout := formatters.ConvertUserTimeFormat(layout)
			return t.Format(newLayout)
		},
		"eq":  func(a, b interface{}) bool { return a == b },
		"ne":  func(a, b interface{}) bool { return a != b },
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"mul": func(a, b int) int { return a * b },
		"div": func(a, b int) int {
			if b == 0 {
				return 0
			}
			return a / b
		},
	}
}

func loadTemplateIfExists(path string, required bool, funcs template.FuncMap) (*template.Template, error) {
	if strings.TrimSpace(path) == "" {
		if required {
			return nil, fmt.Errorf("template file path is empty")
		} else {
			return nil, nil
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file %q: %w", path, err)
	}
	tpl, err := template.New(path).Funcs(funcs).Parse(string(b))
	if err != nil {
		return nil, fmt.Errorf("failed to parse template %q: %w", path, err)
	}
	return tpl, nil
}

func init() {
	MustRegisterExporter(FormatTemplate, func() Exporter {
		return &templateExporter{}
	})
}
