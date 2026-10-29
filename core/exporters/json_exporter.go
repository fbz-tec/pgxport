package exporters

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/core/output"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/fbz-tec/pgxport/internal/ui"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type jsonExporter struct{}

// Export writes query results to a JSON file with buffered I/O.
func (e *jsonExporter) Export(rows pgx.Rows, options ExportOptions) (_ int, err error) {
	start := time.Now()
	logger.Debug("Preparing JSON export (indent=2 spaces, compression=%s)", options.Compression)

	writerCloser, err := output.CreateWriter(output.OutputConfig{
		Path:        options.OutputPath,
		Compression: options.Compression,
		Format:      options.Format,
	})

	if err != nil {
		return 0, err
	}
	defer output.Finish(writerCloser, &err)

	// Write opening bracket
	if _, err := writerCloser.Write([]byte("[\n")); err != nil {
		return 0, fmt.Errorf("error writing start of JSON array: %w", err)
	}

	// Column names are encoded once; values are written in column order, no map per row.
	fields := rows.FieldDescriptions()
	keys := make([][]byte, len(fields))
	for i, name := range uniqueColumnNames(fields) {
		if keys[i], err = marshalWithoutHTMLEscape(name); err != nil {
			return 0, fmt.Errorf("error encoding column name %q: %w", name, err)
		}
	}

	rowCount := 0
	logger.Debug("Starting to write JSON objects...")

	var sp *ui.Spinner

	if options.ProgressBar {
		sp = ui.NewSpinner()
		sp.Start()
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}

		// Write comma separator for subsequent entries
		if rowCount > 0 {
			if _, err := writerCloser.Write([]byte(",\n")); err != nil {
				return rowCount, fmt.Errorf("error writing comma for row %d: %w", rowCount, err)
			}
		}

		jsonBytes, err := encodeJSONRow(keys, fields, values, options)
		if err != nil {
			return rowCount, fmt.Errorf("error encoding JSON for row %d: %w", rowCount, err)
		}

		// Write with indentation
		if _, err := writerCloser.Write([]byte("  ")); err != nil {
			return rowCount, fmt.Errorf("error writing indentation for row %d: %w", rowCount, err)
		}
		if _, err := writerCloser.Write(jsonBytes); err != nil {
			return rowCount, fmt.Errorf("error writing JSON object for row %d: %w", rowCount, err)
		}

		rowCount++
		sp.Update(fmt.Sprintf("Processing rows... %d rows [%ds]",
			rowCount,
			int(time.Since(start).Seconds())))

		if rowCount%10000 == 0 {
			logger.Debug("%d JSON objects written...", rowCount)
		}
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	// Write closing bracket
	if _, err := writerCloser.Write([]byte("\n]\n")); err != nil {
		return rowCount, fmt.Errorf("error writing end of JSON array: %w", err)
	}
	sp.Stop("Completed!")

	logger.Debug("JSON export completed successfully: %d rows written in %v", rowCount, time.Since(start))

	return rowCount, nil
}

// encodeJSONRow writes one row as an indented JSON object, keys in column order.
// keys are the column names already encoded as JSON strings.
func encodeJSONRow(keys [][]byte, fields []pgconn.FieldDescription, values []interface{}, options ExportOptions) ([]byte, error) {
	if len(keys) == 0 {
		return []byte("{}"), nil
	}

	var row bytes.Buffer
	row.Grow(len(keys) * 32)
	row.WriteString("{\n")
	for i, key := range keys {
		if i > 0 {
			row.WriteString(",\n")
		}
		row.WriteString("    ")
		row.Write(key)
		row.WriteString(": ")
		value := formatters.FormatJSONValue(values[i], fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		valueJSON, err := marshalWithoutHTMLEscape(value)
		if err != nil {
			return nil, fmt.Errorf("error marshaling value for key %s: %w", key, err)
		}
		row.Write(valueJSON)
	}
	row.WriteString("\n  }")
	return row.Bytes(), nil
}

// marshalWithoutHTMLEscape encodes v as JSON without escaping <, > and &.
// JSON objects (json/jsonb columns) are indented to sit inside the row object.
func marshalWithoutHTMLEscape(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if _, ok := v.(map[string]interface{}); ok {
		encoder.SetIndent("    ", "  ")
	}
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func init() {
	MustRegister(FormatJSON, func() Exporter { return &jsonExporter{} })
}
