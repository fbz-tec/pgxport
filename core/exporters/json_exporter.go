package exporters

import (
	"fmt"
	"time"

	"github.com/elliotchance/orderedmap/v3"
	"github.com/fbz-tec/pgxport/core/encoders"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
)

type jsonExporter struct{}

// writes query results to a JSON file with buffered I/O
func (e *jsonExporter) Export(rows pgx.Rows, jsonPath string, options ExportOptions) (int, error) {
	start := time.Now()
	logger.Debug("Preparing JSON export (indent=2 spaces, compression=%s)", options.Compression)

	writeCloser, err := createOutputWriter(jsonPath, options, FormatJSON)
	if err != nil {
		return 0, err
	}
	defer writeCloser.Close()

	// Write opening bracket
	if _, err := writeCloser.Write([]byte("[\n")); err != nil {
		return 0, fmt.Errorf("error writing start of JSON array: %w", err)
	}

	// Get column descriptions
	fields := rows.FieldDescriptions()

	// Create ordered JSON encoder
	orderedEncoder := encoders.NewOrderedJsonEncoder(options.TimeFormat, options.TimeZone)

	rowCount := 0
	logger.Debug("Starting to write JSON objects...")

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}

		// Write comma separator for subsequent entries
		if rowCount > 0 {
			if _, err := writeCloser.Write([]byte(",\n")); err != nil {
				return rowCount, fmt.Errorf("error writing comma for row %d: %w", rowCount, err)
			}
		}

		rowData := orderedmap.NewOrderedMap[string, encoders.DataParams]()

		for i, fd := range fields {
			rowData.Set(fd.Name, encoders.DataParams{
				Value:     values[i],
				ValueType: fd.DataTypeOID,
			})
		}
		// Encode with preserved order
		jsonBytes, err := orderedEncoder.EncodeRow(rowData)
		if err != nil {
			return rowCount, fmt.Errorf("error encoding JSON for row %d: %w", rowCount, err)
		}

		// Write with indentation
		if _, err := writeCloser.Write([]byte("  ")); err != nil {
			return rowCount, fmt.Errorf("error writing indentation for row %d: %w", rowCount, err)
		}
		if _, err := writeCloser.Write(jsonBytes); err != nil {
			return rowCount, fmt.Errorf("error writing JSON object for row %d: %w", rowCount, err)
		}

		rowCount++

		if rowCount%10000 == 0 {
			logger.Debug("%d JSON objects written...", rowCount)
		}
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	// Write closing bracket
	if _, err := writeCloser.Write([]byte("\n]\n")); err != nil {
		return rowCount, fmt.Errorf("error writing end of JSON array: %w", err)
	}

	logger.Debug("JSON export completed successfully: %d rows written in %v", rowCount, time.Since(start))

	return rowCount, nil
}

func init() {
	MustRegisterExporter(FormatJSON, func() Exporter { return &jsonExporter{} })
}
