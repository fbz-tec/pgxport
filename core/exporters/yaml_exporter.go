package exporters

import (
	"fmt"
	"time"

	"github.com/elliotchance/orderedmap/v3"
	"github.com/fbz-tec/pgxport/core/encoders"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

type yamlExporter struct{}

func (e *yamlExporter) Export(rows pgx.Rows, options ExportOptions) (int, error) {
	start := time.Now()
	logger.Debug("Preparing YAML export (compression=%s)", options.Compression)

	writeCloser, err := createOutputWriter(options)
	if err != nil {
		return 0, err
	}
	defer writeCloser.Close()

	enc := yaml.NewEncoder(writeCloser)
	enc.SetIndent(2)
	defer enc.Close()

	// Root YAML Sequence (the "-" items)
	rootSeq := &yaml.Node{
		Kind: yaml.SequenceNode,
	}

	// Column order
	fields := rows.FieldDescriptions()

	rowEncoder := encoders.NewOrderedYamlEncoder(options.TimeFormat, options.TimeZone)

	rowCount := 0

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row %d: %w", rowCount+1, err)
		}

		rowData := orderedmap.NewOrderedMap[string, encoders.DataParams]()

		for i, fd := range fields {
			rowData.Set(fd.Name, encoders.DataParams{
				Value:     values[i],
				ValueType: fd.DataTypeOID,
			})
		}

		rowNode, err := rowEncoder.EncodeRow(rowData)
		if err != nil {
			return rowCount, fmt.Errorf("error encoding YAML row %d: %w", rowCount+1, err)
		}

		// Add to sequence
		rootSeq.Content = append(rootSeq.Content, rowNode)
		rowCount++

		if rowCount%10000 == 0 {
			logger.Debug("%d YAML rows processed...", rowCount)
		}
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	// Encode final YAML sequence
	if err := enc.Encode(rootSeq); err != nil {
		return rowCount, fmt.Errorf("error writing YAML: %w", err)
	}

	logger.Debug("YAML export completed: %d rows written in %v",
		rowCount, time.Since(start))

	return rowCount, nil
}

func init() {
	MustRegister(FormatYAML, func() Exporter { return &yamlExporter{} })
}
