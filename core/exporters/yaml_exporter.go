package exporters

import (
	"fmt"
	"time"

	"github.com/fbz-tec/pgxport/core/encoders"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

type yamlExporter struct{}

func (e *yamlExporter) Export(rows pgx.Rows, yamlPath string, options ExportOptions) (int, error) {
	start := time.Now()
	logger.Debug("Preparing YAML export (compression=%s)", options.Compression)

	writeCloser, err := createOutputWriter(yamlPath, options, FormatYAML)
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
	keys := make([]string, len(fields))
	dataTypes := make([]uint32, len(fields))
	for i, fd := range fields {
		keys[i] = string(fd.Name)
		dataTypes[i] = fd.DataTypeOID
	}

	rowEncoder := encoders.NewOrderedYamlEncoder(options.TimeFormat, options.TimeZone)

	rowCount := 0

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row %d: %w", rowCount+1, err)
		}

		rowNode, err := rowEncoder.EncodeRow(keys, dataTypes, values)
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
	MustRegisterExporter(FormatYAML, func() Exporter { return &yamlExporter{} })
}
