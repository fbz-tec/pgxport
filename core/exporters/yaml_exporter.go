package exporters

import (
	"fmt"
	"time"

	"github.com/elliotchance/orderedmap/v3"
	"github.com/fbz-tec/pgxport/core/encoders"
	"github.com/fbz-tec/pgxport/core/output"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/fbz-tec/pgxport/internal/ui"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

type yamlExporter struct{}

// Export writes query results to a YAML file.
func (e *yamlExporter) Export(rows pgx.Rows, options ExportOptions) (int, error) {
	start := time.Now()
	logger.Debug("Preparing YAML export (compression=%s)", options.Compression)

	writerCloser, err := output.CreateWriter(output.OutputConfig{
		Path:        options.OutputPath,
		Compression: options.Compression,
		Format:      options.Format,
	})

	if err != nil {
		return 0, err
	}
	defer writerCloser.Close()

	enc := yaml.NewEncoder(writerCloser)
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
	var sp *ui.Spinner

	if options.ProgressBar {
		sp = ui.NewSpinner()
		sp.Start()
	}
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
		sp.Update(fmt.Sprintf("[1/2] Processing rows... %d rows [%ds]",
			rowCount,
			int(time.Since(start).Seconds())))

		if rowCount%10000 == 0 {
			logger.Debug("%d YAML rows processed...", rowCount)
		}
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	sp.Stop("Completed!")

	var sp2 *ui.Spinner
	if options.ProgressBar {
		sp2 = ui.NewSpinner()
		sp2.Start()
	}
	sp2.Update("[2/2] Writing output...")
	// Encode final YAML sequence
	if err := enc.Encode(rootSeq); err != nil {
		return rowCount, fmt.Errorf("error writing YAML: %w", err)
	}
	sp2.Stop("Completed!")

	logger.Debug("YAML export completed: %d rows written in %v",
		rowCount, time.Since(start))

	return rowCount, nil
}

func init() {
	MustRegister(FormatYAML, func() Exporter { return &yamlExporter{} })
}
