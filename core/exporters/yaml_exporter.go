package exporters

import (
	"fmt"
	"io"
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
func (e *yamlExporter) Export(rows pgx.Rows, options ExportOptions) (_ int, err error) {
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
	defer output.Finish(writerCloser, &err)

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

		// Written right away as a one-item list: "- key: value" items placed one after
		// another form the same single YAML list, without keeping all rows in memory.
		if err := writeYAML(writerCloser, &yaml.Node{Kind: yaml.SequenceNode, Content: []*yaml.Node{rowNode}}); err != nil {
			return rowCount, fmt.Errorf("error writing YAML row %d: %w", rowCount+1, err)
		}
		rowCount++
		sp.Update(fmt.Sprintf("Processing rows... %d rows [%ds]",
			rowCount,
			int(time.Since(start).Seconds())))

		if rowCount%10000 == 0 {
			logger.Debug("%d YAML rows processed...", rowCount)
		}
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	// No rows: an empty list ("[]"), not an empty document (which reads as null)
	if rowCount == 0 {
		if err := writeYAML(writerCloser, &yaml.Node{Kind: yaml.SequenceNode}); err != nil {
			return 0, fmt.Errorf("error writing YAML: %w", err)
		}
	}

	sp.Stop("Completed!")

	logger.Debug("YAML export completed: %d rows written in %v",
		rowCount, time.Since(start))

	return rowCount, nil
}

// writeYAML writes node as one YAML document with pgxport's 2-space indentation.
// A new encoder per call: reusing one would separate documents with "---".
func writeYAML(w io.Writer, node *yaml.Node) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	if err := enc.Encode(node); err != nil {
		return err
	}
	return enc.Close()
}

func init() {
	MustRegister(FormatYAML, func() Exporter { return &yamlExporter{} })
}
