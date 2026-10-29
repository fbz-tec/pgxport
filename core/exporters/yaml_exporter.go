package exporters

import (
	"fmt"
	"io"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/core/output"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/fbz-tec/pgxport/internal/ui"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

	fields := rows.FieldDescriptions()
	keys := uniqueColumnNames(fields)

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

		rowNode, err := yamlRowNode(keys, fields, values, options)
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

// yamlRowNode builds one row as a YAML mapping, keys in column order.
func yamlRowNode(keys []string, fields []pgconn.FieldDescription, values []interface{}, options ExportOptions) (*yaml.Node, error) {
	row := &yaml.Node{Kind: yaml.MappingNode}
	for i, key := range keys {
		valueNode := &yaml.Node{}
		value := formatters.FormatYAMLValue(values[i], fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		if err := valueNode.Encode(value); err != nil {
			return nil, err
		}
		row.Content = append(row.Content, &yaml.Node{Kind: yaml.ScalarNode, Value: key}, valueNode)
	}
	return row, nil
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
