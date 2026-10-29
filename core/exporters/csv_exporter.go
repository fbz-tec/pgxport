package exporters

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/core/output"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/fbz-tec/pgxport/internal/ui"
	"github.com/jackc/pgx/v5"
)

type csvExporter struct{}

// Export writes query results to a CSV file with buffered I/O.
func (e *csvExporter) Export(rows pgx.Rows, options ExportOptions) (_ int, err error) {
	start := time.Now()

	logger.Debug("Preparing CSV export (delimiter=%q, noHeader=%v, compression=%s)",
		string(options.Delimiter), options.NoHeader, options.Compression)

	writerCloser, err := output.CreateWriter(output.OutputConfig{
		Path:        options.OutputPath,
		Compression: options.Compression,
		Format:      options.Format,
	})

	if err != nil {
		return 0, err
	}

	defer output.Finish(writerCloser, &err)

	writer := csv.NewWriter(writerCloser)
	writer.Comma = options.Delimiter
	defer writer.Flush()

	// Write headers
	fields := rows.FieldDescriptions()

	if !options.NoHeader {
		headers := make([]string, len(fields))
		for i, fd := range fields {
			headers[i] = string(fd.Name)
		}

		if err := writer.Write(headers); err != nil {
			return 0, fmt.Errorf("error writing headers: %w", err)
		}
		logger.Debug("CSV headers written: %s", strings.Join(headers, string(options.Delimiter)))
	}

	// Write data rows
	logger.Debug("Starting to write CSV rows...")

	var sp *ui.Spinner

	if options.ProgressBar {
		sp = ui.NewSpinner()
		sp.Start()
	}

	rowCount := 0
	lastLog := time.Now()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}
		//format values to strings
		record := make([]string, len(values))
		for i, v := range values {
			record[i] = formatters.FormatTextValue(v, fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		}

		if err := writer.Write(record); err != nil {
			return 0, fmt.Errorf("error writing row %d: %w", rowCount, err)
		}
		rowCount++
		sp.Update(fmt.Sprintf("Processing rows... %d rows [%ds]",
			rowCount,
			int(time.Since(start).Seconds())))

		if logger.IsVerbose() && (rowCount%10000 == 0 || time.Since(lastLog) > 2*time.Second) {
			elapsed := time.Since(start)
			rowsPerSec := float64(rowCount) / elapsed.Seconds()

			logger.Debug("%d rows written (%.0f rows/s, elapsed %v)",
				rowCount, rowsPerSec, elapsed.Truncate(100*time.Millisecond))

			writer.Flush()
			lastLog = time.Now()
		}

	}

	logger.Debug("Flushing CSV buffers to disk...")
	writer.Flush()

	if err := writer.Error(); err != nil {
		return rowCount, fmt.Errorf("error flushing CSV: %w", err)
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	elapsed := time.Since(start)
	logger.Debug("CSV export completed successfully: %d rows written in %v (%.0f rows/s)",
		rowCount, elapsed.Round(time.Millisecond), float64(rowCount)/elapsed.Seconds())

	sp.Stop("Completed!")
	return rowCount, nil
}

// ExportCopy uses PostgreSQL COPY command for high-performance CSV export.
// This method is significantly faster than standard Export for large datasets.
func (e *csvExporter) ExportCopy(conn *pgx.Conn, query string, options ExportOptions) (_ int, err error) {

	start := time.Now()
	logger.Debug("Starting PostgreSQL COPY export (noHeader=%v, compression=%s)", options.NoHeader, options.Compression)

	writerCloser, err := output.CreateWriter(output.OutputConfig{
		Path:        options.OutputPath,
		Compression: options.Compression,
		Format:      options.Format,
	})

	if err != nil {
		return 0, err
	}

	defer output.Finish(writerCloser, &err)

	// The newlines end a trailing -- comment before the closing parenthesis.
	copySql := fmt.Sprintf("COPY (\n%s\n) TO STDOUT WITH (FORMAT csv, HEADER %t, DELIMITER '%c')", query, !options.NoHeader, options.Delimiter)

	tag, err := conn.PgConn().CopyTo(context.Background(), writerCloser, copySql)
	if err != nil {
		return 0, fmt.Errorf("COPY TO STDOUT failed: %w", err)
	}

	rowCount := int(tag.RowsAffected())
	logger.Debug("COPY export completed successfully: %d rows written in %v", rowCount, time.Since(start))

	return rowCount, nil

}

func init() {
	MustRegister(FormatCSV, func() Exporter { return &csvExporter{} })
}
