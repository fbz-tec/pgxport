package exporters

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
)

type csvExporter struct{}

// Export writes query results to a CSV file with buffered I/O.
func (e *csvExporter) Export(rows pgx.Rows, csvPath string, options ExportOptions) (int, error) {
	start := time.Now()

	logger.Debug("Preparing CSV export (delimiter=%q, noHeader=%v, compression=%s)",
		string(options.Delimiter), options.NoHeader, options.Compression)

	writerCloser, err := createOutputWriter(csvPath, options, FormatCSV)
	if err != nil {
		return 0, err
	}

	defer writerCloser.Close()

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

	rowCount := 0
	lastLog := time.Now()
	var fetchTime time.Duration // Track time spent waiting for rows from PostgreSQL

	for {
		fetchStart := time.Now()
		hasNext := rows.Next()
		fetchTime += time.Since(fetchStart)

		if !hasNext {
			break
		}

		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}
		//format values to strings
		record := make([]string, len(values))
		for i, v := range values {
			record[i] = formatters.FormatCSVValue(v, fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		}

		rowCount++

		if err := writer.Write(record); err != nil {
			return 0, fmt.Errorf("error writing row %d: %w", rowCount, err)
		}

		if logger.IsVerbose() && (rowCount%10000 == 0 || time.Since(lastLog) > 2*time.Second) {
			elapsed := time.Since(start)
			rowsPerSec := float64(rowCount) / elapsed.Seconds()
			avgFetchMs := float64(fetchTime.Milliseconds()) / float64(rowCount)

			logger.Debug("%d rows written (%.0f rows/s, elapsed %v, avg fetch=%.2fms/row)",
				rowCount, rowsPerSec, elapsed.Truncate(100*time.Millisecond), avgFetchMs)

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

	// Detect slow streaming and suggest COPY mode
	if logger.IsVerbose() && rowCount > 1000 {
		avgFetchMs := float64(fetchTime.Milliseconds()) / float64(rowCount)
		fetchPercent := (fetchTime.Seconds() / elapsed.Seconds()) * 100

		// If average fetch time > 5ms per row OR fetch time > 70% of total time, suggest COPY mode
		if avgFetchMs > 5.0 || fetchPercent > 70 {
			logger.Warn("Slow row streaming detected (%.1fms/row, %.0f%% fetch time)", avgFetchMs, fetchPercent)
			logger.Info("For better performance, use --with-copy flag (PostgreSQL COPY is 10-100x faster)")
		}
	}

	return rowCount, nil
}

func (e *csvExporter) ExportCopy(conn *pgx.Conn, query string, csvPath string, options ExportOptions) (int, error) {

	start := time.Now()
	logger.Debug("Starting PostgreSQL COPY export (noHeader=%v, compression=%s)", options.NoHeader, options.Compression)

	writerCloser, err := createOutputWriter(csvPath, options, FormatCSV)
	if err != nil {
		return 0, err
	}

	defer writerCloser.Close()

	copySql := fmt.Sprintf("COPY (%s) TO STDOUT WITH (FORMAT csv, HEADER %t, DELIMITER '%c')", query, !options.NoHeader, options.Delimiter)

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
