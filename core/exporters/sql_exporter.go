package exporters

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
)

type sqlExporter struct{}

func (e *sqlExporter) Export(rows pgx.Rows, sqlPath string, options ExportOptions) (int, error) {

	start := time.Now()
	logger.Debug("Preparing SQL export (table=%s, compression=%s, rows-per-statement=%d)",
		options.TableName, options.Compression, options.RowPerStatement)

	writeCloser, err := createOutputWriter(sqlPath, options, FormatSQL)
	if err != nil {
		return 0, err
	}
	defer writeCloser.Close()

	fields := rows.FieldDescriptions()
	columns := make([]string, len(fields))
	for i, fd := range fields {
		columns[i] = formatters.QuoteIdent(fd.Name)
	}
	size := len(columns)

	logger.Debug("Starting to write SQL INSERT statements...")

	var rowCount int
	var statementCount int
	batchInsertValues := make([][]string, 0, options.RowPerStatement)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("error reading row: %w", err)
		}

		record := make([]string, size)

		//format values
		for i, val := range values {
			record[i] = formatters.FormatSQLValue(val, fields[i].DataTypeOID)
		}

		rowCount++
		batchInsertValues = append(batchInsertValues, record)

		// Write batch when full
		if len(batchInsertValues) == options.RowPerStatement {
			if err := e.writeBatchInsert(writeCloser, options.TableName, columns, batchInsertValues); err != nil {
				return 0, fmt.Errorf("error writing batch statement %d: %w", statementCount+1, err)
			}
			statementCount++
			batchInsertValues = batchInsertValues[:0]

			if statementCount%1000 == 0 {
				logger.Debug("%d rows processed (%d INSERT statements written)...", rowCount, statementCount)
			}
		}
	}

	// Write remaining rows as final batch
	if len(batchInsertValues) > 0 {
		if err := e.writeBatchInsert(writeCloser, options.TableName, columns, batchInsertValues); err != nil {
			return 0, fmt.Errorf("error writing final batch statement: %w", err)
		}
		statementCount++
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	logger.Debug("SQL export completed successfully: %d rows written in %d INSERT statements (%v)",
		rowCount, statementCount, time.Since(start))

	return rowCount, nil
}

// writeBatchInsert writes a single or multi-row INSERT statement
func (e *sqlExporter) writeBatchInsert(writer io.Writer, table string, columns []string, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	var stmt strings.Builder

	// Write INSERT header
	stmt.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES\n",
		formatters.QuoteIdent(table), strings.Join(columns, ", ")))

	// Write value rows
	for i, record := range rows {
		separator := ","
		if i == len(rows)-1 {
			separator = ";"
		}
		stmt.WriteString(fmt.Sprintf("\t(%s)%s\n", strings.Join(record, ", "), separator))
	}

	_, err := writer.Write([]byte(stmt.String()))
	return err
}

func init() {
	MustRegisterExporter(FormatSQL, func() Exporter { return &sqlExporter{} })
}
