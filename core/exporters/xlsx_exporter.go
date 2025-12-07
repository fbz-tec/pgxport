package exporters

import (
	"fmt"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/core/output"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/fbz-tec/pgxport/internal/ui"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
)

type xlsxExporter struct{}

// Export writes query results to an Excel XLSX file.
// Automatically creates multiple sheets if the row count exceeds Excel's maximum (1,048,576 rows per sheet).
func (e *xlsxExporter) Export(rows pgx.Rows, options ExportOptions) (int, error) {

	const maxRows = 1_048_576 // Maximum rows in an XLSX sheet

	start := time.Now()

	logger.Debug("Preparing XLSX export (compression=%s)", options.Compression)

	// Create new Excel file
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			logger.Warn("Error closing Excel file: %v", err)
		}
	}()

	// Remove default sheet to avoid duplication
	f.DeleteSheet("Sheet1")

	fields := rows.FieldDescriptions()

	columns := make([]string, len(fields))
	for i, fd := range fields {
		columns[i] = string(fd.Name)
	}

	// Create style for headers if present
	var headerStyleID int
	if !options.NoHeader {
		styleID, err := f.NewStyle(&excelize.Style{
			Font: &excelize.Font{
				Bold:  true,
				Color: "000000",
			},
		})
		if err != nil {
			logger.Warn("Failed to create header style: %v", err)
		} else {
			headerStyleID = styleID
		}
	}

	// Write data rows
	logger.Debug("Starting to write XLSX rows...")

	rowCount := 0
	lastLog := time.Now()

	var sp *ui.Spinner

	if options.ProgressBar {
		sp = ui.NewSpinner()
		sp.Start()
	}

	var sw *excelize.StreamWriter
	var err error
	var currentRow int
	sheetIndex := 1

	sw, currentRow, err = initSheet(columns, options.NoHeader, headerStyleID, f, sheetIndex)
	if err != nil {
		return 0, err
	}

	for rows.Next() {
		values, err := rows.Values()

		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}

		//format values for excel
		excelValues := make([]interface{}, len(values))
		for i, v := range values {
			excelValues[i] = formatters.FormatXLSXValue(v, fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		}

		if currentRow > maxRows {

			if err := sw.Flush(); err != nil {
				return rowCount, fmt.Errorf("error flushing sheet %d: %w", sheetIndex, err)
			}

			sheetIndex++
			logger.Debug("Created new sheet Sheet%d (row limit reached)", sheetIndex)

			sw, currentRow, err = initSheet(columns, options.NoHeader, headerStyleID, f, sheetIndex)
			if err != nil {
				return 0, err
			}

		}

		cell, _ := excelize.CoordinatesToCellName(1, currentRow)
		if err := sw.SetRow(cell, excelValues); err != nil {
			return rowCount, fmt.Errorf("error writing row %d: %w", currentRow, err)
		}

		rowCount++
		currentRow++

		sp.Update(fmt.Sprintf("Processing rows... %d rows [%ds]",
			rowCount,
			int(time.Since(start).Seconds())))

		// Log progress every 10000 rows
		if rowCount%10000 == 0 {
			elapsed := time.Since(lastLog)
			logger.Debug("Processed %d rows (%.2f rows/sec)", rowCount, float64(10000)/elapsed.Seconds())
			lastLog = time.Now()
		}
	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	// Flush stream writer
	if err := sw.Flush(); err != nil {
		return rowCount, fmt.Errorf("error flushing stream: %w", err)
	}

	writerCloser, err := output.CreateWriter(output.OutputConfig{
		Path:        options.OutputPath,
		Compression: options.Compression,
		Format:      options.Format,
	})

	if err != nil {
		return rowCount, err
	}
	defer writerCloser.Close()

	if err := f.Write(writerCloser); err != nil {
		return rowCount, fmt.Errorf("error writing Excel file: %w", err)
	}

	elapsed := time.Since(start)
	logger.Debug("XLSX export completed: %d rows in %.2fs (%.2f rows/sec)",
		rowCount, elapsed.Seconds(), float64(rowCount)/elapsed.Seconds())

	sp.Stop("Completed!")
	return rowCount, nil
}

// initSheet initializes a new Excel sheet with optional headers.
// Returns a stream writer, the starting row number, and an error if initialization fails.
func initSheet(columns []string, noHeader bool, headerStyleID int, f *excelize.File, sheetIndex int) (*excelize.StreamWriter, int, error) {

	sheetName := fmt.Sprintf("Sheet%d", sheetIndex)
	currentRow := 1
	if _, err := f.NewSheet(sheetName); err != nil {
		return nil, currentRow, fmt.Errorf("failed to create new sheet: %w", err)
	}

	// Use StreamWriter for better performance
	sw, err := f.NewStreamWriter(sheetName)
	if err != nil {
		return nil, currentRow, fmt.Errorf("error creating stream writer: %w", err)
	}

	// Write headers
	if !noHeader {
		headerCells := make([]interface{}, len(columns))
		for i, col := range columns {
			headerCells[i] = excelize.Cell{
				Value:   col,
				StyleID: headerStyleID,
			}
		}

		cell, _ := excelize.CoordinatesToCellName(1, currentRow)
		if err := sw.SetRow(cell, headerCells); err != nil {
			return nil, currentRow, fmt.Errorf("error writing headers: %w", err)
		}

		logger.Debug("XLSX headers written: %d columns", len(columns))
		currentRow++
	}

	return sw, currentRow, nil
}

func init() {
	MustRegister(FormatXLSX, func() Exporter {
		return &xlsxExporter{}
	})
}
