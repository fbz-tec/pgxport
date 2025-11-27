package exporters

import (
	"fmt"
	"time"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
)

type xlsxExporter struct{}

// Export writes query results to an Excel XLSX file.
func (e *xlsxExporter) Export(rows pgx.Rows, xlsxPath string, options ExportOptions) (int, error) {
	start := time.Now()

	logger.Debug("Preparing XLSX export (compression=%s)", options.Compression)

	// Create new Excel file
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			logger.Warn("Error closing Excel file: %v", err)
		}
	}()

	sheetName := "Sheet1"

	fields := rows.FieldDescriptions()

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

	// Use StreamWriter for better performance
	sw, err := f.NewStreamWriter(sheetName)
	if err != nil {
		return 0, fmt.Errorf("error creating stream writer: %w", err)
	}

	// Write headers
	currentRow := 1
	if !options.NoHeader {
		headerCells := make([]interface{}, len(fields))
		for i, fd := range fields {
			headerCells[i] = excelize.Cell{
				Value:   string(fd.Name),
				StyleID: headerStyleID,
			}
		}

		cell, _ := excelize.CoordinatesToCellName(1, currentRow)
		if err := sw.SetRow(cell, headerCells); err != nil {
			return 0, fmt.Errorf("error writing headers: %w", err)
		}

		logger.Debug("XLSX headers written: %d columns", len(fields))
		currentRow++
	}

	// Write data rows
	logger.Debug("Starting to write XLSX rows...")

	rowCount := 0
	lastLog := time.Now()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return rowCount, fmt.Errorf("error reading row: %w", err)
		}

		excelValues := make([]interface{}, len(values))
		for i, v := range values {
			excelValues[i] = formatters.FormatXLSXValue(v, fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
		}

		cell, _ := excelize.CoordinatesToCellName(1, currentRow)
		if err := sw.SetRow(cell, excelValues); err != nil {
			return rowCount, fmt.Errorf("error writing row %d: %w", currentRow, err)
		}

		rowCount++
		currentRow++

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

	if options.Compression == "none" {
		if err := f.SaveAs(xlsxPath); err != nil {
			return rowCount, fmt.Errorf("error saving Excel file: %w", err)
		}
	} else {
		writerCloser, err := createOutputWriter(xlsxPath, options, FormatXLSX)
		if err != nil {
			return rowCount, err
		}
		defer writerCloser.Close()

		if err := f.Write(writerCloser); err != nil {
			return rowCount, fmt.Errorf("error writing compressed Excel file: %w", err)
		}
	}

	elapsed := time.Since(start)
	logger.Debug("XLSX export completed: %d rows in %.2fs (%.2f rows/sec)",
		rowCount, elapsed.Seconds(), float64(rowCount)/elapsed.Seconds())

	return rowCount, nil
}

func init() {
	MustRegister(FormatXLSX, func() Exporter {
		return &xlsxExporter{}
	})
}
