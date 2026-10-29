package exporters

import (
	"encoding/xml"
	"fmt"
	"time"
	"unicode"

	"github.com/fbz-tec/pgxport/core/formatters"
	"github.com/fbz-tec/pgxport/core/output"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/fbz-tec/pgxport/internal/ui"
	"github.com/jackc/pgx/v5"
)

type xmlExporter struct{}

// Export writes query results to an XML file with buffered I/O.
func (e *xmlExporter) Export(rows pgx.Rows, options ExportOptions) (_ int, err error) {

	start := time.Now()
	logger.Debug("Preparing XML export (indent=2 spaces, compression=%s)", options.Compression)

	writerCloser, err := output.CreateWriter(output.OutputConfig{
		Path:        options.OutputPath,
		Compression: options.Compression,
		Format:      options.Format,
	})

	if err != nil {
		return 0, err
	}
	defer output.Finish(writerCloser, &err)

	// Encode to XML with indentation
	encoder := xml.NewEncoder(writerCloser)
	encoder.Indent("", "  ")

	// Write XML header
	if _, err := writerCloser.Write([]byte(xml.Header)); err != nil {
		return 0, fmt.Errorf("error writing XML header: %w", err)
	}

	logger.Debug("XML header written")

	// get fields names, made valid as XML element names (e.g. "?column?" for SELECT 1+1)
	fields := rows.FieldDescriptions()
	keys := make([]string, len(fields))
	for i, fd := range fields {
		keys[i] = xmlName(fd.Name)
	}

	startResults := xml.StartElement{Name: xml.Name{Local: options.XmlRootElement}}
	if err := encoder.EncodeToken(startResults); err != nil {
		return 0, fmt.Errorf("error starting <%s>: %w", options.XmlRootElement, err)
	}

	rowCount := 0

	logger.Debug("Starting to write XML rows...")

	var sp *ui.Spinner

	if options.ProgressBar {
		sp = ui.NewSpinner()
		sp.Start()
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("error reading row: %w", err)
		}

		startRow := xml.StartElement{Name: xml.Name{Local: options.XmlRowElement}}

		if err := encoder.EncodeToken(startRow); err != nil {
			return rowCount, fmt.Errorf("error opening <%s>: %w", options.XmlRowElement, err)
		}

		for i, field := range keys {
			elem := xml.StartElement{Name: xml.Name{Local: field}}
			val := formatters.FormatTextValue(values[i], fields[i].DataTypeOID, options.TimeFormat, options.TimeZone)
			// JSON and arrays are text like any other value: they must stay inside the element and be escaped.
			if err := encoder.EncodeElement(val, elem); err != nil {
				return rowCount, fmt.Errorf("error encoding field %s: %w", field, err)
			}
		}

		// End </row>
		if err := encoder.EncodeToken(xml.EndElement{Name: startRow.Name}); err != nil {
			return rowCount, fmt.Errorf("error closing </%s>: %w", options.XmlRowElement, err)
		}

		rowCount++
		sp.Update(fmt.Sprintf("Processing rows... %d rows [%ds]",
			rowCount,
			int(time.Since(start).Seconds())))

		if rowCount%10000 == 0 {
			logger.Debug("%d XML rows written...", rowCount)
		}

	}

	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("error iterating rows: %w", err)
	}

	if err := encoder.EncodeToken(xml.EndElement{Name: startResults.Name}); err != nil {
		return 0, fmt.Errorf("error ending </%s>: %w", options.XmlRootElement, err)
	}

	if err := encoder.Flush(); err != nil {
		return 0, fmt.Errorf("error flushing XML encoder: %w", err)
	}

	// Add final newline
	if _, err := writerCloser.Write([]byte("\n")); err != nil {
		return 0, fmt.Errorf("error writing final newline: %w", err)
	}

	logger.Debug("XML export completed successfully: %d rows written in %v", rowCount, time.Since(start))
	sp.Stop("Completed!")

	return rowCount, nil
}

// xmlName turns a column name into a valid XML element name: characters not allowed
// in a name become '_', and a name that cannot start an element gets a '_' prefix.
// Valid names are returned unchanged.
func xmlName(name string) string {
	runes := []rune(name)
	for i, r := range runes {
		if !isXMLNameRune(r, false) {
			runes[i] = '_'
		}
	}
	if len(runes) == 0 || !isXMLNameRune(runes[0], true) {
		return "_" + string(runes)
	}
	return string(runes)
}

// ValidXMLName reports whether name can be used as an XML element name as is.
func ValidXMLName(name string) bool {
	return name != "" && xmlName(name) == name
}

// isXMLNameRune is a simplified XML Name rule: letters and '_' anywhere,
// digits, '-' and '.' after the first character. ':' is left out (XML namespaces).
func isXMLNameRune(r rune, first bool) bool {
	if unicode.IsLetter(r) || r == '_' {
		return true
	}
	return !first && (unicode.IsDigit(r) || r == '-' || r == '.')
}

func init() {
	MustRegister(FormatXML, func() Exporter { return &xmlExporter{} })
}
