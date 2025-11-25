package encoders

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/elliotchance/orderedmap/v3"
	"github.com/fbz-tec/pgxport/core/formatters"
)

// OrderedJsonEncoder encodes JSON while preserving key order
type OrderedJsonEncoder struct {
	timeLayout string
	timezone   string
}

// NewOrderedJsonEncoder creates a new ordered JSON encoder with time formatting options
func NewOrderedJsonEncoder(timeFormat, timeZone string) OrderedJsonEncoder {
	return OrderedJsonEncoder{
		timeLayout: timeFormat,
		timezone:   timeZone,
	}
}

// EncodeJSONWithOrder encodes a map to JSON preserving the order with proper indentation
func (o OrderedJsonEncoder) EncodeRow(rowData *orderedmap.OrderedMap[string, DataParams]) ([]byte, error) {

	if rowData.Len() == 0 {
		return []byte("{}"), nil
	}

	var row bytes.Buffer

	// Pre-allocate memory to avoid reallocation
	row.Grow(rowData.Len() * 32)

	row.WriteString("{\n")

	i := 0

	for k, v := range rowData.AllFromFront() {

		if i > 0 {
			row.WriteString(",\n")
		}
		// Add indentation (4 spaces for inner content)
		row.WriteString("    ")

		row.WriteString(fmt.Sprintf("%q", k))
		row.WriteString(": ")
		// value
		formattedValue := formatters.FormatJSONValue(v.Value, v.ValueType, o.timeLayout, o.timezone)
		// Marshal formatted value with HTML escaping disabled
		valueJSON, err := marshalWithoutHTMLEscape(formattedValue)
		if err != nil {
			return nil, fmt.Errorf("error marshaling value for key %q: %w", k, err)
		}

		row.Write(valueJSON)
		i++
	}

	row.WriteString("\n  }")
	return row.Bytes(), nil
}

func marshalWithoutHTMLEscape(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if _, ok := v.(map[string]interface{}); ok {
		encoder.SetIndent("    ", "  ")
	}

	if err := encoder.Encode(v); err != nil {
		return nil, err
	}

	result := buf.Bytes()
	return bytes.TrimSuffix(result, []byte("\n")), nil
}
