package formatters

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"gopkg.in/yaml.v3"
)

// Decimal is the exact text of a finite NUMERIC value.
// It is written as a bare number in JSON and YAML; going through float64 would drop digits.
type Decimal string

func (d Decimal) MarshalJSON() ([]byte, error) { return []byte(d), nil }

func (d Decimal) MarshalYAML() (interface{}, error) {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: string(d)}, nil
}

// floatText returns a float32/float64 as PostgreSQL writes it: the shortest digits that
// read back as the same value (0.30000000000000004, 0.1 for real), exponent notation only
// below 1e-4 or from 1e15 (1e6 for real), and NaN, Infinity, -Infinity.
// ok is false when v is not a float.
func floatText(v interface{}) (s string, ok bool) {
	var f float64
	bits, maxFixedExp := 64, 15
	switch x := v.(type) {
	case float64:
		f = x
	case float32:
		f, bits, maxFixedExp = float64(x), 32, 6
	default:
		return "", false
	}
	switch {
	case math.IsNaN(f):
		return "NaN", true
	case math.IsInf(f, 1):
		return "Infinity", true
	case math.IsInf(f, -1):
		return "-Infinity", true
	}
	s = strconv.FormatFloat(f, 'e', -1, bits)
	if exp, err := strconv.Atoi(s[strings.LastIndexByte(s, 'e')+1:]); err == nil && exp >= -4 && exp < maxFixedExp {
		s = strconv.FormatFloat(f, 'f', -1, bits)
	}
	return s, true
}

// textMap encodes values to PostgreSQL text format.
// shortcut: a pgtype.Map is not safe for concurrent use; fine while each export runs in one goroutine.
var textMap = pgtype.NewMap()

// pgText returns val as PostgreSQL writes it in text format for type oid, the same text
// as COPY and psql: 13:45:30.123456 for time, (1.5,2) for point, {1,NULL,3} for arrays.
// ok is false when pgx cannot encode it (e.g. a type it does not know).
func pgText(oid uint32, val interface{}) (string, bool) {
	if r, isRange := val.(pgtype.Range[interface{}]); isRange {
		return rangeText(oid, r)
	}
	b, err := textMap.Encode(oid, pgtype.TextFormatCode, val, nil)
	if err != nil || b == nil {
		return "", false
	}
	return string(b), true
}

// needsPGText reports whether val is a pgx type (pgtype.Time, pgtype.Point, net.HardwareAddr...)
// whose Go form is not readable, rather than a plain value pgxport writes as is.
func needsPGText(val interface{}) bool {
	switch val.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64, time.Time, map[string]interface{}, []interface{}, Decimal:
		return false
	}
	return true
}

// rangeElementOIDs maps the built-in range types to their element type.
var rangeElementOIDs = map[uint32]uint32{
	pgtype.Int4rangeOID: pgtype.Int4OID,
	pgtype.Int8rangeOID: pgtype.Int8OID,
	pgtype.NumrangeOID:  pgtype.NumericOID,
	pgtype.TsrangeOID:   pgtype.TimestampOID,
	pgtype.TstzrangeOID: pgtype.TimestamptzOID,
	pgtype.DaterangeOID: pgtype.DateOID,
}

// rangeText writes a range as PostgreSQL does ([lower,upper), empty), since pgx cannot
// encode the pgtype.Range[any] it decodes ranges into.
func rangeText(oid uint32, r pgtype.Range[interface{}]) (string, bool) {
	if r.LowerType == pgtype.Empty {
		return "empty", true
	}
	elemOID, ok := rangeElementOIDs[oid]
	if !ok {
		return "", false
	}
	lower, okLower := rangeBound(elemOID, r.Lower, r.LowerType)
	upper, okUpper := rangeBound(elemOID, r.Upper, r.UpperType)
	if !okLower || !okUpper {
		return "", false
	}
	open, close := "(", ")"
	if r.LowerType == pgtype.Inclusive {
		open = "["
	}
	if r.UpperType == pgtype.Inclusive {
		close = "]"
	}
	return open + lower + "," + upper + close, true
}

// rangeBound writes one range bound, double-quoted when it contains spaces or
// characters that are part of the range syntax.
func rangeBound(oid uint32, v interface{}, bound pgtype.BoundType) (string, bool) {
	if bound == pgtype.Unbounded {
		return "", true
	}
	s, ok := pgText(oid, v)
	if ok && (s == "" || strings.ContainsAny(s, " \t\n\",\\()[]")) {
		s = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
	}
	return s, ok
}

// byteaHex returns binary data in PostgreSQL's hex format (\x0041ff), the same as COPY.
// Raw bytes would put NUL and invalid UTF-8 into text files.
func byteaHex(b []byte) string {
	return fmt.Sprintf(`\x%x`, b)
}

// numericValue returns a NUMERIC as Decimal, as a plain string for NaN and ±Infinity
// (JSON has no such numbers), or nil for NULL.
func numericValue(num pgtype.Numeric) interface{} {
	s, err := num.Value()
	if err != nil || s == nil {
		return nil
	}
	if num.NaN || num.InfinityModifier != pgtype.Finite {
		return s
	}
	return Decimal(s.(string))
}

var timeFormatReplacer = strings.NewReplacer(
	"yyyy", "2006",
	"yy", "06",
	"MM", "01",
	"dd", "02",
	"HH", "15",
	"mm", "04",
	"ss", "05",
	"SSS", "000", // Milliseconds
	"S", "0", // Deciseconds
)

// formatValue is kept for backward compatibility (not used in new code)
func formatValue(v interface{}, layout string, loc *time.Location) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case time.Time:
		return val.In(loc).Format(layout)
	case []byte:
		return string(val)
	case float32:
		return fmt.Sprintf("%.15g", val)
	case float64:
		return fmt.Sprintf("%.15g", val)
	default:
		return val
	}
}

// formatValueByOID is the central function that handles all PostgreSQL type conversions.
// It returns interface{} for maximum flexibility across different export formats.
func formatValueByOID(val interface{}, valueType uint32, userTimefmt string, timeZone string) interface{} {
	if val == nil {
		return nil
	}

	switch valueType {
	case pgtype.Float4OID, pgtype.Float8OID:
		// JSON has no NaN or Infinity: they would abort the export, so they become text.
		// Finite values stay numbers for JSON and YAML.
		if s, ok := floatText(val); ok && (s == "NaN" || strings.HasSuffix(s, "Infinity")) {
			return s
		}

	case pgtype.DateOID:
		if t, ok := val.(time.Time); ok {
			dateFmt := extractUserDateFormat(userTimefmt)
			layout := ConvertUserTimeFormat(dateFmt)
			return t.Format(layout)
		}

	case pgtype.TimestampOID:
		if t, ok := val.(time.Time); ok {
			layout := ConvertUserTimeFormat(userTimefmt)
			return t.Format(layout)
		}

	case pgtype.TimestamptzOID:
		if t, ok := val.(time.Time); ok {
			layout, loc := UserTimeZoneFormat(userTimefmt, timeZone)
			return t.In(loc).Format(layout)
		}

	case pgtype.UUIDOID:
		if uuid, ok := val.([16]byte); ok {
			return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
		}

	case pgtype.ByteaOID:
		if bytes, ok := val.([]byte); ok {
			return byteaHex(bytes)
		}

	case pgtype.NumericOID:
		if num, ok := val.(pgtype.Numeric); ok {
			return numericValue(num)
		}

	case pgtype.NumericArrayOID, pgtype.ByteaArrayOID:
		if arr, ok := val.([]interface{}); ok {
			out := make([]interface{}, len(arr))
			for i, elem := range arr {
				switch e := elem.(type) {
				case pgtype.Numeric:
					out[i] = numericValue(e)
				case []byte:
					out[i] = byteaHex(e)
				default:
					out[i] = elem
				}
			}
			return out
		}

	case pgtype.IntervalOID:
		if interval, ok := val.(pgtype.Interval); ok {
			if !interval.Valid {
				return nil
			}
			strVal, err := interval.Value()
			if err != nil {
				return nil
			}
			return strVal
		}

	case pgtype.JSONBOID, pgtype.JSONOID:
		// Return as-is for JSON export, will be marshaled for CSV/XML/YAML
		return val
	}

	// Types without special handling: PostgreSQL's text form rather than the Go structure
	// pgx decodes them into (a time would print as {49530123456 true}).
	if needsPGText(val) {
		if s, ok := pgText(valueType, val); ok {
			return s
		}
	}
	return val
}

// FormatJSONValue formats a PostgreSQL value for JSON export.
// Handles type-specific conversions including dates, timestamps, UUIDs, and numeric types.
func FormatJSONValue(val interface{}, valueType uint32, userTimefmt string, timeZone string) interface{} {
	return formatValueByOID(val, valueType, userTimefmt, timeZone)
}

// FormatCSVValue formats a PostgreSQL value for CSV export as a string.
// Handles type-specific conversions and ensures proper string representation for CSV format.
func FormatCSVValue(val interface{}, valueType uint32, userTimefmt string, timeZone string) string {
	// Arrays (and JSON arrays) as PostgreSQL writes them: quoted elements, NULL for nulls
	if arr, ok := val.([]interface{}); ok {
		if s, ok := pgText(valueType, arr); ok {
			return s
		}
	}
	result := formatValueByOID(val, valueType, userTimefmt, timeZone)

	if result == nil {
		return ""
	}

	// Handle specific conversions for CSV format
	switch v := result.(type) {
	case string:
		return v

	case float64, float32:
		s, _ := floatText(v)
		return s

	case []interface{}:
		if len(v) == 0 {
			return "{}"
		}
		elems := make([]string, len(v))
		for i, elem := range v {
			elems[i] = fmt.Sprintf("%v", elem)
		}
		return fmt.Sprintf("{%s}", strings.Join(elems, ","))

	default:
		// Special handling for JSON/JSONB in CSV
		if valueType == pgtype.JSONBOID || valueType == pgtype.JSONOID {
			jsonStr, err := json.Marshal(v)
			if err != nil {
				return "{}"
			}
			return string(jsonStr)
		}
		return fmt.Sprintf("%v", v)
	}
}

// FormatXMLValue formats a PostgreSQL value for XML export as a string.
// Handles type-specific conversions and ensures proper string representation for XML format.
func FormatXMLValue(val interface{}, valueType uint32, userTimefmt string, timeZone string) string {
	// Arrays (and JSON arrays) as PostgreSQL writes them: quoted elements, NULL for nulls
	if arr, ok := val.([]interface{}); ok {
		if s, ok := pgText(valueType, arr); ok {
			return s
		}
	}
	result := formatValueByOID(val, valueType, userTimefmt, timeZone)

	if result == nil {
		return ""
	}

	// Handle specific conversions for XML format
	switch v := result.(type) {
	case string:
		return v

	case float64, float32:
		s, _ := floatText(v)
		return s

	case []interface{}:
		if len(v) == 0 {
			return "{}"
		}
		elems := make([]string, len(v))
		for i, elem := range v {
			elems[i] = fmt.Sprintf("%v", elem)
		}
		return fmt.Sprintf("{%s}", strings.Join(elems, ","))

	default:
		// Special handling for JSON/JSONB in XML
		if valueType == pgtype.JSONBOID || valueType == pgtype.JSONOID {
			jsonStr, err := json.Marshal(v)
			if err != nil {
				return ""
			}
			return string(jsonStr)
		}
		return fmt.Sprintf("%v", v)
	}
}

// FormatYAMLValue formats a PostgreSQL value for YAML export.
// Handles type-specific conversions including dates, timestamps, UUIDs, and numeric types.
func FormatYAMLValue(val interface{}, valueType uint32, userTimefmt string, timeZone string) interface{} {
	return formatValueByOID(val, valueType, userTimefmt, timeZone)
}

// FormatSQLValue formats a PostgreSQL value for SQL INSERT statement export.
// Returns a properly formatted SQL literal with type casting (e.g., 'value'::type).
func FormatSQLValue(val interface{}, valueType uint32) string {
	if val == nil {
		return "NULL"
	}

	switch valueType {
	case pgtype.DateOID:
		if t, ok := val.(time.Time); ok {
			return fmt.Sprintf("'%s'::date", t.Format("2006-01-02"))
		}

	case pgtype.TimestampOID:
		if t, ok := val.(time.Time); ok {
			return fmt.Sprintf("'%s'::timestamp", t.Format("2006-01-02 15:04:05.999999"))
		}

	case pgtype.TimestamptzOID:
		// Written in UTC: the file no longer depends on the exporting machine's time zone,
		// and a full +00:00 offset avoids losing half-hour offsets such as +05:30.
		if t, ok := val.(time.Time); ok {
			return fmt.Sprintf("'%s'::timestamptz", t.UTC().Format("2006-01-02 15:04:05.999999-07:00"))
		}

	case pgtype.UUIDOID:
		if uuid, ok := val.([16]byte); ok {
			return fmt.Sprintf("'%x-%x-%x-%x-%x'::uuid", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
		}

	case pgtype.ByteaOID:
		if bytes, ok := val.([]byte); ok {
			return fmt.Sprintf("'%s'::bytea", byteaHex(bytes))
		}

	case pgtype.BoolOID:
		if b, ok := val.(bool); ok {
			if b {
				return "true"
			}
			return "false"
		}

	case pgtype.NumericOID:
		if num, ok := val.(pgtype.Numeric); ok {
			switch v := numericValue(num).(type) {
			case nil:
				return "NULL"
			case Decimal:
				return string(v)
			default:
				return fmt.Sprintf("'%s'::numeric", v)
			}
		}

	case pgtype.IntervalOID:
		if interval, ok := val.(pgtype.Interval); ok {
			if !interval.Valid {
				return "NULL"
			}
			strVal, err := interval.Value()
			if err != nil {
				return "NULL"
			}
			return fmt.Sprintf("'%v'::interval", strVal)
		}

	case pgtype.JSONBOID:
		jsonStr, err := json.Marshal(val)
		if err != nil {
			return "'{}'::jsonb"
		}
		return fmt.Sprintf("'%s'::jsonb", strings.ReplaceAll(string(jsonStr), "'", "''"))

	case pgtype.JSONOID:
		jsonStr, err := json.Marshal(val)
		if err != nil {
			return "'{}'::json"
		}
		return fmt.Sprintf("'%s'::json", strings.ReplaceAll(string(jsonStr), "'", "''"))
	}

	// Generic SQL value formatting
	switch v := val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)

	case float32, float64:
		s, _ := floatText(val)
		// A bare NaN would be read as a column name, and a bare -0 as the integer 0
		if s == "NaN" || strings.HasSuffix(s, "Infinity") || s == "-0" {
			return fmt.Sprintf("'%s'::float8", s)
		}
		return s

	case []interface{}:
		if s, ok := pgText(valueType, v); ok {
			return "'" + strings.ReplaceAll(s, "'", "''") + "'"
		}
		// Fallback for arrays pgx cannot encode (e.g. arrays of a custom enum)
		if len(v) == 0 {
			return "'{}'"
		}
		elems := make([]string, len(v))
		for i, elem := range v {
			elems[i] = sqlArrayElem(elem)
		}
		return fmt.Sprintf("'{%s}'", strings.ReplaceAll(strings.Join(elems, ","), "'", "''"))

	default:
		str := fmt.Sprintf("%v", val)
		if needsPGText(val) {
			if s, ok := pgText(valueType, val); ok {
				str = s
			}
		}
		escaped := strings.ReplaceAll(str, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}
}

// sqlArrayElem formats one element of a PostgreSQL array literal.
// Non-numeric elements are double-quoted so commas, braces, quotes and spaces stay inside the element.
func sqlArrayElem(elem interface{}) string {
	switch e := elem.(type) {
	case pgtype.Numeric:
		elem = numericValue(e)
	case []byte:
		elem = byteaHex(e)
	case time.Time:
		// PostgreSQL ignores the offset for timestamp[] elements, so one format fits timestamp[] and timestamptz[].
		elem = e.UTC().Format("2006-01-02 15:04:05.999999-07:00")
	}
	switch elem.(type) {
	case nil:
		return "NULL"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool, Decimal:
		return fmt.Sprintf("%v", elem)
	}
	s := strings.ReplaceAll(fmt.Sprintf("%v", elem), `\`, `\\`)
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}

// FormatXLSXValue formats a PostgreSQL value for Excel XLSX export.
// Preserves native types (dates, times) for Excel compatibility and converts complex types to JSON strings.
func FormatXLSXValue(value interface{}, oid uint32, timeFormat, timeZone string) interface{} {

	if pgtype.DateOID == oid || pgtype.TimestampOID == oid || pgtype.TimestamptzOID == oid {
		return value
	}

	if pgtype.JSONBOID == oid || pgtype.JSONOID == oid {
		jsonStr, err := json.Marshal(value)
		if err != nil {
			return "{}"
		}
		return string(jsonStr)
	}

	if val, ok := value.([]interface{}); ok {
		b, err := json.Marshal(val)
		if err != nil {
			return "[]"
		}
		return string(b)
	}

	result := formatValueByOID(value, oid, timeFormat, timeZone)
	// Excel stores numbers as float64, so a numeric cell beats exact text here.
	if d, ok := result.(Decimal); ok {
		if f, err := strconv.ParseFloat(string(d), 64); err == nil {
			return f
		}
	}
	return result
}

// FormatTemplateValue formats a PostgreSQL value for template-based exports.
// Converts complex types (JSON, arrays) to JSON strings for template rendering.
func FormatTemplateValue(val interface{}, oid uint32, userTimefmt string, timeZone string) interface{} {

	if val == nil {
		return nil
	}

	base := formatValueByOID(val, oid, userTimefmt, timeZone)

	if oid == pgtype.JSONBOID || oid == pgtype.JSONOID {
		b, err := json.Marshal(base)
		if err != nil {
			return "{}"
		}
		return string(b)
	}

	if arr, ok := base.([]interface{}); ok {
		b, err := json.Marshal(arr)
		if err != nil {
			return "[]"
		}
		return string(b)
	}

	return base
}

// QuoteIdent quotes a PostgreSQL identifier (table or column name).
// Handles schema-qualified names (e.g., "schema"."table") and escapes double quotes.
func QuoteIdent(s string) string {
	parts := strings.Split(s, ".")
	for i, part := range parts {
		parts[i] = `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

// UserTimeZoneFormat converts a user time format string to Go time layout and loads the timezone.
// Returns the Go time layout format and the timezone location.
// Falls back to local timezone if the provided timezone is invalid.
func UserTimeZoneFormat(userTimefmt string, timeZone string) (string, *time.Location) {

	layout := ConvertUserTimeFormat(userTimefmt)

	if timeZone == "" {
		return layout, time.Local
	}

	// Called for every timestamptz cell: time.LoadLocation reads the zone file from disk
	// on each call (~11µs), so each name is loaded once. nil marks an invalid name.
	cached, ok := locations.Load(timeZone)
	if !ok {
		loc, err := time.LoadLocation(timeZone)
		if err != nil {
			log.Printf("Warning: Invalid timezone %q, using local time: %v", timeZone, err)
			loc = nil
		}
		cached, _ = locations.LoadOrStore(timeZone, loc)
	}
	if loc := cached.(*time.Location); loc != nil {
		return layout, loc
	}
	return layout, time.Local
}

// locations caches loaded time zones by name (see UserTimeZoneFormat).
var locations sync.Map

// ConvertUserTimeFormat converts a user-friendly time format (e.g., "yyyy-MM-dd HH:mm:ss")
// to Go's time layout format (e.g., "2006-01-02 15:04:05").
func ConvertUserTimeFormat(userTimefmt string) string {
	return timeFormatReplacer.Replace(userTimefmt)
}

// extractUserDateFormat extracts only the date portion from a datetime format string.
// For example, "yyyy-MM-dd HH:mm:ss" becomes "yyyy-MM-dd".
// This ensures DATE columns are exported without time components.
func extractUserDateFormat(userFmt string) string {
	dateTokens := []string{"yyyy", "yy", "MM", "dd"}
	last := -1
	for _, tok := range dateTokens {
		idx := strings.LastIndex(userFmt, tok)
		if idx != -1 {
			end := idx + len(tok)
			if end > last {
				last = end
			}
		}
	}

	if last == -1 {
		// No date tokens found, return original
		return userFmt
	}
	return strings.TrimSpace(userFmt[:last])
}
