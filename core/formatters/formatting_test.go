package formatters

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"gopkg.in/yaml.v3"
)

func TestConvertUserTimeFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ISO 8601 format",
			input:    "yyyy-MM-dd HH:mm:ss",
			expected: "2006-01-02 15:04:05",
		},
		{
			name:     "ISO 8601 with milliseconds",
			input:    "yyyy-MM-ddTHH:mm:ss.SSS",
			expected: "2006-01-02T15:04:05.000",
		},
		{
			name:     "European format",
			input:    "dd/MM/yyyy HH:mm:ss",
			expected: "02/01/2006 15:04:05",
		},
		{
			name:     "US format",
			input:    "MM/dd/yyyy HH:mm:ss",
			expected: "01/02/2006 15:04:05",
		},
		{
			name:     "Short year format",
			input:    "yy-MM-dd",
			expected: "06-01-02",
		},
		{
			name:     "With deciseconds",
			input:    "yyyy-MM-dd HH:mm:ss.S",
			expected: "2006-01-02 15:04:05.0",
		},
		{
			name:     "Date only",
			input:    "yyyy-MM-dd",
			expected: "2006-01-02",
		},
		{
			name:     "Time only",
			input:    "HH:mm:ss",
			expected: "15:04:05",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertUserTimeFormat(tt.input)
			if result != tt.expected {
				t.Errorf("convertUserTimeFormat(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExtractUserDateFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "datetime format extracts date only",
			input:    "yyyy-MM-dd HH:mm:ss",
			expected: "yyyy-MM-dd",
		},
		{
			name:     "date only format unchanged",
			input:    "yyyy-MM-dd",
			expected: "yyyy-MM-dd",
		},
		{
			name:     "European datetime format",
			input:    "dd/MM/yyyy HH:mm:ss",
			expected: "dd/MM/yyyy",
		},
		{
			name:     "US datetime format",
			input:    "MM/dd/yyyy HH:mm:ss",
			expected: "MM/dd/yyyy",
		},
		{
			name:     "time only format unchanged",
			input:    "HH:mm:ss",
			expected: "HH:mm:ss",
		},
		{
			name:     "no date tokens returns original",
			input:    "HH:mm",
			expected: "HH:mm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractUserDateFormat(tt.input)
			if result != tt.expected {
				t.Errorf("extractUserDateFormat(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	testTime := time.Date(2024, 3, 15, 14, 30, 45, 123000000, time.UTC)
	layout := "2006-01-02 15:04:05"
	loc := time.UTC

	tests := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{
			name:     "nil value",
			value:    nil,
			expected: nil,
		},
		{
			name:     "time.Time value",
			value:    testTime,
			expected: "2024-03-15 14:30:45",
		},
		{
			name:     "[]byte value",
			value:    []byte("test data"),
			expected: "test data",
		},
		{
			name:     "float32 value",
			value:    float32(3.14159),
			expected: "3.1415901184082",
		},
		{
			name:     "float64 value",
			value:    float64(2.718281828459045),
			expected: "2.71828182845905",
		},
		{
			name:     "string value",
			value:    "test string",
			expected: "test string",
		},
		{
			name:     "int value",
			value:    42,
			expected: 42,
		},
		{
			name:     "bool value",
			value:    true,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValue(tt.value, layout, loc)

			// For floats, we need to compare strings since the formatting might differ slightly
			if _, ok := tt.value.(float32); ok {
				if result.(string) != tt.expected.(string) {
					t.Errorf("formatValue(%v) = %v, want %v", tt.value, result, tt.expected)
				}
			} else if _, ok := tt.value.(float64); ok {
				if result.(string) != tt.expected.(string) {
					t.Errorf("formatValue(%v) = %v, want %v", tt.value, result, tt.expected)
				}
			} else if result != tt.expected {
				t.Errorf("formatValue(%v) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestFormatValueByOID(t *testing.T) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)
	testTimestamp := time.Date(2024, 3, 15, 14, 30, 45, 123000000, time.UTC)
	testUUID := [16]byte{0x9f, 0x4d, 0xaf, 0x39, 0x5b, 0x76, 0x4b, 0x9c, 0xa1, 0x47, 0x82, 0x0f, 0x8f, 0x0c, 0x94, 0x5f}

	tests := []struct {
		name      string
		val       interface{}
		valueType uint32
		timefmt   string
		timezone  string
		expected  interface{}
	}{
		{
			name:      "nil value",
			val:       nil,
			valueType: pgtype.TextOID,
			timefmt:   "yyyy-MM-dd",
			timezone:  "",
			expected:  nil,
		},
		{
			name:      "Date with date-only format",
			val:       testDate,
			valueType: pgtype.DateOID,
			timefmt:   "yyyy-MM-dd",
			timezone:  "",
			expected:  "2021-09-25",
		},
		{
			name:      "Date with datetime format extracts date",
			val:       testDate,
			valueType: pgtype.DateOID,
			timefmt:   "yyyy-MM-dd HH:mm:ss",
			timezone:  "",
			expected:  "2021-09-25",
		},
		{
			name:      "Date with European format",
			val:       testDate,
			valueType: pgtype.DateOID,
			timefmt:   "dd/MM/yyyy",
			timezone:  "",
			expected:  "25/09/2021",
		},
		{
			name:      "Timestamp without timezone",
			val:       testTimestamp,
			valueType: pgtype.TimestampOID,
			timefmt:   "yyyy-MM-dd HH:mm:ss",
			timezone:  "",
			expected:  "2024-03-15 14:30:45",
		},
		{
			name:      "Timestamp with milliseconds",
			val:       testTimestamp,
			valueType: pgtype.TimestampOID,
			timefmt:   "yyyy-MM-dd HH:mm:ss.SSS",
			timezone:  "",
			expected:  "2024-03-15 14:30:45.123",
		},
		{
			name:      "Timestamptz with UTC",
			val:       testTimestamp,
			valueType: pgtype.TimestamptzOID,
			timefmt:   "yyyy-MM-dd HH:mm:ss",
			timezone:  "UTC",
			expected:  "2024-03-15 14:30:45",
		},
		{
			name:      "Timestamptz with Europe/Paris",
			val:       testTimestamp,
			valueType: pgtype.TimestamptzOID,
			timefmt:   "yyyy-MM-dd HH:mm:ss",
			timezone:  "Europe/Paris",
			expected:  "2024-03-15 15:30:45", // UTC+1 in March
		},
		{
			name:      "UUID formatting",
			val:       testUUID,
			valueType: pgtype.UUIDOID,
			timefmt:   "",
			timezone:  "",
			expected:  "9f4daf39-5b76-4b9c-a147-820f8f0c945f",
		},
		{
			name:      "Bytea to hex",
			val:       []byte{0x00, 'A', '\\', 0xff},
			valueType: pgtype.ByteaOID,
			timefmt:   "",
			timezone:  "",
			expected:  `\x00415cff`,
		},
		{
			name:      "Valid Numeric",
			val:       pgtype.Numeric{Int: big.NewInt(125075), Exp: -2, Valid: true},
			valueType: pgtype.NumericOID,
			timefmt:   "",
			timezone:  "",
			expected:  Decimal("1250.75"),
		},
		{
			name:      "Invalid Numeric",
			val:       pgtype.Numeric{Valid: false},
			valueType: pgtype.NumericOID,
			timefmt:   "",
			timezone:  "",
			expected:  nil,
		},
		{
			name:      "Generic string value",
			val:       "test string",
			valueType: pgtype.TextOID,
			timefmt:   "",
			timezone:  "",
			expected:  "test string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValueByOID(tt.val, tt.valueType, tt.timefmt, tt.timezone)
			if result != tt.expected {
				t.Errorf("formatValueByOID() = %v (type %T), want %v (type %T)",
					result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestFormatCSVValue(t *testing.T) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)
	testArray := []interface{}{"pgxport", "go", "json"}

	tests := []struct {
		name      string
		val       interface{}
		valueType uint32
		timefmt   string
		timezone  string
		expected  string
	}{
		{
			name:      "nil value returns empty string",
			val:       nil,
			valueType: pgtype.TextOID,
			timefmt:   "",
			timezone:  "",
			expected:  "",
		},
		{
			name:      "Date formatting",
			val:       testDate,
			valueType: pgtype.DateOID,
			timefmt:   "yyyy-MM-dd",
			timezone:  "",
			expected:  "2021-09-25",
		},
		{
			name:      "Float32 formatting",
			val:       float32(3.14159),
			valueType: pgtype.Float4OID,
			timefmt:   "",
			timezone:  "",
			expected:  "3.1415901184082",
		},
		{
			name:      "Float64 formatting",
			val:       float64(1250.75),
			valueType: pgtype.Float8OID,
			timefmt:   "",
			timezone:  "",
			expected:  "1250.75",
		},
		{
			name:      "Array formatting",
			val:       testArray,
			valueType: pgtype.TextArrayOID,
			timefmt:   "",
			timezone:  "",
			expected:  "{pgxport,go,json}",
		},
		{
			name:      "Empty array",
			val:       []interface{}{},
			valueType: pgtype.TextArrayOID,
			timefmt:   "",
			timezone:  "",
			expected:  "{}",
		},
		{
			name:      "String value",
			val:       "test string",
			valueType: pgtype.TextOID,
			timefmt:   "",
			timezone:  "",
			expected:  "test string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatCSVValue(tt.val, tt.valueType, tt.timefmt, tt.timezone)
			if result != tt.expected {
				t.Errorf("FormatCSVValue() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestFormatJSONValue(t *testing.T) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		val       interface{}
		valueType uint32
		timefmt   string
		timezone  string
		expected  interface{}
	}{
		{
			name:      "nil value",
			val:       nil,
			valueType: pgtype.TextOID,
			timefmt:   "",
			timezone:  "",
			expected:  nil,
		},
		{
			name:      "Date formatting",
			val:       testDate,
			valueType: pgtype.DateOID,
			timefmt:   "yyyy-MM-dd",
			timezone:  "",
			expected:  "2021-09-25",
		},
		{
			name:      "String value",
			val:       "test string",
			valueType: pgtype.TextOID,
			timefmt:   "",
			timezone:  "",
			expected:  "test string",
		},
		{
			name:      "Float value kept as number",
			val:       float64(1250.75),
			valueType: pgtype.Float8OID,
			timefmt:   "",
			timezone:  "",
			expected:  float64(1250.75),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatJSONValue(tt.val, tt.valueType, tt.timefmt, tt.timezone)
			if result != tt.expected {
				t.Errorf("formatJSONValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFormatXMLValue(t *testing.T) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		val       interface{}
		valueType uint32
		timefmt   string
		timezone  string
		expected  string
	}{
		{
			name:      "nil value returns empty string",
			val:       nil,
			valueType: pgtype.TextOID,
			timefmt:   "",
			timezone:  "",
			expected:  "",
		},
		{
			name:      "Date formatting",
			val:       testDate,
			valueType: pgtype.DateOID,
			timefmt:   "yyyy-MM-dd",
			timezone:  "",
			expected:  "2021-09-25",
		},
		{
			name:      "String value",
			val:       "test string",
			valueType: pgtype.TextOID,
			timefmt:   "",
			timezone:  "",
			expected:  "test string",
		},
		{
			name:      "Float formatting",
			val:       float64(1250.75),
			valueType: pgtype.Float8OID,
			timefmt:   "",
			timezone:  "",
			expected:  "1250.75",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatXMLValue(tt.val, tt.valueType, tt.timefmt, tt.timezone)
			if result != tt.expected {
				t.Errorf("formatXMLValue() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestFormatSQLValue(t *testing.T) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)
	testTimestamp := time.Date(2024, 3, 15, 14, 30, 45, 123000000, time.UTC)
	testUUID := [16]byte{0x9f, 0x4d, 0xaf, 0x39, 0x5b, 0x76, 0x4b, 0x9c, 0xa1, 0x47, 0x82, 0x0f, 0x8f, 0x0c, 0x94, 0x5f}

	tests := []struct {
		name      string
		value     interface{}
		valueType uint32
		expected  string
	}{
		{
			name:      "nil value",
			value:     nil,
			valueType: 0,
			expected:  "NULL",
		},
		{
			name:      "Date with cast",
			value:     testDate,
			valueType: pgtype.DateOID,
			expected:  "'2021-09-25'::date",
		},
		{
			name:      "Timestamp with cast",
			value:     testTimestamp,
			valueType: pgtype.TimestampOID,
			expected:  "'2024-03-15 14:30:45.123'::timestamp",
		},
		{
			name:      "Timestamp keeps microseconds",
			value:     time.Date(2024, 3, 15, 14, 30, 45, 123456000, time.UTC),
			valueType: pgtype.TimestampOID,
			expected:  "'2024-03-15 14:30:45.123456'::timestamp",
		},
		{
			name:      "Timestamptz with cast",
			value:     testTimestamp,
			valueType: pgtype.TimestamptzOID,
			expected:  "'2024-03-15 14:30:45.123+00:00'::timestamptz",
		},
		{
			name:      "Timestamptz in half-hour zone is written in UTC",
			value:     time.Date(2024, 3, 15, 16, 0, 0, 123456000, time.FixedZone("IST", 5*3600+30*60)),
			valueType: pgtype.TimestamptzOID,
			expected:  "'2024-03-15 10:30:00.123456+00:00'::timestamptz",
		},
		{
			name:      "UUID with cast",
			value:     testUUID,
			valueType: pgtype.UUIDOID,
			expected:  "'9f4daf39-5b76-4b9c-a147-820f8f0c945f'::uuid",
		},
		{
			name:      "string value",
			value:     "test string",
			valueType: pgtype.TextOID,
			expected:  "'test string'",
		},
		{
			name:      "string with single quote",
			value:     "O'Brien",
			valueType: pgtype.TextOID,
			expected:  "'O''Brien'",
		},
		{
			name:      "string with multiple quotes",
			value:     "It's a 'test'",
			valueType: pgtype.TextOID,
			expected:  "'It''s a ''test'''",
		},
		{
			name:      "bytea value with cast",
			value:     []byte("Hi"),
			valueType: pgtype.ByteaOID,
			expected:  `'\x4869'::bytea`,
		},
		{
			name:      "bytea with NUL, quote, backslash and invalid UTF-8",
			value:     []byte{0x00, '\'', '\\', 0xff},
			valueType: pgtype.ByteaOID,
			expected:  `'\x00275cff'::bytea`,
		},
		{
			name:      "bool true",
			value:     true,
			valueType: pgtype.BoolOID,
			expected:  "true",
		},
		{
			name:      "bool false",
			value:     false,
			valueType: pgtype.BoolOID,
			expected:  "false",
		},
		{
			name:      "int value",
			value:     int(42),
			valueType: pgtype.Int4OID,
			expected:  "42",
		},
		{
			name:      "int64 value",
			value:     int64(9223372036854775807),
			valueType: pgtype.Int8OID,
			expected:  "9223372036854775807",
		},
		{
			name:      "float32 value",
			value:     float32(3.14159),
			valueType: pgtype.Float4OID,
			expected:  "3.1415901184082",
		},
		{
			name:      "float64 value",
			value:     float64(2.718281828459045),
			valueType: pgtype.Float8OID,
			expected:  "2.71828182845905",
		},
		{
			name:      "negative int",
			value:     int(-42),
			valueType: pgtype.Int4OID,
			expected:  "-42",
		},
		{
			name:      "negative float",
			value:     float64(-3.14),
			valueType: pgtype.Float4OID,
			expected:  "-3.14",
		},
		{
			name:      "array value",
			value:     []interface{}{1, 2, 3},
			valueType: pgtype.Int4ArrayOID,
			expected:  "'{1,2,3}'",
		},
		{
			name:      "empty array",
			value:     []interface{}{},
			valueType: pgtype.Int4ArrayOID,
			expected:  "'{}'",
		},
		{
			name:      "jsonb with single quote",
			value:     map[string]interface{}{"name": "O'Brien"},
			valueType: pgtype.JSONBOID,
			expected:  `'{"name":"O''Brien"}'::jsonb`,
		},
		{
			name:      "json with single quote",
			value:     map[string]interface{}{"name": "O'Brien"},
			valueType: pgtype.JSONOID,
			expected:  `'{"name":"O''Brien"}'::json`,
		},
		{
			name:      "text array with injection attempt",
			value:     []interface{}{"x'); DROP TABLE users; --"},
			valueType: pgtype.TextArrayOID,
			expected:  `'{"x''); DROP TABLE users; --"}'`,
		},
		{
			name:      "text array with comma, quote, backslash and NULL",
			value:     []interface{}{"a,b", `say "hi"`, `C:\dir`, nil},
			valueType: pgtype.TextArrayOID,
			expected:  `'{"a,b","say \"hi\"","C:\\dir",NULL}'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatSQLValue(tt.value, tt.valueType)
			if result != tt.expected {
				t.Errorf("FormatSQLValue(%v) = %q, want %q", tt.value, result, tt.expected)
			}
		})
	}
}

func TestFormatValueWithDifferentTimezones(t *testing.T) {
	testTime := time.Date(2024, 3, 15, 14, 30, 45, 0, time.UTC)
	layout := "2006-01-02 15:04:05"

	tests := []struct {
		name     string
		timezone string
		expected string
	}{
		{
			name:     "UTC timezone",
			timezone: "UTC",
			expected: "2024-03-15 14:30:45",
		},
		{
			name:     "America/New_York timezone",
			timezone: "America/New_York",
			expected: "2024-03-15 10:30:45", // UTC-4 in March (EDT)
		},
		{
			name:     "Europe/Paris timezone",
			timezone: "Europe/Paris",
			expected: "2024-03-15 15:30:45", // UTC+1 in March (CET)
		},
		{
			name:     "Asia/Tokyo timezone",
			timezone: "Asia/Tokyo",
			expected: "2024-03-15 23:30:45", // UTC+9
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc, err := time.LoadLocation(tt.timezone)
			if err != nil {
				t.Skipf("Timezone %s not available: %v", tt.timezone, err)
			}

			result := formatValue(testTime, layout, loc)
			if result != tt.expected {
				t.Errorf("formatValue with timezone %s = %v, want %v", tt.timezone, result, tt.expected)
			}
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "users",
			expected: `"users"`,
		},
		{
			name:     "schema.table",
			input:    "public.users",
			expected: `"public"."users"`,
		},
		{
			name:     "schema.table.column",
			input:    "public.users.id",
			expected: `"public"."users"."id"`,
		},
		{
			name:     "identifier with quotes",
			input:    `table"name`,
			expected: `"table""name"`,
		},
		{
			name:     "schema with quotes",
			input:    `schema"1.table"2`,
			expected: `"schema""1"."table""2"`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
		{
			name:     "special characters",
			input:    "table-name",
			expected: `"table-name"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteIdent(tt.input)
			if result != tt.expected {
				t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestUserTimeZoneFormat(t *testing.T) {
	tests := []struct {
		name            string
		userTimefmt     string
		timeZone        string
		expectedLayout  string
		expectedLocName string
	}{
		{
			name:            "ISO format with UTC",
			userTimefmt:     "yyyy-MM-dd HH:mm:ss",
			timeZone:        "UTC",
			expectedLayout:  "2006-01-02 15:04:05",
			expectedLocName: "UTC",
		},
		{
			name:            "European format with Paris timezone",
			userTimefmt:     "dd/MM/yyyy HH:mm:ss",
			timeZone:        "Europe/Paris",
			expectedLayout:  "02/01/2006 15:04:05",
			expectedLocName: "Europe/Paris",
		},
		{
			name:            "Empty timezone defaults to Local",
			userTimefmt:     "yyyy-MM-dd",
			timeZone:        "",
			expectedLayout:  "2006-01-02",
			expectedLocName: "Local",
		},
		{
			name:            "Invalid timezone defaults to Local",
			userTimefmt:     "yyyy-MM-dd HH:mm:ss",
			timeZone:        "Invalid/Timezone",
			expectedLayout:  "2006-01-02 15:04:05",
			expectedLocName: "Local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout, loc := UserTimeZoneFormat(tt.userTimefmt, tt.timeZone)

			if layout != tt.expectedLayout {
				t.Errorf("userTimeZoneFormat() layout = %q, want %q", layout, tt.expectedLayout)
			}

			if loc.String() != tt.expectedLocName {
				t.Errorf("userTimeZoneFormat() location = %q, want %q", loc.String(), tt.expectedLocName)
			}
		})
	}
}

// Benchmark tests
func BenchmarkFormatValue(b *testing.B) {
	testTime := time.Date(2024, 3, 15, 14, 30, 45, 0, time.UTC)
	layout := "2006-01-02 15:04:05"
	loc := time.UTC

	b.Run("time", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValue(testTime, layout, loc)
		}
	})

	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValue("test string", layout, loc)
		}
	})

	b.Run("float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValue(3.14159, layout, loc)
		}
	})
}

func BenchmarkFormatValueByOID(b *testing.B) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)

	b.Run("date", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValueByOID(testDate, pgtype.DateOID, "yyyy-MM-dd", "")
		}
	})

	b.Run("timestamp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValueByOID(testDate, pgtype.TimestampOID, "yyyy-MM-dd HH:mm:ss", "")
		}
	})

	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			formatValueByOID("test string", pgtype.TextOID, "", "")
		}
	})
}

func BenchmarkFormatCSVValue(b *testing.B) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)

	b.Run("date", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatCSVValue(testDate, pgtype.DateOID, "yyyy-MM-dd", "")
		}
	})

	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatCSVValue("test string", pgtype.TextOID, "", "")
		}
	})

	b.Run("float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatCSVValue(3.14159, pgtype.Float4OID, "", "")
		}
	})
}

func BenchmarkFormatSQLValue(b *testing.B) {
	testDate := time.Date(2021, 9, 25, 0, 0, 0, 0, time.UTC)

	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatSQLValue("test string", pgtype.TextOID)
		}
	})

	b.Run("string_with_quotes", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatSQLValue("O'Brien's test", pgtype.TextOID)
		}
	})

	b.Run("int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatSQLValue(42, pgtype.Int4OID)
		}
	})

	b.Run("float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatSQLValue(3.14159, pgtype.Float4OID)
		}
	})

	b.Run("date", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FormatSQLValue(testDate, pgtype.DateOID)
		}
	})
}

// TestNumericExact checks that NUMERIC values keep every digit in every format
// and that NaN/Infinity do not break JSON or SQL.
func TestNumericExact(t *testing.T) {
	big1 := pgtype.Numeric{Int: big.NewInt(1234567890123456789), Exp: -2, Valid: true} // 12345678901234567.89
	trailingZero := pgtype.Numeric{Int: big.NewInt(123450), Exp: -2, Valid: true}     // 1234.50
	nan := pgtype.Numeric{NaN: true, Valid: true}
	inf := pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}
	null := pgtype.Numeric{Valid: false}

	if got := FormatCSVValue(big1, pgtype.NumericOID, "", ""); got != "12345678901234567.89" {
		t.Errorf("CSV big = %q", got)
	}
	if got := FormatCSVValue(trailingZero, pgtype.NumericOID, "", ""); got != "1234.50" {
		t.Errorf("CSV trailing zero = %q", got)
	}
	if got := FormatCSVValue(nan, pgtype.NumericOID, "", ""); got != "NaN" {
		t.Errorf("CSV NaN = %q", got)
	}

	sqlCases := map[string]struct {
		val  pgtype.Numeric
		want string
	}{
		"big":      {big1, "12345678901234567.89"},
		"NaN":      {nan, "'NaN'::numeric"},
		"Infinity": {inf, "'Infinity'::numeric"},
		"NULL":     {null, "NULL"},
	}
	for name, c := range sqlCases {
		if got := FormatSQLValue(c.val, pgtype.NumericOID); got != c.want {
			t.Errorf("SQL %s = %q, want %q", name, got, c.want)
		}
	}

	for _, c := range []struct {
		val  pgtype.Numeric
		want string
	}{{big1, "12345678901234567.89"}, {nan, `"NaN"`}, {inf, `"Infinity"`}} {
		b, err := json.Marshal(FormatJSONValue(c.val, pgtype.NumericOID, "", ""))
		if err != nil || string(b) != c.want {
			t.Errorf("JSON = %s (err %v), want %s", b, err, c.want)
		}
	}

	// Same path as OrderedYamlEncoder: encode the value into a node.
	var node yaml.Node
	if err := node.Encode(FormatYAMLValue(big1, pgtype.NumericOID, "", "")); err != nil {
		t.Fatalf("YAML encode: %v", err)
	}
	if y, _ := yaml.Marshal(&node); string(y) != "12345678901234567.89\n" {
		t.Errorf("YAML = %q", y)
	}

	if got := FormatXLSXValue(trailingZero, pgtype.NumericOID, "", ""); got != 1234.5 {
		t.Errorf("XLSX = %v (%T), want float64 1234.5", got, got)
	}

	arr := []interface{}{pgtype.Numeric{Int: big.NewInt(150), Exp: -2, Valid: true}, nan}
	if got := FormatSQLValue(arr, pgtype.NumericArrayOID); got != `'{1.50,"NaN"}'` {
		t.Errorf("SQL numeric[] = %q", got)
	}
	if got := FormatCSVValue(arr, pgtype.NumericArrayOID, "", ""); got != "{1.50,NaN}" {
		t.Errorf("CSV numeric[] = %q", got)
	}
}

func BenchmarkQuoteIdent(b *testing.B) {
	b.Run("simple", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			QuoteIdent("users")
		}
	})

	b.Run("schema_table", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			QuoteIdent("public.users")
		}
	})

	b.Run("with_quotes", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			QuoteIdent(`table"name`)
		}
	})
}
