package exporters

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/elliotchance/orderedmap/v3"
	"github.com/fbz-tec/pgxport/core/encoders"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

// TestWriteYAMLStreamingMatchesWholeDocument checks that writing rows one by one gives
// exactly the file the previous version built in memory as a single YAML list.
func TestWriteYAMLStreamingMatchesWholeDocument(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	query := `SELECT g AS id, 'line1' || chr(10) || 'line2: "quoted"' AS text, NULL AS empty,
		12.50::numeric AS amount, '{"a": {"b": [1, 2]}}'::jsonb AS meta, ARRAY['x', 'y z'] AS tags,
		'2024-01-15'::date AS day, g % 2 = 0 AS even, '- not a list' AS dash
		FROM generate_series(1, 3) g`
	options := ExportOptions{Format: FormatYAML, Compression: "none", TimeFormat: "yyyy-MM-dd HH:mm:ss"}

	// Reference: the whole document built in memory and encoded once, as before.
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	fields := rows.FieldDescriptions()
	rowEncoder := encoders.NewOrderedYamlEncoder(options.TimeFormat, options.TimeZone)
	rootSeq := &yaml.Node{Kind: yaml.SequenceNode}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("Failed to read row: %v", err)
		}
		rowData := orderedmap.NewOrderedMap[string, encoders.DataParams]()
		for i, fd := range fields {
			rowData.Set(fd.Name, encoders.DataParams{Value: values[i], ValueType: fd.DataTypeOID})
		}
		rowNode, err := rowEncoder.EncodeRow(rowData)
		if err != nil {
			t.Fatalf("Failed to encode row: %v", err)
		}
		rootSeq.Content = append(rootSeq.Content, rowNode)
	}
	rows.Close()
	var want bytes.Buffer
	enc := yaml.NewEncoder(&want)
	enc.SetIndent(2)
	if err := enc.Encode(rootSeq); err != nil {
		t.Fatalf("Failed to encode reference: %v", err)
	}
	enc.Close()

	got := exportYAML(t, conn, query, options)
	if !bytes.Equal(got, want.Bytes()) {
		t.Errorf("streamed YAML differs from whole document:\n--- got\n%s\n--- want\n%s", got, want.Bytes())
	}

	var parsed []map[string]any
	if err := yaml.Unmarshal(got, &parsed); err != nil || len(parsed) != 3 {
		t.Errorf("expected a list of 3 rows, got %d (err %v)", len(parsed), err)
	}
}

func TestWriteYAMLNoRows(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	got := exportYAML(t, conn, "SELECT 1 AS id WHERE false", ExportOptions{Format: FormatYAML, Compression: "none"})
	if string(got) != "[]\n" {
		t.Errorf("no rows: got %q, want %q", got, "[]\n")
	}
}

// exportYAML runs query through the YAML exporter and returns the file content.
func exportYAML(t *testing.T, conn *pgx.Conn, query string, options ExportOptions) []byte {
	t.Helper()
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	options.OutputPath = filepath.Join(t.TempDir(), "output.yaml")
	exporter, err := Get(FormatYAML)
	if err != nil {
		t.Fatalf("Failed to get yaml exporter: %v", err)
	}
	if _, err := exporter.Export(rows, options); err != nil {
		t.Fatalf("Export() error: %v", err)
	}
	content, err := os.ReadFile(options.OutputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	return content
}
