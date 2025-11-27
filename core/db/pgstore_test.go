package db

import (
	"context"
	"os"
	"testing"
)

// TestNewStore verifies that NewStore returns a non-nil Store instance
func TestNewStore(t *testing.T) {
	store := NewPgStore("")
	if store == nil {
		t.Error("NewStore() returned nil, expected non-nil Store instance")
	}
}

// TestStoreInterface verifies that dbStore implements Store interface
func TestStoreInterface(t *testing.T) {
	var _ Store = &PgStore{}
}

// TestOpenInvalidURL tests connection with invalid database URLs
func TestOpenInvalidURL(t *testing.T) {
	tests := []struct {
		name  string
		dbURL string
	}{
		{
			name:  "completely invalid URL",
			dbURL: "not-a-valid-url",
		},
		{
			name:  "missing host",
			dbURL: "postgres://user:pass@:5432/db",
		},
		{
			name:  "invalid port",
			dbURL: "postgres://user:pass@localhost:invalid/db",
		},
		{
			name:  "empty URL",
			dbURL: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewPgStore(tt.dbURL)
			err := store.Connect()
			if err == nil {
				t.Error("Open() with invalid URL should return error, got nil")
				store.Close()
			}
		})
	}
}

// TestOpenClose tests the basic Open/Close flow
// Note: This test requires a running PostgreSQL instance
// It will be skipped if DB_TEST_URL is not set
func TestOpenClose(t *testing.T) {
	// Skip if no test database URL is provided
	testURL := getTestDatabaseURL()
	if testURL == "" {
		t.Skip("Skipping integration test: DB_TEST_URL not set")
	}

	store := NewPgStore(testURL)

	// Test Open
	err := store.Connect()
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	// Test Close
	err = store.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}
}

// TestCloseWithoutOpen tests closing a store that was never opened
func TestCloseWithoutOpen(t *testing.T) {
	store := NewPgStore("")
	err := store.Close()
	if err != nil {
		t.Errorf("Close() without Open() should not error, got: %v", err)
	}
}

// TestExecuteQueryWithoutConnection tests query execution without connection
func TestExecuteQueryWithoutConnection(t *testing.T) {
	store := NewPgStore("")

	// Should return error, not panic
	result, err := store.Query(context.Background(), "SELECT 1")

	if err == nil {
		t.Error("ExecuteQuery() without connection should return error")
	}

	if result != nil {
		t.Error("ExecuteQuery() without connection should return nil result")
	}
}

// Integration tests that require a real database
// These will be skipped if DB_TEST_URL is not set

func TestExecuteQueryIntegration(t *testing.T) {
	testURL := getTestDatabaseURL()
	if testURL == "" {
		t.Skip("Skipping integration test: DB_TEST_URL not set")
	}

	store := NewPgStore(testURL)
	if err := store.Connect(); err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer store.Close()

	tests := []struct {
		name         string
		query        string
		wantErr      bool
		expectedCols []string
		expectedRows int
	}{
		{
			name:         "simple SELECT 1",
			query:        "SELECT 1 as num",
			wantErr:      false,
			expectedCols: []string{"num"},
			expectedRows: 1,
		},
		{
			name:         "SELECT with multiple columns",
			query:        "SELECT 1 as id, 'test' as name",
			wantErr:      false,
			expectedCols: []string{"id", "name"},
			expectedRows: 1,
		},
		{
			name:         "SELECT version",
			query:        "SELECT version()",
			wantErr:      false,
			expectedCols: []string{"version"},
			expectedRows: 1,
		},
		{
			name:    "invalid SQL syntax",
			query:   "SELECT * FROM",
			wantErr: true,
		},
		{
			name:    "non-existent table",
			query:   "SELECT * FROM this_table_does_not_exist_12345",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := store.Query(context.Background(), tt.query)

			if tt.wantErr {
				if err == nil {
					t.Error("ExecuteQuery() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("ExecuteQuery() unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("ExecuteQuery() returned nil result")
			}

			// Check columns
			fieldDescs := result.FieldDescriptions()
			if len(fieldDescs) != len(tt.expectedCols) {
				t.Errorf("Column count = %d, want %d", len(fieldDescs), len(tt.expectedCols))
			}

			for i, col := range tt.expectedCols {
				if i < len(fieldDescs) && string(fieldDescs[i].Name) != col {
					t.Errorf("Column[%d] = %q, want %q", i, string(fieldDescs[i].Name), col)
				}
			}

			// Check row count
			rowCount := 0
			for result.Next() {
				rowCount++
			}
			if rowCount != tt.expectedRows {
				t.Errorf("Row count = %d, want %d", rowCount, tt.expectedRows)
			}
		})
	}
}

func TestExecuteQueryEmptyResult(t *testing.T) {
	testURL := getTestDatabaseURL()
	if testURL == "" {
		t.Skip("Skipping integration test: DB_TEST_URL not set")
	}

	store := NewPgStore(testURL)
	if err := store.Connect(); err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer store.Close()

	// Query that returns no rows
	result, err := store.Query(context.Background(), "SELECT 1 as num WHERE 1=0")
	if err != nil {
		t.Fatalf("ExecuteQuery() unexpected error: %v", err)
	}

	if result == nil {
		t.Fatal("ExecuteQuery() returned nil result")
	}

	// Check columns
	fieldDescs := result.FieldDescriptions()
	if len(fieldDescs) != 1 {
		t.Errorf("Expected 1 column, got %d", len(fieldDescs))
	}

	// Check rows
	rowCount := 0
	for result.Next() {
		rowCount++
	}
	if rowCount != 0 {
		t.Errorf("Expected 0 rows, got %d", rowCount)
	}
}

func TestExecuteQueryDataTypes(t *testing.T) {
	testURL := getTestDatabaseURL()
	if testURL == "" {
		t.Skip("Skipping integration test: DB_TEST_URL not set")
	}

	store := NewPgStore(testURL)
	if err := store.Connect(); err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer store.Close()

	query := `
		SELECT 
			1::integer as int_col,
			'test'::text as text_col,
			true::boolean as bool_col,
			3.14::numeric as numeric_col,
			NULL as null_col,
			NOW() as timestamp_col
	`

	result, err := store.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("ExecuteQuery() error: %v", err)
	}

	expectedCols := []string{"int_col", "text_col", "bool_col", "numeric_col", "null_col", "timestamp_col"}
	fieldDescs := result.FieldDescriptions()
	if len(fieldDescs) != len(expectedCols) {
		t.Errorf("Column count = %d, want %d", len(fieldDescs), len(expectedCols))
	}

	// Verify we got the row with correct number of values
	rowCount := 0
	var rowValues []interface{}
	for result.Next() {
		values, err := result.Values()
		if err != nil {
			t.Fatalf("Failed to get values: %v", err)
		}
		rowValues = values
		rowCount++
	}

	if rowCount != 1 {
		t.Fatalf("Expected 1 row, got %d", rowCount)
	}

	if len(rowValues) != len(expectedCols) {
		t.Errorf("Row value count = %d, want %d", len(rowValues), len(expectedCols))
	}

	// Check that null value is actually nil
	if rowValues[4] != nil {
		t.Errorf("null_col should be nil, got %v", rowValues[4])
	}
}

func TestMultipleQueries(t *testing.T) {
	testURL := getTestDatabaseURL()
	if testURL == "" {
		t.Skip("Skipping integration test: DB_TEST_URL not set")
	}

	store := NewPgStore(testURL)
	if err := store.Connect(); err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer store.Close()

	// Execute multiple queries in sequence
	queries := []string{
		"SELECT 1 as num",
		"SELECT 'hello' as greeting",
		"SELECT true as flag",
	}

	for i, query := range queries {
		t.Run(query, func(t *testing.T) {
			result, err := store.Query(context.Background(), query)
			if err != nil {
				t.Errorf("Query %d failed: %v", i, err)
				return
			}

			if result == nil {
				t.Errorf("Query %d returned nil result", i)
				return
			}

			rowCount := 0
			for result.Next() {
				rowCount++
			}
			if rowCount != 1 {
				t.Errorf("Query %d: expected 1 row, got %d", i, rowCount)
			}
		})
	}
}

func TestConnectionReuse(t *testing.T) {
	testURL := getTestDatabaseURL()
	if testURL == "" {
		t.Skip("Skipping integration test: DB_TEST_URL not set")
	}

	store := NewPgStore(testURL)
	if err := store.Connect(); err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer store.Close()

	// Execute same query multiple times to verify connection reuse
	for i := 0; i < 5; i++ {
		result, err := store.Query(context.Background(), "SELECT 1")
		if err != nil {
			t.Errorf("Query %d failed: %v", i+1, err)
		}
		if result == nil {
			t.Errorf("Query %d returned nil", i+1)
		}
	}
}

// Helper function to get test database URL from environment
// Set DB_TEST_URL environment variable to run integration tests
// Example: export DB_TEST_URL="postgres://user:pass@localhost:5432/testdb"
func getTestDatabaseURL() string {
	// Check for test-specific database URL
	return os.Getenv("DB_TEST_URL")
}
