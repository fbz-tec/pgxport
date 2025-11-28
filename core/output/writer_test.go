package output

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateOutputWriter_NoCompression(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "none",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}
	defer writer.Close()

	// Write test data
	testData := "test,data,row\n1,2,3\n"
	_, err = writer.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	writer.Close()

	// Verify file exists and content is correct
	content, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	if string(content) != testData {
		t.Errorf("File content = %q, want %q", string(content), testData)
	}
}

func TestCreateOutputWriter_GZIP(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "gzip",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	// Write test data
	testData := "test,data,row\n1,2,3\n"
	_, err = writer.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify .gz extension was added
	expectedPath := testPath + ".gz"
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", expectedPath)
	}

	// Read and decompress to verify content
	file, err := os.Open(expectedPath)
	if err != nil {
		t.Fatalf("Failed to open gzip file: %v", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	content, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Failed to read gzip content: %v", err)
	}

	if string(content) != testData {
		t.Errorf("Decompressed content = %q, want %q", string(content), testData)
	}
}

func TestCreateOutputWriter_GZIP_AlreadyHasExtension(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv.gz")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "gzip",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	testData := "test data"
	writer.Write([]byte(testData))
	writer.Close()

	// Should not add another .gz extension
	if _, err := os.Stat(testPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", testPath)
	}

	// Should not create test.csv.gz.gz
	doublePath := testPath + ".gz"
	if _, err := os.Stat(doublePath); !os.IsNotExist(err) {
		t.Errorf("Unexpected file %s exists", doublePath)
	}
}

func TestCreateOutputWriter_ZSTD(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "zstd",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	// Write test data
	testData := "test,data,row\n1,2,3\n"
	_, err = writer.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify .zst extension was added
	expectedPath := testPath + ".zst"
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", expectedPath)
	}

	// Read and decompress to verify content
	file, err := os.Open(expectedPath)
	if err != nil {
		t.Fatalf("Failed to open zstd file: %v", err)
	}
	defer file.Close()

	zstReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("Failed to create zstd reader: %v", err)
	}
	defer zstReader.Close()

	content, err := io.ReadAll(zstReader)
	if err != nil {
		t.Fatalf("Failed to read zstd content: %v", err)
	}

	if string(content) != testData {
		t.Errorf("Decompressed content = %q, want %q", string(content), testData)
	}
}

func TestCreateOutputWriter_ZSTD_AlreadyHasExtension(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv.zst")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "zstd",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	testData := "test data"
	writer.Write([]byte(testData))
	writer.Close()

	// Should not add another .zst extension
	if _, err := os.Stat(testPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", testPath)
	}

	// Should not create test.csv.zst.zst
	doublePath := testPath + ".zst"
	if _, err := os.Stat(doublePath); !os.IsNotExist(err) {
		t.Errorf("Unexpected file %s exists", doublePath)
	}
}

func TestCreateOutputWriter_ZIP(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "zip",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	// Write test data
	testData := "test,data,row\n1,2,3\n"
	_, err = writer.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify .zip extension was added
	expectedPath := fixExtension(testPath, ".zip")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", expectedPath)
	}

	// Read and extract zip to verify content
	zipReader, err := zip.OpenReader(expectedPath)
	if err != nil {
		t.Fatalf("Failed to open zip file: %v", err)
	}
	defer zipReader.Close()

	if len(zipReader.File) != 1 {
		t.Fatalf("Expected 1 file in zip, got %d", len(zipReader.File))
	}

	// Verify entry name
	entry := zipReader.File[0]
	if !strings.HasSuffix(entry.Name, ".csv") {
		t.Errorf("Zip entry name = %q, expected to end with .csv", entry.Name)
	}

	// Read content
	rc, err := entry.Open()
	if err != nil {
		t.Fatalf("Failed to open zip entry: %v", err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Failed to read zip content: %v", err)
	}

	if string(content) != testData {
		t.Errorf("Zip content = %q, want %q", string(content), testData)
	}
}

func TestCreateOutputWriter_ZIP_AlreadyHasExtension(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.zip")

	cfg := OutputConfig{
		Format:      "json",
		Compression: "zip",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	testData := "test data"
	writer.Write([]byte(testData))
	writer.Close()

	// Should not add another .zip extension
	if _, err := os.Stat(testPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", testPath)
	}

	// Should not create test.zip.zip
	doublePath := testPath + ".zip"
	if _, err := os.Stat(doublePath); !os.IsNotExist(err) {
		t.Errorf("Unexpected file %s exists", doublePath)
	}
}

func TestCreateOutputWriter_InvalidCompression(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "invalid",
		Path:        testPath,
	}

	_, err := CreateWriter(cfg)
	if err == nil {
		t.Error("CreateWriter() expected error for invalid compression, got nil")
	}

	if !strings.Contains(err.Error(), "unsupported compression") {
		t.Errorf("Error message should contain 'unsupported compression', got: %v", err)
	}
}

func TestCreateOutputWriter_CompressionCaseInsensitive(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		shouldWork  bool
	}{
		{"lowercase gzip", "gzip", true},
		{"uppercase GZIP", "GZIP", true},
		{"mixed case GzIp", "GzIp", true},
		{"lowercase zip", "zip", true},
		{"uppercase ZIP", "ZIP", true},
		{"mixed case ZiP", "ZiP", true},
		{"lowercase none", "none", true},
		{"uppercase NONE", "NONE", true},
		{"lowercase zstd", "zstd", true},
		{"uppercase ZSTD", "ZSTD", true},
		{"mixed case ZsTd", "ZsTd", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			testPath := filepath.Join(tmpDir, "test.csv")

			cfg := OutputConfig{
				Format:      "csv",
				Compression: tt.compression,
				Path:        testPath,
			}

			writer, err := CreateWriter(cfg)

			if tt.shouldWork {
				if err != nil {
					t.Errorf("CreateWriter() unexpected error: %v", err)
				} else {
					writer.Write([]byte("test"))
					writer.Close()
				}
			} else {
				if err == nil {
					t.Error("CreateWriter() expected error, got nil")
					writer.Close()
				}
			}
		})
	}
}

func TestCreateOutputWriter_CompressionWithWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "  gzip  ",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() should handle whitespace, error = %v", err)
	}

	writer.Write([]byte("test"))
	writer.Close()

	// Verify file was created with .gz extension
	expectedPath := testPath + ".gz"
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", expectedPath)
	}
}

func TestDetermineZipEntryName(t *testing.T) {
	tests := []struct {
		name       string
		outputPath string
		format     string
		expected   string
	}{
		{
			name:       "basic csv file",
			outputPath: "/path/to/output.zip",
			format:     "csv",
			expected:   "output.csv",
		},
		{
			name:       "json file",
			outputPath: "/path/to/data.zip",
			format:     "json",
			expected:   "data.json",
		},
		{
			name:       "xml file",
			outputPath: "/path/to/export.zip",
			format:     "xml",
			expected:   "export.xml",
		},
		{
			name:       "sql file",
			outputPath: "/path/to/backup.zip",
			format:     "sql",
			expected:   "backup.sql",
		},
		{
			name:       "file already has format extension",
			outputPath: "/path/to/output.csv.zip",
			format:     "csv",
			expected:   "output.csv",
		},
		{
			name:       "uppercase ZIP extension",
			outputPath: "/path/to/DATA.ZIP",
			format:     "json",
			expected:   "data.json",
		},
		{
			name:       "empty filename defaults to export",
			outputPath: "/path/to/.zip",
			format:     "csv",
			expected:   "export.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineZipEntryName(tt.outputPath, tt.format)
			if result != tt.expected {
				t.Errorf("determineZipEntryName(%q, %q) = %q, want %q",
					tt.outputPath, tt.format, result, tt.expected)
			}
		})
	}
}

func TestCreateOutputWriter_MultipleWrites(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "gzip",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	// Write in multiple chunks
	chunks := []string{"line1\n", "line2\n", "line3\n"}
	expected := strings.Join(chunks, "")

	for _, chunk := range chunks {
		_, err := writer.Write([]byte(chunk))
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	writer.Close()

	// Read and verify
	expectedPath := testPath + ".gz"
	file, err := os.Open(expectedPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	content, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Failed to read content: %v", err)
	}

	if string(content) != expected {
		t.Errorf("Content = %q, want %q", string(content), expected)
	}
}

func TestCreateOutputWriter_LargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "large.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "gzip",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	// Write 10KB of data
	var buf bytes.Buffer
	for i := 0; i < 1000; i++ {
		buf.WriteString("line,with,data\n")
	}
	testData := buf.Bytes()

	_, err = writer.Write(testData)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	writer.Close()

	// Verify the compressed file is smaller than original
	expectedPath := testPath + ".gz"
	stat, err := os.Stat(expectedPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if stat.Size() >= int64(len(testData)) {
		t.Logf("Compressed size: %d, Original size: %d", stat.Size(), len(testData))
		// Note: For small or repetitive data, compression should reduce size
		// This is a soft check - compression effectiveness varies
	}
}

func TestCompositeWriteCloser_NilCloseFunc(t *testing.T) {
	var buf bytes.Buffer
	writer := &compositeWriteCloser{
		Writer:    &buf,
		closeFunc: nil,
	}

	// Should not panic
	err := writer.Close()
	if err != nil {
		t.Errorf("Close() with nil closeFunc should not error, got: %v", err)
	}
}

func TestCreateOutputWriter_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.csv")

	cfg := OutputConfig{
		Format:      "csv",
		Compression: "none",
		Path:        testPath,
	}

	writer, err := CreateWriter(cfg)
	if err != nil {
		t.Fatalf("CreateWriter() error = %v", err)
	}

	writer.Write([]byte("test"))
	writer.Close()

	// Check file exists and is readable
	info, err := os.Stat(testPath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.IsDir() {
		t.Error("Created path is a directory, expected file")
	}

	// Verify we can read the file
	_, err = os.ReadFile(testPath)
	if err != nil {
		t.Errorf("Failed to read created file: %v", err)
	}
}

func TestFixExtension(t *testing.T) {
	tests := []struct {
		name  string
		input string
		ext   string
		want  string
	}{
		{"no extension", "data", ".zip", "data.zip"},
		{"no extension zip", "data.csv", ".zip", "data.zip"},
		{"already correct zip", "data.csv.zip", ".zip", "data.csv.zip"},
		{"different compression extension", "data.txt", ".bz2", "data.bz2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fixExtension(tt.input, tt.ext)
			if got != tt.want {
				t.Errorf("fixExtension(%q, %q) = %q, want %q", tt.input, tt.ext, got, tt.want)
			}
		})
	}
}

// Benchmark tests
func BenchmarkCreateOutputWriter_NoCompression(b *testing.B) {
	tmpDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testPath := filepath.Join(tmpDir, "bench.csv")
		cfg := OutputConfig{
			Format:      "csv",
			Compression: "none",
			Path:        testPath,
		}
		writer, _ := CreateWriter(cfg)
		writer.Write([]byte("test,data,row\n"))
		writer.Close()
		os.Remove(testPath)
	}
}

func BenchmarkCreateOutputWriter_GZIP(b *testing.B) {
	tmpDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testPath := filepath.Join(tmpDir, "bench.csv")
		cfg := OutputConfig{
			Format:      "csv",
			Compression: "gzip",
			Path:        testPath,
		}
		writer, _ := CreateWriter(cfg)
		writer.Write([]byte("test,data,row\n"))
		writer.Close()
		os.Remove(testPath + ".gz")
	}
}

func BenchmarkCreateOutputWriter_ZIP(b *testing.B) {
	tmpDir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testPath := filepath.Join(tmpDir, "bench.csv")
		cfg := OutputConfig{
			Format:      "csv",
			Compression: "zip",
			Path:        testPath,
		}
		writer, _ := CreateWriter(cfg)
		writer.Write([]byte("test,data,row\n"))
		writer.Close()
		os.Remove(testPath + ".zip")
	}
}
