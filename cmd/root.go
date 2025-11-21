package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fbz-tec/pgxport/core/config"
	"github.com/fbz-tec/pgxport/core/db"
	"github.com/fbz-tec/pgxport/core/exporters"
	"github.com/fbz-tec/pgxport/core/validation"
	"github.com/fbz-tec/pgxport/internal/logger"
	"github.com/fbz-tec/pgxport/internal/version"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

var (
	sqlQuery        string
	sqlFile         string
	outputPath      string
	format          string
	delimiter       string
	connString      string
	tableName       string
	compression     string
	timeFormat      string
	timeZone        string
	xmlRootElement  string
	xmlRowElement   string
	withCopy        bool
	failOnEmpty     bool
	noHeader        bool
	verbose         bool
	quiet           bool
	rowPerStatement int
	// Connection flags
	dbHost     string
	dbPort     int
	dbUser     string
	dbName     string
	dbPassword string
	// template file
	templateFile      string
	templateHeader    string
	templateRow       string
	templateFooter    string
	templateStreaming bool
)

var rootCmd = &cobra.Command{
	Use:   "pgxport",
	Short: "Export PostgreSQL query results to CSV, JSON, XML, YAML or SQL formats",
	Long: `A powerful CLI tool to export PostgreSQL query results.
It supports direct SQL queries or SQL files, with customizable output options.
		
Supported output formats:
 • CSV  — standard text export with customizable delimiter
 • JSON — structured export for API or data processing
 • XML  — hierarchical export for interoperability
 • YAML — human-readable structured export for configs and tools
 • SQL  — generate INSERT statements`,
	Example: `  # Export with inline query
  pgxport -s "SELECT * FROM users" -o users.csv

  # Export from SQL file with custom delimiter
  pgxport -F query.sql -o output.csv -D ";"

  # Use the high-performance COPY mode for large CSV exports
  pgxport -s "SELECT * FROM events" -o events.csv -f csv --with-copy

  # Export to JSON
  pgxport -s "SELECT * FROM products" -o products.json -f json
  
  # Export to XML
  pgxport -s "SELECT * FROM orders" -o orders.xml -f xml

  # Export to YAML
  pgxport -s "SELECT * FROM user" -o orders.yml -f yaml

   # Export to SQL insert statements
  pgxport -s "SELECT * FROM orders" -o orders.sql -f sql -t orders_table`,
	RunE:          runExport,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	rootCmd.Flags().SortFlags = false

	// Connection flags (PostgreSQL-compatible)
	rootCmd.Flags().StringVarP(&dbHost, "host", "H", "", "Database host (overrides .env and environment)")
	rootCmd.Flags().IntVarP(&dbPort, "port", "P", 5432, "Database port (overrides .env and environment)")
	rootCmd.Flags().StringVarP(&dbUser, "user", "u", "", "Database username (overrides .env and environment)")
	rootCmd.Flags().StringVarP(&dbName, "database", "d", "", "Database name (overrides .env and environment)")
	rootCmd.Flags().StringVarP(&dbPassword, "password", "p", "", "Database password (overrides .env and environment)")
	rootCmd.Flags().StringVarP(&connString, "dsn", "", "", "Database connection string (postgres://user:pass@host:port/dbname)")

	//QUERY INPUT - what to export
	rootCmd.Flags().StringVarP(&sqlQuery, "sql", "s", "", "SQL query to execute")
	rootCmd.Flags().StringVarP(&sqlFile, "sqlfile", "F", "", "Path to SQL file containing the query")

	// OUTPUT DESTINATION - where and how to export
	rootCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output file path (required)")
	rootCmd.Flags().StringVarP(&format, "format", "f", "csv", "Output format (csv, json, xml, sql)")
	rootCmd.Flags().StringVarP(&compression, "compression", "z", "none", "Compression to apply to the output file (none, gzip, zip)")

	// CSV options
	rootCmd.Flags().StringVarP(&delimiter, "delimiter", "D", ",", "CSV delimiter character")
	rootCmd.Flags().BoolVar(&withCopy, "with-copy", false, "Use PostgreSQL native COPY for CSV export (faster for large datasets)")
	rootCmd.Flags().BoolVarP(&noHeader, "no-header", "n", false, "Skip header row in CSV output")

	// XML options
	rootCmd.Flags().StringVarP(&xmlRootElement, "xml-root-tag", "", "results", "Sets the root element name for XML exports")
	rootCmd.Flags().StringVarP(&xmlRowElement, "xml-row-tag", "", "row", "Sets the row element name for XML exports")

	// SQL options
	rootCmd.Flags().StringVarP(&tableName, "table", "t", "", "Table name for SQL insert exports")
	rootCmd.Flags().IntVarP(&rowPerStatement, "insert-batch", "", 1, "Number of rows per INSERT statement in SQL export")

	// Template options
	rootCmd.Flags().StringVar(&templateFile, "tpl-file", "", "Path to template file")
	rootCmd.Flags().StringVar(&templateHeader, "tpl-header", "", "Optional header template file (streaming mode)")
	rootCmd.Flags().StringVar(&templateRow, "tpl-row", "", "Row template file (streaming mode)")
	rootCmd.Flags().StringVar(&templateFooter, "tpl-footer", "", "Optional footer template file (streaming mode)")

	// Date FORMATTING
	rootCmd.Flags().StringVarP(&timeFormat, "time-format", "T", "yyyy-MM-dd HH:mm:ss", "Custom time format (e.g. yyyy-MM-ddTHH:mm:ss.SSS)")
	rootCmd.Flags().StringVarP(&timeZone, "time-zone", "Z", "", "Time zone for date/time formatting (e.g. UTC, Europe/Paris). Defaults to local time zone.")

	// BEHAVIOR OPTIONS
	rootCmd.Flags().BoolVarP(&failOnEmpty, "fail-on-empty", "x", false, "Exit with error if query returns 0 rows")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output with detailed information")
	rootCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Enable quiet mode: only display error messages")

	if err := rootCmd.MarkFlagRequired("output"); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	rootCmd.PreRun = func(cmd *cobra.Command, args []string) {
		logger.Debug("Validating export parameters")
		if err := validateExportParams(); err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		logger.Debug("Export parameters validated successfully")
		if quiet {
			logger.SetQuiet(true)
			logger.SetVerbose(false)
		} else {
			logger.SetVerbose(verbose)
			if verbose {
				logger.Debug("Verbose mode enabled")
			}
		}

	}

	rootCmd.AddCommand(versionCmd)

}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runExport(cmd *cobra.Command, args []string) error {

	logger.Debug("Initializing pgxport execution environment")
	logger.Debug("Version: %s, Build: %s, Commit: %s", version.AppVersion, version.BuildTime, version.GitCommit)

	var dbUrl string
	if connString != "" {
		logger.Debug("Using connection string from --dsn flag")
		dbUrl = connString
	} else {
		logger.Debug("Loading configuration from environment and flags")
		cfg := config.LoadConfig()
		if dbHost != "" {
			cfg.DBHost = dbHost
			logger.Debug("Overriding DB host from flag: %s", dbHost)
		}
		if dbPort != 5432 {
			cfg.DBPort = dbPort
			logger.Debug("Overriding DB port from flag: %s", dbPort)
		}
		if dbUser != "" {
			cfg.DBUser = dbUser
			logger.Debug("Overriding DB user from flag: %s", dbUser)
		}
		if dbName != "" {
			cfg.DBName = dbName
			logger.Debug("Overriding DB name from flag: %s", dbName)
		}
		if dbPassword != "" {
			cfg.DBPass = dbPassword
			logger.Debug("Overriding DB password from flag (hidden)")
		}

		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}
		dbUrl = cfg.GetConnectionString()
		logger.Debug("Configuration loaded: host=%s port=%s database=%s user=%s",
			cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBUser)
	}

	var query string
	var err error
	var rowCount int
	var rows pgx.Rows
	var exporter exporters.Exporter

	if sqlFile != "" {
		logger.Debug("Reading SQL from file: %s", sqlFile)
		query, err = readSQLFromFile(sqlFile)
		if err != nil {
			return fmt.Errorf("error reading SQL file: %w", err)
		}
		logger.Debug("SQL query loaded from file (%d characters)", len(query))
	} else {
		query = sqlQuery
		logger.Debug("Using inline SQL query (%d characters)", len(query))
	}

	if err := validation.ValidateQuery(query); err != nil {
		return err
	}

	format = strings.ToLower(strings.TrimSpace(format))

	var delimRune rune = ','
	if format == "csv" {
		delimRune, err = parseDelimiter(delimiter)
		if err != nil {
			return fmt.Errorf("invalid delimiter: %w", err)
		}
		logger.Debug("CSV delimiter: %q", string(delimRune))
	}

	store := db.NewStore()

	if err := store.Open(dbUrl); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	defer store.Close()

	if templateFile != "" {
		templateStreaming = false
	} else {
		templateStreaming = true
	}

	options := exporters.ExportOptions{
		Format:            format,
		Delimiter:         delimRune,
		TableName:         tableName,
		Compression:       compression,
		TimeFormat:        timeFormat,
		TimeZone:          timeZone,
		NoHeader:          noHeader,
		XmlRootElement:    xmlRootElement,
		XmlRowElement:     xmlRowElement,
		RowPerStatement:   rowPerStatement,
		TemplateFile:      templateFile,
		TemplateHeader:    templateHeader,
		TemplateRow:       templateRow,
		TemplateFooter:    templateFooter,
		TemplateStreaming: templateStreaming,
	}

	exporter, err = exporters.GetExporter(format)
	if err != nil {
		return err
	}

	if format == "csv" && withCopy {
		logger.Debug("Using PostgreSQL COPY mode for fast CSV export")

		if copyExp, ok := exporter.(exporters.CopyCapable); ok {
			rowCount, err = copyExp.ExportCopy(store.GetConnection(), query, outputPath, options)
		} else {
			return fmt.Errorf("format %s does not support COPY mode", format)
		}
	} else {
		logger.Debug("Using standard export mode for format: %s", format)
		rows, err = store.ExecuteQuery(context.Background(), query)
		if err != nil {
			return err
		}
		defer rows.Close()

		rowCount, err = exporter.Export(rows, outputPath, options)
	}

	if err != nil {
		return fmt.Errorf("export failed: %w", err)
	}

	return handleExportResult(rowCount, outputPath)
}

func validateExportParams() error {

	if verbose && quiet {
		return fmt.Errorf("error: Cannot use --verbose and --quiet flags together")
	}
	// Validate SQL query source
	if sqlQuery == "" && sqlFile == "" {
		return fmt.Errorf("error: Either --sql or --sqlfile must be provided")
	}

	if sqlQuery != "" && sqlFile != "" {
		return fmt.Errorf("error: Cannot use both --sql and --sqlfile at the same time")
	}

	// Normalize and validate format
	format = strings.ToLower(strings.TrimSpace(format))
	validFormats := exporters.ListExporters()

	isValid := false
	for _, f := range validFormats {
		if format == f {
			isValid = true
			break
		}
	}

	if !isValid {
		return fmt.Errorf("error: Invalid format '%s'. Valid formats are: %s",
			format, strings.Join(validFormats, ", "))
	}

	compression = strings.ToLower(strings.TrimSpace(compression))
	if compression == "" {
		compression = "none"
	}
	validCompressions := []string{"none", "gzip", "zip"}
	compressionValid := false
	for _, c := range validCompressions {
		if compression == c {
			compressionValid = true
			break
		}
	}

	if !compressionValid {
		return fmt.Errorf("error: Invalid compression '%s'. Valid options are: %s",
			compression, strings.Join(validCompressions, ", "))
	}

	// Validate table name for SQL format
	if format == "sql" && strings.TrimSpace(tableName) == "" {
		return fmt.Errorf("error: --table (-t) is required when using SQL format")
	}

	if format == "sql" && rowPerStatement < 1 {
		return fmt.Errorf("error: --insert-batch must be at least 1")
	}

	if format == "template" {
		hasFull := templateFile != ""
		hasStreaming := templateRow != "" || templateHeader != "" || templateFooter != ""
		if hasFull && hasStreaming {
			return fmt.Errorf("template export error: use either --tpl-file (full mode) OR --tpl-row (streaming mode), not both")
		}
		if hasStreaming {
			if templateRow == "" {
				return fmt.Errorf("template streaming mode requires --tpl-row to be specified")
			}
		}
		if !hasFull && !hasStreaming {
			return fmt.Errorf("template format requires either --tpl-file (full mode) OR --tpl-row")
		}
	}

	// Validate time format if provided
	if timeFormat != "" {
		if err := validation.ValidateTimeFormat(timeFormat); err != nil {
			return fmt.Errorf("error: Invalid time format '%s'. Use format like 'yyyy-MM-dd HH:mm:ss'", timeFormat)
		}
	}

	// Validate timezone if provided
	if timeZone != "" {
		if err := validation.ValidateTimeZone(timeZone); err != nil {
			return fmt.Errorf("error: Invalid timezone '%s'. Use format like 'UTC' or 'Europe/Paris'", timeZone)
		}
	}

	return nil
}

func readSQLFromFile(filepath string) (string, error) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return "", fmt.Errorf("unable to read file: %w", err)
	}
	return string(content), nil
}

func parseDelimiter(delim string) (rune, error) {
	delim = strings.TrimSpace(delim)

	if delim == "" {
		return 0, fmt.Errorf("delimiter cannot be empty")
	}

	if delim == `\t` {
		return '\t', nil
	}

	runes := []rune(delim)

	if len(runes) != 1 {
		return 0, fmt.Errorf("delimiter must be a single character (use \\t for tab)")
	}

	return runes[0], nil
}

func handleExportResult(rowCount int, outputPath string) error {
	if rowCount == 0 {

		if failOnEmpty {
			return fmt.Errorf("export failed: query returned 0 rows")
		}

		logger.Warn("Query returned 0 rows. File created at %s but contains no data rows", outputPath)

	} else {
		logger.Success("Export completed: %d rows -> %s", rowCount, outputPath)
	}

	return nil
}
