# Changelog

All notable changes to pgxport will be documented in this file.

## [v2.0.0] - 2025-01-05
Stable release of pgxport v2.0.0. No changes since rc2. Recommended for production use.

## [v2.0.0-rc2] - 2025-12-08

### 🛠️ Fixes & Improvements
- Hardened SQL validation to correctly reject unsafe or multi-statement queries.
- Upgraded Go toolchain to the latest version.

## [v2.0.0-rc1] - 2025-12-02

### Second Major Release Candidate

This release introduces significant improvements, new export formats, better performance, and multiple user-experience enhancements.


### 🚀 New Features

- **Advanced XLSX exporter**  
  High-performance Excel export with minimal memory usage, bold headers, type-aware formatting, and **automatic multi-sheet generation** when a sheet exceeds Excel’s row limit.

- **Template exporter (full + streaming modes)**  
  Flexible Go-template based exporter for generating custom outputs (HTML, Markdown, JSONL, configuration files, etc.).  
  Supports full dataset templates and low-memory streaming mode.


- **Progress indicator (`--progress`)**  
  Lightweight live spinner showing row count and elapsed time for long-running exports.

- **New compression formats**  
  Added support for **ZSTD** and **LZ4** for fast high-ratio compression.

- **Individual connection flags**  
  `--host`, `--port`, `--user`, `--database`, `--password`  
  Now available alongside `.env`, env vars, and `--dsn`.

- **Quiet mode (`--quiet`)**  
  Suppress all non-error output.

- **Improved error messages & validation**  
  More descriptive validation for flags, templates, file paths, and type handling.



### 🛠️ Fixes & Improvements

- Improved memory efficiency across all streaming exporters.
- More robust handling of NULL values in all formats.
- Consistent column ordering for JSON, YAML, XML, and template exporters.
- Better performance logs in verbose mode with clearer throughput metrics.
- Improved CLI help text and documentation readability.
- CSV, JSON, XML, and YAML exporters now share a unified date/time formatting layer.



## [v1.0.0] - 2025-01-20

### First Stable Release 🎉

This is the first stable release of pgxport, a powerful CLI tool for exporting PostgreSQL query results to multiple formats.

#### Features

- **Multi-format export support**: CSV, JSON, XML, SQL, and YAML
- **High-performance CSV export**: PostgreSQL native COPY mode (`--with-copy`) for up to 10× faster exports
- **Flexible compression**: Support for gzip and zip compression
- **Advanced date/time handling**:
  - Customizable formats with `--time-format` flag
  - Timezone conversion support with `--time-zone` flag
  - Proper handling of DATE, TIMESTAMP, and TIMESTAMPTZ types
- **CSV export options**:
  - Customizable delimiter (`--delimiter`)
  - Optional header row (`--no-header`)
  - High-performance COPY mode
- **XML customization**:
  - Custom root element with `--xml-root-tag`
  - Custom row element with `--xml-row-tag`
- **SQL export features**:
  - Schema-qualified table names support
  - Batch INSERT statements (`--insert-batch`) for optimized imports
  - Proper type casting and escaping
- **Configuration flexibility**:
  - `.env` file support
  - Environment variables
  - Direct DSN connection string (`--dsn`)
- **Developer-friendly**:
  - Verbose mode (`--verbose`) with performance diagnostics
  - Fail-on-empty mode (`--fail-on-empty`) for automation
  - Comprehensive error messages
  - Query validation for safety

## [v1.0.0-rc2] - 2025-11-16

### Second Pre-Release

This release candidate focuses on fixing and stabilizing all date/time and type-handling logic across CSV, JSON, XML and SQL exporters.

#### Fixes & Improvements

- Correct handling of `DATE`, `TIMESTAMP`, and `TIMESTAMPTZ` using PostgreSQL OIDs (`pgtype.DateOID`, `pgtype.TimestampOID`, `pgtype.TimestampTzOID`)
- Unified formatting logic for all exporters (CSV, JSON, XML, SQL)
- Fix timezone conversion logic for `TIMESTAMPTZ` with `--time-zone` and `--time-format`
- Improved export handling of:
  - `UUID`
  - `BYTEA`
  - `NUMERIC`
  - `INTERVAL`
  - JSON / JSONB values
  - PostgreSQL array types
- Fixed inconsistent formatting between CSV / JSON / XML exporters
- Added test coverage improvements in `formatting_test.go`

## [v1.0.0-rc1] - 2025-11-10

### First Pre-Release

This is the first pre-release of pgxport.

#### Features

- Export PostgreSQL queries to CSV, JSON, XML, and SQL formats
- High-performance CSV export with PostgreSQL native COPY mode (`--with-copy`)
- Compression support (gzip, zip)
- Flexible configuration (`.env` file, environment variables, or DSN)
- Customizable CSV delimiter and header control
- Custom XML tags with `--xml-root-tag` and `--xml-row-tag` flags
- Verbose mode with performance diagnostics (`--verbose`)
- Fail-on-empty mode for automation (`--fail-on-empty`)
- Custom date/time formats and timezone support
- SQL export with schema-qualified table names
- Batch INSERT statements for SQL exports (`--insert-batch`) for improved import performance

#### Installation

Download the pre-built binary for your platform from the [releases page](https://github.com/fbz-tec/pgxport/releases/tag/v1.0.0-rc2):

- **Linux (x86_64)**: `pgxport-linux-amd64.tar.gz`
- **Linux (ARM64)**: `pgxport-linux-arm64.tar.gz`
- **macOS (Intel)**: `pgxport-darwin-amd64.tar.gz`
- **macOS (Apple Silicon)**: `pgxport-darwin-arm64.tar.gz`
- **Windows (x86_64)**: `pgxport-windows-amd64.zip`
- **Windows (ARM64)**: `pgxport-windows-arm64.zip`

Extract and use immediately - **no installation required!**

**For Go developers:**
```bash
go install github.com/fbz-tec/pgxport@v1.0.0
```


---

For detailed usage, see [README.md](README.md)