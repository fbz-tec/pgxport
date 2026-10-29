# Changelog

All notable changes to pgxport will be documented in this file.

## [v2.0.1] - 2026-10-29

Bug-fix release focused on data correctness: exported files now always contain exactly the data stored in PostgreSQL, or the export fails with an error.

### 🛠️ Fixes & Improvements

- **SQL export: JSON and array values are now escaped.**
  Single quotes inside `json`/`jsonb` values and array elements were written unescaped, producing invalid SQL (e.g. `O'Brien`) or allowing a stored value to inject SQL when the export is replayed. Array elements are now quoted, and `NULL` elements are written as `NULL` instead of `<nil>`.
- **NUMERIC values keep every digit in all formats.**
  `numeric` was converted to a 64-bit float, so `12345678901234567.89` was exported as `1.23456789012346e+16`. Values are now written exactly as PostgreSQL stores them, including trailing zeros (`1234.50`).
- **NUMERIC `NaN` and `Infinity` no longer break exports.**
  They aborted JSON, YAML and template exports and produced invalid SQL. They are now written as strings in JSON/YAML (`"NaN"`) and as `'NaN'::numeric` in SQL.
- **`numeric[]` arrays are exported correctly** instead of as internal Go structures.
- **Write errors at the end of an export are no longer ignored.**
  The last buffered bytes and the compression trailer (gzip, zstd, lz4, zip index) are written when the file is closed. A failure at that point (disk full, network share) was silently ignored: the CLI printed `Export completed` and exited with code 0, leaving an empty, truncated or corrupt file. The export now fails with an error.
- **XML export: JSON, arrays and values starting with `{` or `[` stay inside their element.**
  These values were written outside their element and without escaping, so a single `&` or `<` made the whole document invalid.
- **`bytea` values are exported in hex format in all formats.**
  Binary data was written as raw bytes: SQL exports could not be reloaded (NUL bytes, backslashes) and JSON replaced invalid bytes with `�`. `bytea` and `bytea[]` values are now written as `\x0041ff`, the same as `--with-copy`.
- **SQL export: timestamps keep their microseconds and their exact time.**
  `timestamp` and `timestamptz` were truncated to milliseconds, and `timestamptz` lost the minutes of its UTC offset: exported on a machine in a half-hour time zone (India `+05:30`, Newfoundland, Adelaide…), every value was reloaded 30 minutes off. `timestamptz` values are now written in UTC with a full offset (`2024-01-15 10:30:00.123456+00:00`), and timestamps inside arrays use the same format.
- **Queries now run in a `READ ONLY` transaction enforced by PostgreSQL.**
  The previous keyword-based check could be bypassed by queries that look read-only but write data: `SELECT my_function()` deleting rows, `SELECT setval(...)`, or a `DELETE` inside a CTE. PostgreSQL now refuses any data change, in standard and `--with-copy` modes. Admin functions that do not write data and `dblink` are not covered: use a SELECT-only database user for full protection (see README, Security).
- **Valid read-only queries are no longer rejected.**
  The keyword check refused queries such as `SELECT $$please delete me$$` or a column named `copy`. The query check now only rejects empty queries, multiple statements, and statements that do not start with `SELECT` or `WITH`.
- **`--with-copy` accepts the same queries as the standard mode.**
  COPY mode failed with a syntax error on queries ending with `;` (most `.sql` files) and on queries ending with a `--` comment. The final `;` is now removed before the query is run, in both modes.
- **A comment after the final `;` is no longer rejected.**
  `SELECT ...; -- end of report` was refused as "multiple statements". A real second statement after the comment is still refused.
- **XML export: column names are always valid XML element names.**
  Column names were used as element names as is, so `SELECT 1+1` (column `?column?`), or aliases such as `"my col"`, `"1st"` or `"a&b"`, made the whole document invalid. Characters not allowed in XML names are now replaced with `_`, and names that cannot start an element get a `_` prefix (`?column?` → `_column_`, `1st` → `_1st`). Valid names are unchanged.
- **`--xml-root-tag` and `--xml-row-tag` are validated.** An invalid element name (e.g. `"my row"`) is now rejected with a clear error instead of producing an invalid document.
- **`--port 5432` now overrides `DB_PORT`.**
  The port flag was only applied when it differed from its default value, so `-P 5432` was ignored when `.env` or the environment set another port. The verbose log also printed the port as `%!s(int=5433)`.
- **`PGPASSWORD` and `~/.pgpass` now work when `DB_PASS` is not set.**
  An empty password was always written into the connection URL, which overrode `PGPASSWORD` and `~/.pgpass`, although the README recommends `PGPASSWORD`. `DB_PASS` and `--password` still take precedence when set.
- **`--time-zone` no longer slows exports down.**
  The time zone was loaded from disk again for every `timestamptz` value. Exporting 1 million rows took 12.4 s instead of 1 s with one `timestamptz` column, and 48 s instead of 2.2 s with four. Each time zone is now loaded once.

### ⚠️ Behavior changes

- When an export fails, the partial output file is now **removed**. Devices and symlinks (e.g. `-o /dev/stdout`) are never removed.
- `numeric` columns look different in exported files: exact values, trailing zeros kept, no scientific notation.
- `numeric` cells in XLSX remain Excel numbers (Excel stores numbers as 64-bit floats).
- In XML, JSON values are now escaped like any other text (`&#34;` instead of `"`). XML readers return the original JSON.
- `bytea` columns are exported as `\x` + hex in every format. Text stored in a `bytea` column now appears as `\x48656c6c6f` instead of `Hello`.
- In SQL exports, `timestamptz` values are always written in UTC (`+00:00`), whatever the time zone of the machine running pgxport. The stored instant is unchanged.
- A query that writes data now fails at execution with a PostgreSQL error (`cannot execute ... in a read-only transaction`) instead of being exported. Non-`SELECT`/`WITH` statements are reported as `unsupported SQL command` instead of `forbidden SQL command detected`.
- In XML exports, columns whose names are not valid XML names get a different element name (see above). Two columns can end up with the same element name (`"a b"` and `"a_b"` both become `<a_b>`); the document stays valid and column order is kept.

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