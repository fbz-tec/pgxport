# Changelog

All notable changes to pgxport will be documented in this file.

## [v2.1.0] - 2026-10-29

Release focused on data correctness: exported files now always contain exactly the data stored in PostgreSQL, or the export fails with an error. Some outputs change as a result; see **Behavior changes** before upgrading.

### 🛠️ Fixes & Improvements

- **SQL export: JSON and array values are now escaped.**
  Single quotes inside `json`/`jsonb` values and array elements were written unescaped, producing invalid SQL (e.g. `O'Brien`) or allowing a stored value to inject SQL when the export is replayed. Array elements are now quoted, and `NULL` elements are written as `NULL` instead of `<nil>`.
- **NUMERIC values keep every digit in all formats.**
  `numeric` was converted to a 64-bit float, so `12345678901234567.89` was exported as `1.23456789012346e+16`. Values are now written exactly as PostgreSQL stores them, including trailing zeros (`1234.50`).
- **`float8` and `real` values keep their exact value.**
  Floats were printed with 15 significant digits: `0.1 + 0.2` was exported as `0.3` instead of `0.30000000000000004`, and `real` values gained spurious digits (`0.1` became `0.100000001490116`). Floats are now written as PostgreSQL writes them, the same as `--with-copy`. `float8`/`real` `NaN` and `±Infinity` no longer abort JSON exports or produce invalid SQL: they are written as strings in JSON (`"NaN"`) and as `'NaN'::float8` in SQL, and `-0` keeps its sign.
- **Types without dedicated handling are written as PostgreSQL text.**
  `time`, `point`, `xml`, `bit`/`varbit`, `macaddr` and range types were written as internal Go structures (a `time` became `{49530123456 true}`, a `tsrange` a list of Go fields), and SQL exports containing them could not be reloaded. They are now written exactly as PostgreSQL writes them (`13:45:30.123456`, `(1.5,2)`, `[2024-01-01,2024-02-01)`), the same as `--with-copy`, in every format.
- **Arrays keep `NULL` elements and quoting.** In CSV and XML, `NULL` array elements were written as `<nil>` and elements containing commas or quotes were not quoted (`{a,b}` for the single element `"a,b"`). Arrays are now written as PostgreSQL writes them (`{1,NULL,3}`, `{"a,b"}`).
- **`jsonb` arrays stay JSON in CSV and XML.** A JSON array such as `[1, null]` was written as a PostgreSQL array (`{1,<nil>}`).
- **Duplicate column names no longer lose data in JSON, YAML and template exports.**
  With two columns of the same name, common with `SELECT *` over a `JOIN`, the second value silently replaced the first one: `SELECT o.id, o.total, c.id ...` exported the customer id in place of the order id and dropped a column. Repeated names now get a suffix (`id`, `id_2`, `id_3`); CSV and XML keep the original names.
- **The `json` and `jsonPretty` template functions work on rows.** `{{json .}}` returned `{}` for a row (and for `.Rows`); rows are now written as JSON objects with keys in column order.
- **A `DB_DRIVER` environment variable no longer breaks the connection.** Its value was used as the connection URL scheme, so anything other than `postgres` or `postgresql` (e.g. `mysql`, `pgx`, or `Postgres`, often set by other tools) failed with an unclear parse error. pgxport only connects to PostgreSQL and now ignores it.
- **The file inside a zip archive keeps the case of its name.** `-o Report_Q1.csv -z zip` created `Report_Q1.zip` containing `report_q1.csv`; it now contains `Report_Q1.csv`.
- **The completion message shows the file actually written.** With compression, `Export completed: ... -> users.csv` (and the "0 rows" warning) named a file that did not exist: the real file was `users.csv.gz`, `.zst`, `.lz4` or `users.zip`.
- **Error messages containing `%` are no longer garbled.** A parameter error was printed with its own text as a format string, so `--xml-row-tag "row%d"` was reported as `row%!d(MISSING)`.
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
- **YAML export streams rows instead of keeping the whole result in memory.**
  The YAML document was built in memory and written only after the last row: a 1 million row export (4 columns, 106 MB file) used 12 GB of RAM and could be killed by the system. Rows are now written as they are read, using about 20 MB like the JSON export. The output file is unchanged; an empty result is still written as `[]`.

### 🧹 Internal

- CSV and XML exports share a single value formatter (`FormatTextValue`) instead of two near-identical copies that had to be fixed twice. The unused `formatValue` function and its tests are removed. No change in output.
- The CSV "slow row streaming" warning and the `avg fetch` figure of the verbose log are removed. The fetch time was measured after the row had arrived, so it was always 0 and the warning never fired; its `--with-copy` advice would also not help when the query itself is slow.
- The `core/encoders` package is removed: JSON and YAML exports write each row directly in column order instead of building an ordered map per row. Output is unchanged for distinct column names.
- The single-implementation interfaces `db.Store` and `logger.Logger` are removed, with the unused `GetLogger`, `SetOutput` and `IsQuiet`. Without the interface, `go vet` now checks the format strings passed to the logger.
- Dependencies updated: `pgx` 5.11.0 (fixes GO-2026-5004, SQL injection via dollar-quoted strings), `excelize` 2.11.0 (fixes GO-2026-5960), `x/text` 0.43.0 (fixes GO-2026-5970 and GO-2026-6629), `klauspost/compress` 1.20.3, `lz4` 4.1.33, `cobra` 1.10.2, `pin` 0.10.0, `orderedmap` 3.1.1, `x/term` 0.47.0. Known vulnerabilities reachable from pgxport go from 8 to 2; the remaining two are in excelize, only affect reading workbooks (which pgxport never does), and are fixed upstream after 2.11.0. With excelize 2.11, CSV, JSON and YAML exports are about 6%, 7% and 15% slower (COPY mode unchanged); the cause is being investigated.
- Built with Go 1.27.3 (was 1.25.5). It includes fixes for two `crypto/x509` and `crypto/tls` vulnerabilities (GO-2026-6809, GO-2026-6607).
- `TestReadSQLFromFilePermissions` is skipped, with a reason, where file permissions are not enforced (Windows, or tests run as root). It failed there, so the full test suite never passed on Windows.

### ⚠️ Behavior changes

- When an export fails, the partial output file is now **removed**. Devices and symlinks (e.g. `-o /dev/stdout`) are never removed.
- `numeric` columns look different in exported files: exact values, trailing zeros kept, no scientific notation.
- `numeric` cells in XLSX remain Excel numbers (Excel stores numbers as 64-bit floats).
- In XML, JSON values are now escaped like any other text (`&#34;` instead of `"`). XML readers return the original JSON.
- `bytea` columns are exported as `\x` + hex in every format. Text stored in a `bytea` column now appears as `\x48656c6c6f` instead of `Hello`.
- In SQL exports, `timestamptz` values are always written in UTC (`+00:00`), whatever the time zone of the machine running pgxport. The stored instant is unchanged.
- A query that writes data now fails at execution with a PostgreSQL error (`cannot execute ... in a read-only transaction`) instead of being exported. Non-`SELECT`/`WITH` statements are reported as `unsupported SQL command` instead of `forbidden SQL command detected`.
- The undocumented `DB_DRIVER` setting is ignored.
- Array elements containing spaces are now double-quoted, as PostgreSQL writes them (`{"has space"}` instead of `{has space}`), following pgx 5.11. Both forms read back as the same array.
- In zip exports, the file inside the archive keeps the upper-case letters of the output name (`Report_Q1.csv` instead of `report_q1.csv`).
- In JSON, YAML and template exports, a repeated column name now appears as `name_2`, `name_3`… (template `.Columns` uses the same names).
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