# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HttpdLog is a DuckDB extension for reading and parsing Apache HTTP server log files directly in SQL queries. It provides table functions `read_httpd_log()` and `read_httpd_conf()` that support Common Log Format, Combined Log Format, and custom Apache LogFormat syntax.

## Build Commands

```sh
# Build extension (requires VCPKG_TOOLCHAIN_PATH to be set)
make

# Run all tests
make test
```

Output binaries:
- `./build/release/duckdb` - DuckDB shell with extension built-in
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/httpd_log/httpd_log.duckdb_extension` - Loadable extension

## Testing

SQL tests are in `./test/sql/`. Test files use DuckDB's sqllogictest format with `.test` extension.

To test with built-in DuckDB:
```sh
./build/release/duckdb
```

To load extension in standard DuckDB:
```sh
duckdb -unsigned
```
```sql
LOAD 'build/release/extension/httpd_log/httpd_log.duckdb_extension';
```

## Architecture

The extension registers two table functions in `httpd_log_extension.cpp`:

### Core Components (`src/`)

- **httpd_log_table_function.cpp** - Main `read_httpd_log()` table function. Handles file globbing, format binding, and row iteration via DuckDB's table function API (Bind → Init → Function pattern).

- **httpd_log_format_parser.cpp** - Apache LogFormat string parser. Converts format directives (e.g., `%h`, `%t`, `%{Referer}i`) into:
  - Schema definitions (column names and types)
  - Regex patterns for log line parsing
  - Key structures: `ParsedFormat`, `FormatField`, `DirectiveDefinition`

- **httpd_conf_reader.cpp** - `read_httpd_conf()` table function. Parses httpd.conf files to extract LogFormat and CustomLog directives.

- **httpd_log_buffered_reader.cpp** - Efficient buffered file reading with gzip support.

- **httpd_log_multi_file_info.cpp** - Multi-file handling (glob patterns, `log_file` column).

### Dynamic Schema Generation

The format parser dynamically generates DuckDB column definitions from Apache LogFormat strings. Directive definitions in `httpd_log_format_parser.cpp` map format codes to column names, types, and collision resolution rules. This allows arbitrary log formats without code changes.

### Key Data Structures

- `ParsedFormat` - Holds parsed format fields, compiled regex, and reusable match buffers
- `FormatField` - Single field definition with directive, column name, type, and modifiers
- `DirectiveDefinition` - Maps directive to column name, type, and collision handling
