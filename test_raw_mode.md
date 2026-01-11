# Raw Mode Parameter Tests

This document contains SQL queries for testing the raw mode parameter functionality.

## Test 1: Basic schema verification

```sql
-- Verify schema without raw mode (11 columns: no parse_error, raw_line)
SELECT COUNT(*) FROM (
    DESCRIBE SELECT * FROM read_httpd_log('test/data/sample.log')
);
-- Expected: 11
```

## Test 2: Schema with raw mode

```sql
-- Verify schema with raw mode (13 columns: includes parse_error, raw_line)
SELECT COUNT(*) FROM (
    DESCRIBE SELECT * FROM read_httpd_log('test/data/sample.log', raw=true)
);
-- Expected: 13
```

## Test 3: Parse error row skipping

```sql
-- Verify raw=false skips parse error rows
SELECT COUNT(*)
FROM read_httpd_log('test/data/with_errors.log');
-- Expected: 3 (only successful parses)
```

## Test 4: Parse error row inclusion

```sql
-- Verify raw=true includes parse error rows
SELECT COUNT(*)
FROM read_httpd_log('test/data/with_errors.log', raw=true);
-- Expected: 5 (3 successful + 2 errors)
```

## Test 5: Parse error column visibility

```sql
-- Test with error lines - raw=true shows parse_error column
SELECT
    parse_error,
    COUNT(*) as count
FROM read_httpd_log('test/data/with_errors.log', raw=true)
GROUP BY parse_error
ORDER BY parse_error;
-- Expected:
-- 0	3
-- 1	2
```

## Test 6: Column access error (parse_error)

```sql
-- Verify parse_error column does not exist with raw=false
SELECT parse_error FROM read_httpd_log('test/data/with_errors.log');
-- Expected: Error - column "parse_error" not found
```

## Test 7: Raw line population

```sql
-- Verify raw_line is populated for parse errors with raw=true
SELECT COUNT(*)
FROM read_httpd_log('test/data/with_errors.log', raw=true)
WHERE parse_error = true AND raw_line IS NOT NULL AND LENGTH(raw_line) > 0;
-- Expected: 2
```

## Test 8: Column access error (raw_line)

```sql
-- Verify raw_line column does not exist with raw=false
SELECT raw_line FROM read_httpd_log('test/data/with_errors.log');
-- Expected: Error - column "raw_line" not found
```

## Test 9: All columns with raw=true

```sql
-- Verify all 13 columns are returned with raw=true
SELECT COUNT(*) FROM (
    SELECT
        client_host,
        ident,
        auth_user,
        timestamp,
        method,
        path,
        query_string,
        protocol,
        status,
        bytes,
        log_file,
        parse_error,
        raw_line
    FROM read_httpd_log('test/data/sample.log', raw=true)
);
-- Expected: 6
```

## Test 10: Row data with raw=false

```sql
-- Verify column names with raw=false (should be 11 columns)
SELECT
    client_host,
    ident,
    auth_user,
    timestamp,
    method,
    path,
    query_string,
    protocol,
    status,
    bytes,
    log_file
FROM read_httpd_log('test/data/sample.log')
LIMIT 1;
-- Expected: Single row with 11 columns
```

## Test 11: Row data with raw=true

```sql
-- Verify column names with raw=true (should be 13 columns)
SELECT
    client_host,
    ident,
    auth_user,
    timestamp,
    method,
    path,
    query_string,
    protocol,
    status,
    bytes,
    log_file,
    parse_error,
    raw_line
FROM read_httpd_log('test/data/sample.log', raw=true)
LIMIT 1;
-- Expected: Single row with 13 columns
-- parse_error should be 0
-- raw_line should contain the original log line
```

## Test 12: Raw line populated for successful parses

```sql
-- Verify raw_line is populated for successful parses with raw=true
SELECT COUNT(*)
FROM read_httpd_log('test/data/sample.log', raw=true)
WHERE parse_error = false AND raw_line IS NOT NULL;
-- Expected: 6 (all successful parses)
```

## Test 13: Compatibility with format_type

```sql
-- Verify both raw=true and format_type work together
SELECT COUNT(*)
FROM read_httpd_log('test/data/sample.log', format_type='common', raw=true);
-- Expected: 6
```

## Test 14: Total row count comparison

```sql
-- Count total records across all test files (raw=false skips errors)
SELECT COUNT(*)
FROM read_httpd_log('test/data/*.log');
-- Expected: 24 (errors skipped)

-- Count total records with raw=true (includes errors)
SELECT COUNT(*)
FROM read_httpd_log('test/data/*.log', raw=true);
-- Expected: 26 (all rows including errors)
```

## Running Tests

To run these tests manually:

```bash
./build/release/duckdb
```

Then copy and paste each SQL query from this document.

Alternatively, run individual tests:

```bash
./build/release/duckdb -c "SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_httpd_log('test/data/sample.log'));"
```
