# Output Schema

This document provides detailed information about the output schema for the `read_httpd_log` table function.

## Overview

The output schema varies based on two parameters:
- **`format_type`**: Determines the log format (`'common'`, `'combined'`, or custom format string)
- **`raw`**: Controls visibility of diagnostic columns (default: `false`)

### Schema Variations

- **Common format** with `raw=false` (default): **10 columns**
- **Common format** with `raw=true`: **13 columns** (adds 3 diagnostic columns)
- **Combined format** with `raw=false`: **12 columns**
- **Combined format** with `raw=true`: **15 columns** (adds 3 diagnostic columns)

## Complete Column Reference

This table shows all possible columns and their visibility across different configurations:

| Column Name | Type | Description | Common<br>(raw=false) | Common<br>(raw=true) | Combined<br>(raw=false) | Combined<br>(raw=true) |
|-------------|------|-------------|-----------------------|----------------------|-------------------------|------------------------|
| `client_ip` | VARCHAR | Client IP address | ✓ | ✓ | ✓ | ✓ |
| `ident` | VARCHAR | Remote logname (from identd) | ✓ | ✓ | ✓ | ✓ |
| `auth_user` | VARCHAR | Authenticated username | ✓ | ✓ | ✓ | ✓ |
| `timestamp` | TIMESTAMP | Parsed request timestamp (UTC) | ✓ | ✓ | ✓ | ✓ |
| `timestamp_raw` | VARCHAR | Original timestamp string from log | ✗ | ✓ | ✗ | ✓ |
| `method` | VARCHAR | HTTP method (GET, POST, etc.) | ✓ | ✓ | ✓ | ✓ |
| `path` | VARCHAR | Request URL path | ✓ | ✓ | ✓ | ✓ |
| `protocol` | VARCHAR | HTTP protocol version (e.g., HTTP/1.1) | ✓ | ✓ | ✓ | ✓ |
| `status` | INTEGER | HTTP status code (200, 404, etc.) | ✓ | ✓ | ✓ | ✓ |
| `bytes` | BIGINT | Response size in bytes | ✓ | ✓ | ✓ | ✓ |
| `referer` | VARCHAR | HTTP Referer header value | - | - | ✓ | ✓ |
| `user_agent` | VARCHAR | User-Agent string | - | - | ✓ | ✓ |
| `filename` | VARCHAR | Source log file path | ✓ | ✓ | ✓ | ✓ |
| `parse_error` | BOOLEAN | Whether the log line failed to parse | ✗ | ✓ | ✗ | ✓ |
| `raw_line` | VARCHAR | Original log line text (only for errors) | ✗ | ✓ | ✗ | ✓ |

**Legend:**
- ✓ = Column is included in the schema
- ✗ = Column is not included (attempting to query it will cause an error)
- \- = Not applicable for this format type

## Format-Specific Schemas

### Common Format

Apache's standard "Common Log Format" corresponds to the LogFormat:
```
LogFormat "%h %l %u %t \"%r\" %>s %b" common
```

**Columns (raw=false):** 10 columns
- Standard request fields: `client_ip`, `ident`, `auth_user`, `timestamp`
- Request details: `method`, `path`, `protocol`
- Response info: `status`, `bytes`
- Metadata: `filename`

**Columns (raw=true):** 13 columns
- All 10 standard columns above
- Diagnostic columns: `timestamp_raw`, `parse_error`, `raw_line`

**Example:**
```sql
-- Default: 10 columns, errors excluded
SELECT client_ip, method, status, bytes
FROM read_httpd_log('access.log', format_type='common');

-- With diagnostics: 13 columns, errors included
SELECT client_ip, method, status, parse_error, raw_line
FROM read_httpd_log('access.log', format_type='common', raw=true)
WHERE parse_error = true;
```

### Combined Format

Apache's "Combined Log Format" extends Common format with referrer and user-agent:
```
LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
```

**Columns (raw=false):** 12 columns
- All 10 Common format columns
- Additional fields: `referer`, `user_agent`

**Columns (raw=true):** 15 columns
- All 12 standard columns above
- Diagnostic columns: `timestamp_raw`, `parse_error`, `raw_line`

**Example:**
```sql
-- Default: 12 columns, errors excluded
SELECT client_ip, method, referer, user_agent
FROM read_httpd_log('access.log', format_type='combined');

-- With diagnostics: 15 columns, errors included
SELECT client_ip, referer, user_agent, parse_error
FROM read_httpd_log('access.log', format_type='combined', raw=true);
```

### Custom Formats

When using the `format_str` parameter, the schema is dynamically generated based on the LogFormat directives. The number and types of columns depend on the directives used.

**Example:**
```sql
-- Custom format with specific fields
SELECT * FROM read_httpd_log(
    'access.log',
    format_str='%h %t \"%r\" %>s %b %D'
);
-- Returns: client_ip, timestamp, method, path, protocol, status, bytes, time_us
```

See the [Supported LogFormat Directives](#supported-logformat-directives) section below for all available directives.

## Diagnostic Columns (raw mode only)

These three columns are only available when `raw=true` is specified. They are useful for debugging, error analysis, and preserving original log data.

### timestamp_raw

- **Type:** VARCHAR
- **Description:** The original timestamp string exactly as it appears in the log file
- **Example:** `"10/Oct/2000:13:55:36 -0700"`
- **Use cases:**
  - Debugging timestamp parsing issues
  - Preserving the original timezone information
  - Comparing parsed vs. original timestamps
  - Analyzing logs with non-standard timestamp formats

**Example:**
```sql
SELECT timestamp, timestamp_raw
FROM read_httpd_log('access.log', raw=true)
WHERE timestamp IS NULL  -- Find timestamp parsing failures
LIMIT 10;
```

### parse_error

- **Type:** BOOLEAN
- **Description:** Indicates whether the log line failed to parse according to the format
- **Values:**
  - `false` (0): Successfully parsed
  - `true` (1): Parse failure
- **Behavior:**
  - When `raw=false`: Rows with parse errors are automatically excluded from results
  - When `raw=true`: All rows are included, and this column indicates parse status

**Example:**
```sql
-- Find and analyze parse errors
SELECT
    filename,
    COUNT(*) FILTER (WHERE parse_error = false) as valid_lines,
    COUNT(*) FILTER (WHERE parse_error = true) as error_lines,
    ROUND(100.0 * COUNT(*) FILTER (WHERE parse_error = true) / COUNT(*), 2) as error_rate
FROM read_httpd_log('logs/*.log', raw=true)
GROUP BY filename
ORDER BY error_rate DESC;
```

### raw_line

- **Type:** VARCHAR
- **Description:** The complete original log line text
- **Population:** Only populated when `parse_error=true`; `NULL` for successfully parsed lines
- **Purpose:** Allows inspection and debugging of malformed log entries
- **Use cases:**
  - Identifying patterns in malformed logs
  - Manual inspection of parse failures
  - Exporting problematic lines for further analysis

**Example:**
```sql
-- Export all parse errors for manual review
SELECT filename, raw_line
FROM read_httpd_log('access.log', raw=true)
WHERE parse_error = true
ORDER BY filename;

-- Find most common parse error patterns
SELECT
    SUBSTRING(raw_line, 1, 50) as line_prefix,
    COUNT(*) as occurrences
FROM read_httpd_log('logs/*.log', raw=true)
WHERE parse_error = true
GROUP BY line_prefix
ORDER BY occurrences DESC
LIMIT 10;
```

## Usage Examples

### Example 1: Basic Query (Default Mode)

```sql
-- Common format, raw=false (default): 10 columns, errors excluded
SELECT
    client_ip,
    timestamp,
    method,
    path,
    status
FROM read_httpd_log('access.log')
WHERE status >= 400
ORDER BY timestamp DESC
LIMIT 100;
```

### Example 2: Error Analysis (Raw Mode)

```sql
-- Include diagnostic columns to analyze parse errors
SELECT
    parse_error,
    COUNT(*) as count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM read_httpd_log('access.log', raw=true)
GROUP BY parse_error;
```

### Example 3: Combined Format with All Fields

```sql
-- Combined format, raw=false: 12 columns
SELECT
    client_ip,
    method,
    path,
    status,
    referer,
    user_agent,
    bytes
FROM read_httpd_log('access.log', format_type='combined')
WHERE status = 200
  AND user_agent LIKE '%Mobile%'
ORDER BY bytes DESC;
```

### Example 4: Debugging Parse Failures

```sql
-- Find lines that failed to parse and examine them
SELECT
    filename,
    timestamp_raw,
    raw_line,
    LENGTH(raw_line) as line_length
FROM read_httpd_log('logs/*.log', raw=true)
WHERE parse_error = true
ORDER BY filename, line_length DESC;
```

### Example 5: Schema Inspection

```sql
-- View the schema for a specific configuration
DESCRIBE SELECT * FROM read_httpd_log('access.log', format_type='common', raw=false);
-- Returns: 10 rows (columns)

DESCRIBE SELECT * FROM read_httpd_log('access.log', format_type='common', raw=true);
-- Returns: 13 rows (columns)
```

## Supported LogFormat Directives

When using custom formats with the `format_str` parameter, the following Apache LogFormat directives are supported.

**Table columns:**
- **Column Name**: DuckDB column name in the result set
- **Type**: DuckDB data type (VARCHAR, INTEGER, BIGINT, TIMESTAMP, BOOLEAN)
- **Directive**: Apache LogFormat directive (empty if column is metadata/diagnostic only)
- **Included in raw=false/true**: Whether column appears in default vs raw mode
- **Notes**: Additional information about the column

### Standard Directives

| Column Name | Type | Directive | raw=false | raw=true | Notes |
|-------------|------|-----------|-----------|----------|-------|
| `client_ip` | VARCHAR | `%h` | ✓ | ✓ | Client IP address / remote hostname |
| `ident` | VARCHAR | `%l` | ✓ | ✓ | Remote logname from identd (usually "-") |
| `auth_user` | VARCHAR | `%u` | ✓ | ✓ | Authenticated username from HTTP auth |
| `timestamp` | TIMESTAMP | `%t` | ✓ | ✓ | Parsed request timestamp (converted to UTC) |
| `timestamp_raw` | VARCHAR | `%t` | ✗ | ✓ | Original timestamp string from log (diagnostic) |
| `method` | VARCHAR | `%r` or `%m` | ✓ | ✓ | HTTP method (GET, POST, etc.) |
| `path` | VARCHAR | `%r` or `%U` | ✓ | ✓ | Request URL path (without query string) |
| `protocol` | VARCHAR | `%r` or `%H` | ✓ | ✓ | HTTP protocol version (HTTP/1.0, HTTP/1.1, etc.) |
| `status` | INTEGER | `%>s` or `%s` | ✓ | ✓ | HTTP status code (200, 404, 500, etc.) |
| `bytes` or `bytes_clf` | BIGINT | `%b` or `%B` | ✓ | ✓ | Response size in bytes (see Bytes Directives below) |
| `server_name` | VARCHAR | `%v` or `%V` | ✓ | ✓ | Server name (canonical or requested hostname) |
| `server_port` | INTEGER | `%p` | ✓ | ✓ | Canonical server port from Listen directive |
| `time_us` | BIGINT | `%D` | ✓ | ✓ | Request duration in microseconds |
| `time_sec` | BIGINT | `%T` | ✓ | ✓ | Request duration in seconds (rounded) |
| `process_id` | INTEGER | `%P` | ✓ | ✓ | Server process ID |

### Bytes Directives (%b vs %B)

The `%b` and `%B` directives have different semantics and are handled specially:

| Column Name | Type | Directive | Behavior | Notes |
|-------------|------|-----------|----------|-------|
| `bytes` | BIGINT | `%b` (alone) | "-" → 0 | CLF format: dash converted to 0 |
| `bytes` | BIGINT | `%B` (alone) | Always numeric | "0" for zero bytes (never "-") |
| `bytes_clf` | BIGINT | `%b` (with `%B`) | "-" → 0 | CLF format when both present |
| `bytes` | BIGINT | `%B` (with `%b`) | Always numeric | Standard format when both present |

**When both `%b` and `%B` are present**, they create two separate columns (`bytes_clf` and `bytes`) to preserve the semantic difference.

**NULL handling**: "-" is converted to 0 (not NULL) to make aggregations and filtering more reliable.

### Generic Header Directives

Generic header directives allow logging arbitrary HTTP request/response headers:

| Column Name | Type | Directive | raw=false | raw=true | Notes |
|-------------|------|-----------|-----------|----------|-------|
| `{header_name}` | VARCHAR | `%{HeaderName}i` | ✓ | ✓ | Request header (generic) |
| `{header_name}` | VARCHAR | `%{HeaderName}o` | ✓ | ✓ | Response header (generic) |

**Column naming**: Header names are converted to lowercase with hyphens replaced by underscores.
- Example: `%{User-Agent}i` → `user_agent`
- Example: `%{X-Forwarded-For}i` → `x_forwarded_for`

**Note**: Some headers have special typing (see Typed HTTP Headers below).

### Typed HTTP Headers

Specific HTTP headers are automatically typed as INTEGER or BIGINT instead of VARCHAR for better performance and type safety:

| Column Name | Type | Directive | raw=false | raw=true | Notes |
|-------------|------|-----------|-----------|----------|-------|
| `content_length` | BIGINT | `%{Content-Length}i` | ✓ | ✓ | Request body size in bytes |
| `content_length` | BIGINT | `%{Content-Length}o` | ✓ | ✓ | Response body size in bytes |
| `age` | INTEGER | `%{Age}o` | ✓ | ✓ | Cache age in seconds (response only) |
| `age` | VARCHAR | `%{Age}i` | ✓ | ✓ | Request header (not typed, remains VARCHAR) |
| `max_forwards` | INTEGER | `%{Max-Forwards}i` | ✓ | ✓ | Proxy hop limit (request only) |
| `max_forwards` | VARCHAR | `%{Max-Forwards}o` | ✓ | ✓ | Response header (not typed, remains VARCHAR) |

**Typing rules:**
- `Content-Length`: BIGINT for both `%i` (request) and `%o` (response)
- `Age`: INTEGER only for `%o` (response); VARCHAR for `%i` (request)
- `Max-Forwards`: INTEGER only for `%i` (request); VARCHAR for `%o` (response)
- All other headers: VARCHAR (no special typing)

**Features:**
- **Case-insensitive matching**: `Content-Length`, `content-length`, and `CONTENT-LENGTH` all work
- **NULL handling**: "-" values → 0 (not NULL) for reliable aggregations
- **Invalid values**: Non-numeric values → NULL (no parse errors)
- **Performance**: Enables numeric operations like filtering, aggregations, and sorting without NULL checks

**Examples:**

```sql
-- Numeric filtering on Content-Length
SELECT * FROM read_httpd_log(
    'access.log',
    format_str='%h %t "%r" %>s %{Content-Length}o'
)
WHERE content_length > 1000000;  -- Find large responses

-- Aggregations on typed headers
SELECT
    AVG(content_length) as avg_size,
    MAX(age) as max_cache_age
FROM read_httpd_log(
    'access.log',
    format_str='%h %t "%r" %>s %{Content-Length}o %{Age}o'
);

-- Filtering by cache age
SELECT * FROM read_httpd_log(
    'access.log',
    format_str='%h %t "%r" %>s %{Age}o'
)
WHERE age >= 3600;  -- Cached for at least 1 hour

-- Max-Forwards for proxy debugging
SELECT * FROM read_httpd_log(
    'access.log',
    format_str='%h %t "%r" %>s %{Max-Forwards}i'
)
WHERE max_forwards < 10;  -- Low proxy hop limit
```

**Note**: All other headers (e.g., `User-Agent`, `Referer`) remain VARCHAR type.

### Custom Format Example

```sql
-- Define a custom format with specific fields
SELECT * FROM read_httpd_log(
    'custom.log',
    format_str='%h %t \"%r\" %>s %b %D \"%{User-Agent}i\"'
);

-- Resulting schema:
-- client_ip, timestamp, [timestamp_raw if raw=true], method, path, protocol,
-- status, bytes, time_us, user_agent, filename, [diagnostic columns if raw=true]
```

## Metadata and Diagnostic Columns

These columns are automatically added and not derived from log directives:

| Column Name | Type | Directive | raw=false | raw=true | Notes |
|-------------|------|-----------|-----------|----------|-------|
| `filename` | VARCHAR | (none) | ✓ | ✓ | Source log file path (always included) |
| `parse_error` | BOOLEAN | (none) | ✗ | ✓ | Whether log line failed to parse (diagnostic) |
| `raw_line` | VARCHAR | (none) | ✗ | ✓ | Original log line text (only for parse errors) |

**Details:**

- **`filename`**: Always included in all configurations. Essential for queries across multiple files using glob patterns.
- **`parse_error`**: Only in raw mode. `false` (0) = successfully parsed, `true` (1) = parse failure. When `raw=false`, rows with parse errors are automatically excluded.
- **`raw_line`**: Only in raw mode. Contains the complete original log line text, but only populated when `parse_error=true`; `NULL` for successfully parsed lines.

**Example:**
```sql
-- Analyze logs by source file
SELECT
    filename,
    COUNT(*) as requests,
    AVG(bytes) as avg_response_size
FROM read_httpd_log('logs/*.log')
GROUP BY filename
ORDER BY requests DESC;

-- Find and inspect parse errors
SELECT filename, raw_line
FROM read_httpd_log('logs/*.log', raw=true)
WHERE parse_error = true;
```

## Data Types Reference

### VARCHAR
- Variable-length string
- Used for: IP addresses, usernames, paths, headers, etc.
- Can contain NULL values (represented as "-" in many log formats)

### TIMESTAMP
- Date and time value
- Timezone: Converted to UTC
- Format: `YYYY-MM-DD HH:MM:SS`
- Can be NULL if parsing fails

### INTEGER
- 32-bit signed integer
- Range: -2,147,483,648 to 2,147,483,647
- Used for: status codes, ports, process IDs

### BIGINT
- 64-bit signed integer
- Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
- Used for: byte counts, microsecond durations

### BOOLEAN
- True/false value
- Values: `true` (1) or `false` (0)
- Used only for: `parse_error` diagnostic column

## Performance Considerations

### Impact of raw=true

- **Storage:** No significant impact; diagnostic columns are only populated for parse errors (except `timestamp_raw`)
- **Performance:** Minimal overhead; always collected internally
- **Result size:** Larger when errors are present, as failed rows are included

### Recommendations

- Use `raw=false` (default) for production queries where you only need valid data
- Use `raw=true` for:
  - Debugging and log quality analysis
  - Identifying and fixing parse errors
  - Preserving all log data including malformed entries
  - Detailed error reporting and monitoring

## See Also

- [Main README](../README.md) - Quick start guide and usage examples
- [Parameters Documentation](../README.md#parameters) - Details on all function parameters
- [Building and Testing](../README.md#building) - Development and testing instructions
