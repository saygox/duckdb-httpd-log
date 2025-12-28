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

## Supported Directives

All available Apache LogFormat directives and their corresponding DuckDB columns:

| Column Name | Type | Directive | raw=false | raw=true | Description |
|-------------|------|-----------|-----------|----------|-------------|
| `client_ip` | VARCHAR | `%h` | ✓ | ✓ | Client IP address / remote hostname |
| `peer_host` | VARCHAR | `%{c}h` | ✓ | ✓ | Underlying TCP connection hostname (not modified by mod_remoteip) |
| `remote_ip` | VARCHAR | `%a` | ✓ | ✓ | Client IP address (mod_remoteip aware) |
| `peer_ip` | VARCHAR | `%{c}a` | ✓ | ✓ | Underlying peer IP address of the connection |
| `local_ip` | VARCHAR | `%A` | ✓ | ✓ | Server local IP address |
| `ident` | VARCHAR | `%l` | ✓ | ✓ | Remote logname from identd (usually "-") |
| `auth_user` | VARCHAR | `%u` | ✓ | ✓ | Authenticated username from HTTP auth |
| `timestamp` | TIMESTAMP | `%t` | ✓ | ✓ | Parsed request timestamp (converted to UTC) |
| `timestamp_raw` | VARCHAR | `%t` | ✗ | ✓ | Original timestamp string |
| `request` | VARCHAR | `%r` | ✓ | ✓ | Full request line |
| `method` | VARCHAR | `%m` | ✓ | ✓ | HTTP method (GET, POST, etc.) |
| `path` | VARCHAR | `%U` | ✓ | ✓ | Request URL path |
| `protocol` | VARCHAR | `%H` | ✓ | ✓ | HTTP protocol version |
| `status` | INTEGER | `%>s` | ✓ | ✓ | Final HTTP status code |
| `status_original` | INTEGER | `%s` | ✓ | ✓ | Original status code (before internal redirect) |
| `bytes` | BIGINT | `%B` | ✓ | ✓ | Response size in bytes (0 for no content) |
| `bytes_clf` | BIGINT | `%b` | ✓ | ✓ | Response size in CLF format ("-" → 0) |
| `server_name` | VARCHAR | `%v` | ✓ | ✓ | Canonical server name |
| `server_name_used` | VARCHAR | `%V` | ✓ | ✓ | Server name used in request |
| `server_port` | INTEGER | `%p` | ✓ | ✓ | Canonical server port |
| `time_us` | BIGINT | `%D` | ✓ | ✓ | Request duration in microseconds |
| `time_sec` | BIGINT | `%T` | ✓ | ✓ | Request duration in seconds |
| `process_id` | INTEGER | `%P` | ✓ | ✓ | Server process ID |
| `{header_name}` | VARCHAR | `%{Header}i` | ✓ | ✓ | Request header |
| `{header_name}` | VARCHAR | `%{Header}o` | ✓ | ✓ | Response header |
| `filename` | VARCHAR | (auto) | ✓ | ✓ | Source log file path (always included) |
| `parse_error` | BOOLEAN | (auto) | ✗ | ✓ | Whether parsing failed |
| `raw_line` | VARCHAR | (auto) | ✗ | ✓ | Original log line (only for parse errors) |

**Notes:**
- When `%r` is used, it is parsed into `method`, `path`, and `protocol` columns
- When both `%b` and `%B` are present, they become `bytes_clf` and `bytes` respectively
- When both `%s` and `%>s` are present, they become `status_original` and `status` respectively
- When both `%v` and `%V` are present, they become `server_name` and `server_name_used` respectively
- Header names are converted to lowercase with hyphens replaced by underscores (e.g., `User-Agent` → `user_agent`)

## Format-Specific Schemas

### Common Format

Apache's standard "Common Log Format":
```
LogFormat "%h %l %u %t \"%r\" %>s %b" common
```

**Columns (raw=false):** 10 columns
- `client_ip`, `ident`, `auth_user`, `timestamp`, `method`, `path`, `protocol`, `status`, `bytes`, `filename`

**Columns (raw=true):** 13 columns (adds `timestamp_raw`, `parse_error`, `raw_line`)

### Combined Format

Apache's "Combined Log Format":
```
LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
```

**Columns (raw=false):** 12 columns
- All Common format columns plus `referer`, `user_agent`

**Columns (raw=true):** 15 columns (adds `timestamp_raw`, `parse_error`, `raw_line`)

### Custom Formats

When using `format_str`, the schema is dynamically generated:

```sql
SELECT * FROM read_httpd_log(
    'access.log',
    format_str='%h %t \"%r\" %>s %b %D'
);
-- Returns: client_ip, timestamp, method, path, protocol, status, bytes, time_us, filename
```

## Typed HTTP Headers

Specific headers are automatically typed as numeric:

| Header | Type | Directive | Notes |
|--------|------|-----------|-------|
| `content_length` | BIGINT | `%{Content-Length}i/o` | Both request and response |
| `age` | INTEGER | `%{Age}o` | Response only |
| `max_forwards` | INTEGER | `%{Max-Forwards}i` | Request only |

## Column Name Collision Resolution

When directives produce the same column name:

| Collision Type | Resolution |
|----------------|------------|
| `%s` + `%>s` | `status_original`, `status` |
| `%v` + `%V` | `server_name`, `server_name_used` |
| `%b` + `%B` | `bytes_clf`, `bytes` |
| `%{X}i` + `%{X}o` | `x_in`, `x_out` |
| Same directive twice | `column`, `column_2` |

## Diagnostic Columns (raw mode only)

| Column | Description |
|--------|-------------|
| `timestamp_raw` | Original timestamp string (e.g., `10/Oct/2000:13:55:36 -0700`) |
| `parse_error` | `true` if parsing failed, `false` otherwise |
| `raw_line` | Original log line text (only populated for parse errors) |

```sql
-- Find parse errors
SELECT filename, raw_line
FROM read_httpd_log('logs/*.log', raw=true)
WHERE parse_error = true;
```

## Data Types

| Type | Description | Used for |
|------|-------------|----------|
| VARCHAR | Variable-length string | IP addresses, paths, headers |
| TIMESTAMP | Date/time (UTC) | Request timestamps |
| INTEGER | 32-bit integer | Status codes, ports |
| BIGINT | 64-bit integer | Byte counts, durations |
| BOOLEAN | True/false | `parse_error` only |

## See Also

- [Main README](../README.md) - Quick start guide and usage examples
- [Parameters Documentation](../README.md#parameters) - Details on all function parameters
