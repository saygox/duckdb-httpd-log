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
| `status` | INTEGER | `%s` or `%>s` | ✓ | ✓ | HTTP status code |
| `status_original` | INTEGER | `%s` ( + `%>s` ) | ✓ | ✓ | Original status (when `%s` and `%>s` both present) |
| `bytes` | BIGINT | `%b` or `%B` | ✓ | ✓ | Response size in bytes |
| `bytes_clf` | BIGINT | `%b` ( + `%B` ) | ✓ | ✓ | CLF format bytes (when `%b` and `%B` both present) |
| `server_name` | VARCHAR | `%v` or `%V` | ✓ | ✓ | Server name |
| `server_name_used` | VARCHAR | `%v` ( + `%V` ) | ✓ | ✓ | Server name used (when `%v` and `%V` both present) |
| `server_port` | INTEGER | `%p` | ✓ | ✓ | Canonical server port |
| `time_us` | BIGINT | `%D` | ✓ | ✓ | Request duration in microseconds |
| `time_sec` | BIGINT | `%T` | ✓ | ✓ | Request duration in seconds |
| `process_id` | INTEGER | `%P` | ✓ | ✓ | Server process ID |
| `{header_name}` | VARCHAR | `%{Header}i` or `%{Header}o` | ✓ | ✓ | Request or response header |
| `{header_name}_in` | VARCHAR | `%{Header}i` ( + `%{Header}o` ) | ✓ | ✓ | Request header (when both present) |
| `{header_name}_out` | VARCHAR | `%{Header}o` ( + `%{Header}i` ) | ✓ | ✓ | Response header (when both present) |
| `content_length` | BIGINT | `%{Content-Length}i` or `%{Content-Length}o` | ✓ | ✓ | Request or response Content-Length |
| `content_length_in` | BIGINT | `%{Content-Length}i` ( + `%{Content-Length}o` ) | ✓ | ✓ | Request Content-Length (when both present) |
| `content_length_out` | BIGINT | `%{Content-Length}o` ( + `%{Content-Length}i` ) | ✓ | ✓ | Response Content-Length (when both present) |
| `age` | INTEGER | `%{Age}o` | ✓ | ✓ | Response Age header |
| `max_forwards` | INTEGER | `%{Max-Forwards}i` | ✓ | ✓ | Request Max-Forwards header |
| `filename` | VARCHAR | (auto) | ✓ | ✓ | Source log file path (always included) |
| `parse_error` | BOOLEAN | (auto) | ✗ | ✓ | Whether parsing failed |
| `raw_line` | VARCHAR | (auto) | ✗ | ✓ | Original log line (only for parse errors) |

**Notes:**
- When `%r` is used, it is parsed into `method`, `path`, and `protocol` columns
- Header names are converted to lowercase with hyphens replaced by underscores (e.g., `User-Agent` → `user_agent`)
- Same directive twice produces `column`, `column_2`

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

## See Also

- [Main README](../README.md) - Quick start guide and usage examples
- [Parameters Documentation](../README.md#parameters) - Details on all function parameters
