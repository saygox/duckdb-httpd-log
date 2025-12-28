# Output Schema

This document provides detailed information about the output schema for the `read_httpd_log` table function.

## Overview

The output schema varies based on two parameters:
- **`format_type`**: Determines the log format (`'common'`, `'combined'`, or custom format string)
- **`raw`**: Controls visibility of diagnostic columns (default: `false`)

### Schema Variations

- **Common format** with `raw=false` (default): **11 columns**
- **Common format** with `raw=true`: **14 columns** (adds 3 diagnostic columns)
- **Combined format** with `raw=false`: **13 columns**
- **Combined format** with `raw=true`: **16 columns** (adds 3 diagnostic columns)

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
| `method` | VARCHAR | `%m` or `%r` | ✓ | ✓ | HTTP method (GET, POST, etc.) |
| `path` | VARCHAR | `%U` or `%r` | ✓ | ✓ | Request URL path (without query string) |
| `query_string` | VARCHAR | `%q` or `%r` | ✓ | ✓ | Query string (including leading ?) |
| `protocol` | VARCHAR | `%H` or `%r` | ✓ | ✓ | HTTP protocol version |
| `status` | INTEGER | `%s` or `%>s` | ✓ | ✓ | HTTP status code |
| `status_original` | INTEGER | `%s` ( + `%>s` ) | ✓ | ✓ | Original status (when `%s` and `%>s` both present) |
| `bytes` | BIGINT | `%b` or `%B` | ✓ | ✓ | Response size in bytes |
| `bytes_clf` | BIGINT | `%b` ( + `%B` ) | ✓ | ✓ | CLF format bytes (when `%b` and `%B` both present) |
| `bytes_received` | BIGINT | `%I` | ✓ | ✓ | Bytes received including headers (mod_logio) |
| `bytes_sent` | BIGINT | `%O` | ✓ | ✓ | Bytes sent including headers (mod_logio) |
| `bytes_transferred` | BIGINT | `%S` | ✓ | ✓ | Total bytes transferred (mod_logio) |
| `server_name` | VARCHAR | `%v` or `%V` | ✓ | ✓ | Server name |
| `server_name_used` | VARCHAR | `%v` ( + `%V` ) | ✓ | ✓ | Server name used (when `%v` and `%V` both present) |
| `server_port` | INTEGER | `%p` or `%{canonical}p` | ✓ | ✓ | Canonical server port (%p takes priority when both present) |
| `local_port` | INTEGER | `%{local}p` | ✓ | ✓ | Server's actual port |
| `remote_port` | INTEGER | `%{remote}p` | ✓ | ✓ | Client's actual port |
| `duration` | INTERVAL | `%D`, `%T`, or `%{UNIT}T` | ✓ | ✓ | Request duration (highest precision kept when multiple present) |
| `keepalive_count` | INTEGER | `%k` | ✓ | ✓ | Number of keepalive requests on this connection |
| `connection_status` | VARCHAR | `%X` | ✓ | ✓ | Connection status: `aborted`, `keepalive`, or `close` |
| `process_id` | INTEGER | `%P` or `%{pid}P` | ✓ | ✓ | Server process ID (%P takes priority when both present) |
| `thread_id` | BIGINT | `%{tid}P` | ✓ | ✓ | Server thread ID |
| `thread_id_hex` | VARCHAR | `%{hextid}P` | ✓ | ✓ | Server thread ID in hexadecimal format |
| `{header_name}` | VARCHAR | `%{Header}i` or `%{Header}o` | ✓ | ✓ | Request or response header |
| `{header_name}_in` | VARCHAR | `%{Header}i` ( + `%{Header}o` ) | ✓ | ✓ | Request header (when both present) |
| `{header_name}_out` | VARCHAR | `%{Header}o` ( + `%{Header}i` ) | ✓ | ✓ | Response header (when both present) |
| `content_length` | BIGINT | `%{Content-Length}i` or `%{Content-Length}o` | ✓ | ✓ | Request or response Content-Length |
| `content_length_in` | BIGINT | `%{Content-Length}i` ( + `%{Content-Length}o` ) | ✓ | ✓ | Request Content-Length (when both present) |
| `content_length_out` | BIGINT | `%{Content-Length}o` ( + `%{Content-Length}i` ) | ✓ | ✓ | Response Content-Length (when both present) |
| `age` | INTEGER | `%{Age}o` | ✓ | ✓ | Response Age header |
| `max_forwards` | INTEGER | `%{Max-Forwards}i` | ✓ | ✓ | Request Max-Forwards header |
| `{cookie_name}` | VARCHAR | `%{Name}C` | ✓ | ✓ | Cookie value |
| `{var_name}` | VARCHAR | `%{Name}e` | ✓ | ✓ | Environment variable |
| `{note_name}` | VARCHAR | `%{Name}n` | ✓ | ✓ | Note from another module |
| `{trailer_name}` | VARCHAR | `%{Name}^ti` | ✓ | ✓ | Request trailer line |
| `{trailer_name}` | VARCHAR | `%{Name}^to` | ✓ | ✓ | Response trailer line |
| `filename` | VARCHAR | `%f` | ✓ | ✓ | Requested file path |
| `request_log_id` | VARCHAR | `%L` | ✓ | ✓ | Request log ID from error log |
| `handler` | VARCHAR | `%R` | ✓ | ✓ | Response handler name |
| `log_file` | VARCHAR | (auto) | ✓ | ✓ | Source log file path (always included) |
| `parse_error` | BOOLEAN | (auto) | ✗ | ✓ | Whether parsing failed |
| `raw_line` | VARCHAR | (auto) | ✗ | ✓ | Original log line (only for parse errors) |

**Notes:**
- When `%r` is used, it is parsed into `method`, `path`, `query_string`, and `protocol` columns
- When `%r` is used with individual directives (`%m`, `%U`, `%q`, `%H`), the individual directive takes priority and no duplicate column is created
- Header names are converted to lowercase with hyphens replaced by underscores (e.g., `User-Agent` → `user_agent`)
- Same directive twice produces `column`, `column_2`

## Format-Specific Schemas

### Common Format

Apache's standard "Common Log Format":
```
LogFormat "%h %l %u %t \"%r\" %>s %b" common
```

**Columns (raw=false):** 11 columns
- `client_ip`, `ident`, `auth_user`, `timestamp`, `method`, `path`, `query_string`, `protocol`, `status`, `bytes`, `log_file`

**Columns (raw=true):** 14 columns (adds `timestamp_raw`, `parse_error`, `raw_line`)

### Combined Format

Apache's "Combined Log Format":
```
LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
```

**Columns (raw=false):** 13 columns
- All Common format columns plus `referer`, `user_agent`

**Columns (raw=true):** 16 columns (adds `timestamp_raw`, `parse_error`, `raw_line`)

### Custom Formats

When using `format_str`, the schema is dynamically generated:

```sql
SELECT * FROM read_httpd_log(
    'access.log',
    format_str='%h %t \"%r\" %>s %b %D'
);
-- Returns: client_ip, timestamp, method, path, query_string, protocol, status, bytes, duration, log_file
```

## See Also

- [Main README](../README.md) - Quick start guide and usage examples
- [Parameters Documentation](../README.md#parameters) - Details on all function parameters
