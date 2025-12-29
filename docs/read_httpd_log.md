# read_httpd_log Function

The `read_httpd_log` function reads and parses Apache HTTP server log files.

## Overview

This function parses Apache access log files and returns them as a queryable table. It supports:

- Common Log Format and Combined Log Format
- Custom formats via Apache LogFormat syntax
- Format lookup from httpd.conf files
- Glob patterns for reading multiple files

## Usage

```sql
-- Read with default common format
SELECT * FROM read_httpd_log('access.log');

-- Read with combined format
SELECT * FROM read_httpd_log('access.log', format_type='combined');

-- Read with custom format string
SELECT * FROM read_httpd_log('access.log',
    format_str='%h %l %u %t "%r" %>s %b %D');

-- Read using format from httpd.conf
SELECT * FROM read_httpd_log('access.log', conf='/etc/httpd/conf/httpd.conf');

-- Read multiple files with glob pattern
SELECT * FROM read_httpd_log('logs/*.log');
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | VARCHAR | File path or glob pattern (required) |
| `format_type` | VARCHAR | `'common'` (default), `'combined'`, or nickname from conf |
| `format_str` | VARCHAR | Custom Apache LogFormat string (overrides format_type) |
| `conf` | VARCHAR | Path to httpd.conf for format lookup |
| `raw` | BOOLEAN | Include diagnostic columns (default: false) |

## Output Schema

### Schema Variations

| Format | Columns (raw=false) | Columns (raw=true) |
|--------|---------------------|-------------------|
| Common | 11 | 13 |
| Combined | 13 | 15 |
| Custom | Varies | +2 diagnostic columns |

### Common Format

Apache's standard "Common Log Format":
```
LogFormat "%h %l %u %t \"%r\" %>s %b" common
```

**Columns:** `client_ip`, `ident`, `auth_user`, `timestamp`, `method`, `path`, `query_string`, `protocol`, `status`, `bytes`, `log_file`

### Combined Format

Apache's "Combined Log Format":
```
LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
```

**Columns:** All Common format columns plus `referer`, `user_agent`

### Diagnostic Columns (raw=true only)

| Column | Type | Description |
|--------|------|-------------|
| `parse_error` | BOOLEAN | Whether parsing failed |
| `raw_line` | VARCHAR | Original log line |

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
| `timestamp` | TIMESTAMP | `%t` or `%{format}t` | ✓ | ✓ | Parsed request timestamp (converted to UTC) |
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
| `raw_line` | VARCHAR | (auto) | ✗ | ✓ | Original log line |

**Notes:**
- When `%r` is used, it is parsed into `method`, `path`, `query_string`, and `protocol` columns
- When `%r` is used with individual directives (`%m`, `%U`, `%q`, `%H`), the individual directive takes priority and no duplicate column is created
- Header names are converted to lowercase with hyphens replaced by underscores (e.g., `User-Agent` → `user_agent`)
- Same directive twice produces `column`, `column_2`

## Advanced Topics

### Original/Final Request Modifiers

Apache supports `<` and `>` modifiers for directives that can refer to original or final (redirected) requests:

| Modifier | Meaning | Example |
|----------|---------|---------|
| `%>X` | Final request value | `%>s` (final status) |
| `%<X` | Original request value | `%<s` (original status) |
| `%X` | Default (depends on directive) | `%s` (original status) |

When both are present, `>` (final) gets the base name and others get `_original` suffix:
```sql
format_str='... %s %>s ...'   -- status_original, status
```

### Column Name Collision Resolution

When multiple directives produce the same column name, collisions are resolved by priority:

| Priority | Directive | Suffix |
|----------|-----------|--------|
| 0 | `%>s`, `%>U`, etc. | (none) |
| 1 | `%s`, `%i` | `_original` / `_in` |
| 2 | `%o` | `_out` |
| 3 | `%C` | `_cookie` |
| 4 | `%e` | `_env` |
| 5 | `%n` | `_note` |

Example:
```sql
format_str='%{foo}i %{foo}o'  -- foo, foo_out
```

When the same directive appears multiple times, duplicates are numbered:
```sql
format_str='%{X}i %{X}i %{X}i'  -- x, x_2, x_3
```

### Timestamp Formats

The `%{format}t` directive supports multiple timestamp formats:

| Format | Description | Example Value |
|--------|-------------|---------------|
| `%t` | Standard Apache format | `[10/Oct/2000:13:55:36 -0700]` |
| `%{sec}t` | Seconds since Unix epoch | `1609459200` |
| `%{msec}t` | Milliseconds since Unix epoch | `1609459200123` |
| `%{usec}t` | Microseconds since Unix epoch | `1609459200123456` |
| `%{msec_frac}t` | Millisecond fraction | `123` |
| `%{usec_frac}t` | Microsecond fraction | `123456` |
| `%{strftime}t` | Custom strftime format | `2021-01-01 13:55:36` |

Multiple timestamp directives are automatically combined into a single `timestamp` column. All timestamps are converted to UTC.

## Examples

### Count Requests by Status Code

```sql
SELECT status, COUNT(*) as count
FROM read_httpd_log('access.log')
GROUP BY status
ORDER BY count DESC;
```

### Find Large Responses

```sql
SELECT timestamp, client_ip, path, bytes
FROM read_httpd_log('access.log')
WHERE bytes > 1000000
ORDER BY bytes DESC
LIMIT 10;
```

### Analyze User Agents (Combined Format)

```sql
SELECT user_agent, COUNT(*) as requests
FROM read_httpd_log('access.log', format_type='combined')
GROUP BY user_agent
ORDER BY requests DESC
LIMIT 10;
```

### Debug Parse Errors

```sql
SELECT raw_line
FROM read_httpd_log('access.log', raw=true)
WHERE parse_error = true;
```

## See Also

- [read_httpd_conf](read_httpd_conf.md) - Extract LogFormat definitions from httpd.conf
- [Main README](../README.md) - Quick start guide
