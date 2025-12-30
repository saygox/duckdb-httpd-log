# read_httpd_log Function

The `read_httpd_log` function reads and parses Apache HTTP server log files.

## Overview

This function parses Apache access log files and returns them as a queryable table. It supports:

- Common Log Format and Combined Log Format (auto-detected)
- Glob patterns, S3, gzip, and other sources via DuckDB's filesystem layer
- Automatic format selection from httpd.conf
- Parsed and normalized data (timestamps converted to UTC, request lines decomposed)
- Custom formats via Apache LogFormat syntax
- Dynamic schema generation with automatic column collision resolution

## Usage

```sql
-- Basic usage (format auto-detected)
SELECT * FROM read_httpd_log('access.log');

-- Read from S3 (requires httpfs extension)
LOAD httpfs;
SELECT * FROM read_httpd_log('s3://bucket/logs/*.log.gz');

-- Auto-select format from httpd.conf
SELECT * FROM read_httpd_log('access.log', conf='/etc/httpd/conf/httpd.conf');

-- Read with custom format string
SELECT * FROM read_httpd_log('access.log',
    format_str='%h %l %u %t "%r" %>s %b %D');
```

```sql
-- Timestamps are converted to UTC, request lines decomposed into method/path/etc.
SELECT client_host, timestamp, method, path, status
FROM read_httpd_log('access.log')
LIMIT 3;
```
```
┌─────────────┬─────────────────────┬────────┬──────────────┬────────┐
│  client_host  │      timestamp      │ method │     path     │ status │
│   varchar   │      timestamp      │ varchar│   varchar    │ int32  │
├─────────────┼─────────────────────┼────────┼──────────────┼────────┤
│ 192.168.1.1 │ 2024-01-15 08:23:45 │ GET    │ /index.html  │    200 │
│ 192.168.1.2 │ 2024-01-15 08:23:46 │ POST   │ /api/login   │    201 │
│ 192.168.1.3 │ 2024-01-15 08:23:47 │ GET    │ /style.css   │    304 │
└─────────────┴─────────────────────┴────────┴──────────────┴────────┘
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | VARCHAR | (required) | File path or glob pattern |
| `conf` | VARCHAR | - | Path to httpd.conf for automatic format selection |
| `format_type` | VARCHAR | (auto-detect) | `'common'`, `'combined'`, or nickname from conf |
| `format_str` | VARCHAR | - | Custom Apache LogFormat string (overrides format_type) |
| `raw` | BOOLEAN | false | Include diagnostic columns |

### Specifying Format Explicitly

When auto-detection fails (e.g., misidentifies combined as common), specify the format explicitly.
If your httpd.conf defines a custom format:

```apache
# In httpd.conf
LogFormat "%v %h %l %u %t \"%r\" %>s %b" vhost
```

Use `conf` with `format_type` to select it:

```sql
SELECT server_name, client_host, path, status
FROM read_httpd_log('access.log',
    conf='/etc/httpd/conf/httpd.conf',
    format_type='vhost')
LIMIT 3;
```
```
┌─────────────────┬─────────────┬──────────────┬────────┐
│   server_name   │  client_host  │     path     │ status │
│     varchar     │   varchar   │   varchar    │ int32  │
├─────────────────┼─────────────┼──────────────┼────────┤
│ www.example.com │ 192.168.1.1 │ /index.html  │    200 │
│ api.example.com │ 192.168.1.2 │ /api/users   │    200 │
│ www.example.com │ 192.168.1.3 │ /style.css   │    304 │
└─────────────────┴─────────────┴──────────────┴────────┘
```

### Detecting Parse Errors

With `raw=true`, rows that failed to parse are included with `parse_error=true`.
This is useful when log files contain mixed content (e.g., error logs mixed in, or corrupted lines):

```sql
SELECT client_host, status, parse_error, raw_line
FROM read_httpd_log('access.log', raw=true)
LIMIT 5;
```
```
┌─────────────┬────────┬─────────────┬──────────────────────────────────────────────────────────┐
│  client_host  │ status │ parse_error │                         raw_line                         │
│   varchar   │ int32  │   boolean   │                         varchar                          │
├─────────────┼────────┼─────────────┼──────────────────────────────────────────────────────────┤
│ 192.168.1.1 │    200 │ false       │ 192.168.1.1 - - [15/Jan/2024:08:23:45 +0900] "GET /in... │
│ 192.168.1.2 │    201 │ false       │ 192.168.1.2 - - [15/Jan/2024:08:23:46 +0900] "POST /a... │
│ NULL        │   NULL │ true        │ [Mon Jan 15 08:23:47 2024] [error] [client 192.168.1.... │
│ 192.168.1.3 │    304 │ false       │ 192.168.1.3 - - [15/Jan/2024:08:23:47 +0900] "GET /st... │
│ NULL        │   NULL │ true        │ PHP Fatal error: Uncaught Exception in /var/www/html... │
└─────────────┴────────┴─────────────┴──────────────────────────────────────────────────────────┘
```

## Supported Directives

Directives follow [Apache 2.4 mod_log_config](https://httpd.apache.org/docs/2.4/mod/mod_log_config.html) syntax:

| Column Name | Type | Directive | Group | Description |
|-------------|------|-----------|-------|-------------|
| `client_host` | VARCHAR | `%h` | Client | Client IP address / remote hostname |
| `peer_host` | VARCHAR | `%{c}h` | Client | Underlying TCP connection hostname (not modified by mod_remoteip) |
| `remote_ip` | VARCHAR | `%a` | Client | Client IP address (mod_remoteip aware) |
| `peer_ip` | VARCHAR | `%{c}a` | Client | Underlying peer IP address of the connection |
| `remote_port` | INTEGER | `%{remote}p` | Client | Client's source port |
| `ident` | VARCHAR | `%l` | Auth | Remote logname from identd (usually "-") |
| `auth_user` | VARCHAR | `%u` | Auth | Authenticated username from HTTP auth |
| `local_ip` | VARCHAR | `%A` | Server | Server local IP address |
| `server_name` | VARCHAR | `%v`, `%V` | Server | Server name |
| `server_name_used` | VARCHAR | `%v` ( + `%V` ) | Server | Server name used (when `%v` and `%V` both present) |
| `server_port` | INTEGER | `%p` or `%{canonical}p` | Server | Canonical server port (%p takes priority when both present) |
| `local_port` | INTEGER | `%{local}p` | Server | Server's actual port |
| `timestamp` | TIMESTAMP | `%t` or `%{format}t` | Time | Parsed request timestamp (converted to UTC); `%{end:...}t` gets priority |
| `timestamp_original` | TIMESTAMP | `%{begin:...}t` ( + `%{end:...}t` ) | Time | Original timestamp (when both begin: and end: present) |
| `method` | VARCHAR | `%m` or `%r` | Request | HTTP method (GET, POST, etc.) |
| `path` | VARCHAR | `%>U`, `%>r`, `%U`, or `%r` | Request | Request URL path (without query string) |
| `path_original` | VARCHAR | `%U` ( + `%>U` or `%>r` ), `%r` ( + `%>U` or `%>r` ), `%<U`, `%<r` | Request | Original path (when both present) |
| `query_string` | VARCHAR | `%q` or `%r` | Request | Query string (including leading ?) |
| `protocol` | VARCHAR | `%H` or `%r` | Request | HTTP protocol version |
| `status` | INTEGER | `%>s`, `%s` | Response | HTTP status code |
| `status_original` | INTEGER | `%s` ( + `%>s` ), `%<s` | Response | Original status (when both present) |
| `bytes` | BIGINT | `%B`, `%b` | Response | Response size in bytes (`%b` "-" converted to 0) |
| `bytes_received` | BIGINT | `%I` | Response | Bytes received including headers (mod_logio) |
| `bytes_sent` | BIGINT | `%O` | Response | Bytes sent including headers (mod_logio) |
| `bytes_transferred` | BIGINT | `%S` | Response | Total bytes transferred (mod_logio) |
| `duration` | INTERVAL | `%>D`, `%D`, `%>T`, `%T`, or `%{UNIT}T` | Connection | Request duration (highest precision kept when multiple present) |
| `duration_original` | INTERVAL | `%D` ( + `%>D` ), `%<D`, `%T` ( + `%>T` ), `%<T` | Connection | Original duration (when both present) |
| `keepalive_count` | INTEGER | `%k` | Connection | Number of keepalive requests on this connection |
| `connection_status` | VARCHAR | `%X` | Connection | Connection status: `aborted`, `keepalive`, or `close` |
| `process_id` | INTEGER | `%P` or `%{pid}P` | Process | Server process ID (%P takes priority when both present) |
| `thread_id` | BIGINT | `%{tid}P` | Process | Server thread ID |
| `thread_id_hex` | VARCHAR | `%{hextid}P` | Process | Server thread ID in hexadecimal format |
| `{header_name}` | VARCHAR | `%{Header}i` or `%{Header}o` | Dynamic | Request or response header |
| `content_length` | BIGINT | `%{Content-Length}i` or `%{Content-Length}o` | Dynamic | Request or response Content-Length |
| `age` | INTEGER | `%{Age}o` | Dynamic | Response Age header |
| `max_forwards` | INTEGER | `%{Max-Forwards}i` | Dynamic | Request Max-Forwards header |
| `{cookie_name}` | VARCHAR | `%{Name}C` | Dynamic | Cookie value |
| `{var_name}` | VARCHAR | `%{Name}e` | Dynamic | Environment variable |
| `{note_name}` | VARCHAR | `%{Name}n` | Dynamic | Note from another module |
| `{trailer_name}` | VARCHAR | `%{Name}^ti` | Dynamic | Request trailer line |
| `{trailer_name}` | VARCHAR | `%{Name}^to` | Dynamic | Response trailer line |
| `filename` | VARCHAR | `%f` | Other | Requested file path |
| `request_log_id` | VARCHAR | `%L` | Other | Request log ID from error log |
| `handler` | VARCHAR | `%R` | Other | Response handler name |
| `log_file` | VARCHAR | (auto) | Auto | Source log file path (always included) |
| `line_number` | BIGINT | (auto) | Auto | Line number in file, 1-based (raw=true only) |
| `parse_error` | BOOLEAN | (auto) | Auto | Whether parsing failed (raw=true only) |
| `raw_line` | VARCHAR | (auto) | Auto | Original log line (raw=true only) |

**Notes:**
- Dynamic directive names (`%{Name}i`, `%{Name}o`, `%{Name}C`, `%{Name}e`, `%{Name}n`, `%{Name}^ti`, `%{Name}^to`) are converted to lowercase with hyphens replaced by underscores (e.g., `User-Agent` → `user_agent`)
- When directives produce the same column name, suffixes are added to resolve collisions (see [Column Name Collision Resolution](#column-name-collision-resolution))
- `%r` is parsed into `method`, `path`, `query_string`, and `protocol` columns
- When `%r` is used with individual directives (`%m`, `%U`, `%q`, `%H`), the individual directive takes priority
- Multiple timestamp directives are automatically combined into a single `timestamp` column (converted to UTC)
- `%{begin:...}t` and `%{end:...}t` prefixes are supported; when both present, `end:` gets `timestamp` and `begin:` gets `timestamp_original`

### Column Name Collision Resolution

When multiple directives produce the same column name, collisions are resolved by priority:

| Priority | Directive | Suffix |
|----------|-----------|--------|
| 0 | `%>s`, `%>U`, etc. | (none) |
| 1 | `%s`, `%U`, etc. | `_original` |
| 2 | `%i` | `_in` |
| 3 | `%o` | `_out` |
| 4 | `%C` | `_cookie` |
| 5 | `%e` | `_env` |
| 6 | `%n` | `_note` |

Example:
```sql
format_str='%{foo}i %{foo}o'  -- foo, foo_out
```

When the same directive appears multiple times, duplicates are numbered:
```sql
format_str='%{X}i %{X}i %{X}i'  -- x, x_2, x_3
```

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
SELECT timestamp, client_host, path, bytes
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
