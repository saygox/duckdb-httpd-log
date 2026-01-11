# HttpdLog Extension for DuckDB

A DuckDB extension for reading and parsing Apache HTTP server log files directly in SQL queries.

## Features

- Read Apache log files using the `read_httpd_log()` table function
- Support for Common Log Format and Combined Log Format
- Custom format support via Apache LogFormat syntax
- Read format definitions from httpd.conf with the `read_httpd_conf()` function
- Glob pattern support for reading multiple log files

## Installation

Currently, binaries are available from [GitHub Releases](https://github.com/itosaygo/httpd_log/releases).

```bash
# Download the binary for your platform and extract it
duckdb -unsigned
```

```sql
INSTALL './httpd_log.duckdb_extension';
LOAD httpd_log;
```

> **Note:** Since this extension is not yet signed, you need to launch DuckDB with `allow_unsigned_extensions` enabled (`-unsigned` flag for CLI).

## Usage

### Basic Usage

```sql
SELECT client_host, method, path, status, bytes
FROM read_httpd_log('access.log')
LIMIT 5;
```

```
┌─────────────┬────────┬───────────────┬────────┬───────┐
│  client_host  │ method │     path      │ status │ bytes │
│   varchar   │ varchar│    varchar    │ int32  │ int64 │
├─────────────┼────────┼───────────────┼────────┼───────┤
│ 192.168.1.1 │ GET    │ /index.html   │    200 │  2326 │
│ 192.168.1.2 │ POST   │ /api/login    │    201 │   150 │
│ 192.168.1.3 │ GET    │ /style.css    │    304 │     0 │
│ 192.168.1.4 │ GET    │ /favicon.ico  │    404 │   209 │
│ 192.168.1.5 │ GET    │ /api/users    │    200 │  1024 │
└─────────────┴────────┴───────────────┴────────┴───────┘
```

### Read Multiple Files

```sql
SELECT COUNT(*), log_file
FROM read_httpd_log('logs/*.log')
GROUP BY log_file;
```

```
┌──────────────┬──────────────────────┐
│ count_star() │       log_file       │
│    int64     │       varchar        │
├──────────────┼──────────────────────┤
│         1250 │ logs/access.log      │
│          890 │ logs/access.log.1    │
│          456 │ logs/access.log.2    │
└──────────────┴──────────────────────┘
```

### Custom Format Strings

```sql
-- Use Apache LogFormat string directly
SELECT * FROM read_httpd_log('access.log',
    format_str='%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-agent}i"');

-- Custom format with request duration
SELECT client_host, path, status, duration
FROM read_httpd_log('access.log',
    format_str='%h %l %u %t "%r" %>s %b %D');
```

### Using httpd.conf

```sql
-- Auto-detect format from httpd.conf
SELECT * FROM read_httpd_log('access.log', conf='/etc/httpd/conf/httpd.conf');

-- Use specific format nickname from httpd.conf
SELECT * FROM read_httpd_log('access.log', conf='/etc/httpd/conf/httpd.conf', format_type='combined');
```

### Example Queries

```sql
-- Count requests by status code
SELECT status, COUNT(*) as count
FROM read_httpd_log('access.log')
GROUP BY status
ORDER BY count DESC;
```

```
┌────────┬───────┐
│ status │ count │
│ int32  │ int64 │
├────────┼───────┤
│    200 │  8542 │
│    304 │  1203 │
│    404 │   156 │
│    500 │    23 │
└────────┴───────┘
```

```sql
-- Top 5 requested paths
SELECT path, COUNT(*) as hits
FROM read_httpd_log('access.log')
GROUP BY path
ORDER BY hits DESC
LIMIT 5;
```

```
┌─────────────────────┬───────┐
│        path         │ hits  │
│       varchar       │ int64 │
├─────────────────────┼───────┤
│ /api/health         │  2341 │
│ /index.html         │  1892 │
│ /static/app.js      │  1567 │
│ /static/style.css   │  1234 │
│ /api/users          │   987 │
└─────────────────────┴───────┘
```

```sql
-- Top user agents (combined format)
SELECT user_agent, COUNT(*) as requests
FROM read_httpd_log('access.log', format_type='combined')
GROUP BY user_agent
ORDER BY requests DESC
LIMIT 3;
```

```
┌─────────────────────────────────────────────────┬──────────┐
│                   user_agent                    │ requests │
│                     varchar                     │  int64   │
├─────────────────────────────────────────────────┼──────────┤
│ Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...   │     4521 │
│ Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15).. │     2103 │
│ curl/7.68.0                                     │      892 │
└─────────────────────────────────────────────────┴──────────┘
```

## Parameters

### read_httpd_log

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | VARCHAR | File path or glob pattern (required) |
| `format_type` | VARCHAR | `'common'` (default), `'combined'`, or nickname from conf |
| `format_str` | VARCHAR | Custom Apache LogFormat string (overrides format_type) |
| `conf` | VARCHAR | Path to httpd.conf for format lookup |
| `raw` | BOOLEAN | Include diagnostic columns (default: false) |

### read_httpd_conf

See [read_httpd_conf documentation](docs/read_httpd_conf.md) for details.

## Output Schema

The output schema depends on the log format:

| Format | Columns (raw=false) | Columns (raw=true) |
|--------|---------------------|-------------------|
| Common | 11 | 13 |
| Combined | 13 | 15 |
| Custom | Varies | +2 diagnostic columns |

Common columns include: `client_host`, `ident`, `auth_user`, `timestamp`, `method`, `path`, `query_string`, `protocol`, `status`, `bytes`, `log_file`

Combined format adds: `referer`, `user_agent`

Diagnostic columns (raw=true only): `parse_error`, `raw_line`

See [read_httpd_log documentation](docs/read_httpd_log.md) for complete column reference.

## Building

```sh
make
```

The built extension will be at `./build/release/extension/httpd_log/httpd_log.duckdb_extension`.

For development setup, testing, and contributing, see [CONTRIBUTING.md](CONTRIBUTING.md).
