# HttpdLog Extension for DuckDB

This DuckDB extension provides functionality to read and parse Apache HTTP server log files directly in SQL queries. It supports both Common Log Format (CLF) and Combined Log Format, with extensibility for custom formats via Apache LogFormat strings.

This repository is based on https://github.com/duckdb/extension-template.

## Features

- **Read Apache log files** using the `read_httpd_log()` table function
- **Support for standard formats**:
  - Common Log Format (CLF): `%h %l %u %t "%r" %>s %b`
  - Combined Log Format: `%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-agent}i"`
- **Custom format support** via `format_str` parameter with Apache LogFormat syntax
- **Dynamic schema generation** - columns are automatically inferred from format strings
- **Error handling** - malformed log lines are captured with `parse_error` flag
- **Glob pattern support** - read multiple log files with wildcards

## Usage

### Basic Usage

```sql
-- Read log file with default common format
SELECT * FROM read_httpd_log('access.log');

-- Read with explicit format type
SELECT * FROM read_httpd_log('access.log', format_type='combined');

-- Read multiple files with glob pattern
SELECT * FROM read_httpd_log('logs/*.log', format_type='common');
```

### Using Custom Format Strings

```sql
-- Use Apache LogFormat string directly
SELECT * FROM read_httpd_log('access.log',
    format_str='%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-agent}i"');

-- Custom format with additional fields
SELECT * FROM read_httpd_log('access.log',
    format_str='%h %l %u %t "%r" %>s %b %D');
```

### Example Queries

```sql
-- Count requests by status code
SELECT status, COUNT(*) as count
FROM read_httpd_log('access.log', format_type='common')
WHERE parse_error = false
GROUP BY status
ORDER BY count DESC;

-- Analyze user agents (combined format)
SELECT user_agent, COUNT(*) as requests
FROM read_httpd_log('access.log', format_type='combined')
WHERE parse_error = false
GROUP BY user_agent
ORDER BY requests DESC
LIMIT 10;

-- Find errors with their details
SELECT timestamp, client_ip, method, path, status
FROM read_httpd_log('logs/*.log', format_type='common')
WHERE status >= 400 AND parse_error = false
ORDER BY timestamp DESC;
```

## Parameters

### `read_httpd_log(path, [format_type], [format_str])`

- **`path`** (required): File path or glob pattern (e.g., `'access.log'`, `'logs/*.log'`)
- **`format_type`** (optional): Predefined format type
  - `'common'` (default) - Apache Common Log Format
  - `'combined'` - Apache Combined Log Format
- **`format_str`** (optional): Custom Apache LogFormat string
  - Takes precedence over `format_type` if both are specified
  - Enables support for custom log formats

## Output Schema

### Common Format (13 columns)
- `client_ip` (VARCHAR) - Client IP address
- `ident` (VARCHAR) - Remote logname
- `auth_user` (VARCHAR) - Authenticated user
- `timestamp` (TIMESTAMP) - Request timestamp (UTC)
- `timestamp_raw` (VARCHAR) - Raw timestamp string
- `method` (VARCHAR) - HTTP method (GET, POST, etc.)
- `path` (VARCHAR) - Request path
- `protocol` (VARCHAR) - HTTP protocol version
- `status` (INTEGER) - HTTP status code
- `bytes` (BIGINT) - Response size in bytes
- `filename` (VARCHAR) - Source log file path
- `parse_error` (BOOLEAN) - Whether parsing failed
- `raw_line` (VARCHAR) - Raw log line (only if parse_error=true)

### Combined Format (15 columns)
All common format columns, plus:
- `referer` (VARCHAR) - HTTP Referer header
- `user_agent` (VARCHAR) - User-Agent string

### Custom Formats
Schema is dynamically generated based on the `format_str` directives.

## Supported LogFormat Directives

| Directive | Description | Column Name | Type |
|-----------|-------------|-------------|------|
| `%h` | Client IP address | `client_ip` | VARCHAR |
| `%l` | Remote logname (identd) | `ident` | VARCHAR |
| `%u` | Remote user (auth) | `auth_user` | VARCHAR |
| `%t` | Timestamp | `timestamp`, `timestamp_raw` | TIMESTAMP, VARCHAR |
| `%r` | Request line | `method`, `path`, `protocol` | VARCHAR |
| `%>s`, `%s` | Status code | `status` | INTEGER |
| `%b`, `%B` | Response bytes | `bytes` | BIGINT |
| `%m` | Request method | `method` | VARCHAR |
| `%U` | URL path | `path` | VARCHAR |
| `%H` | Request protocol | `protocol` | VARCHAR |
| `%{Header}i` | Request header | lowercase header name | VARCHAR |
| `%{Header}o` | Response header | lowercase header name | VARCHAR |
| `%v`, `%V` | Server name | `server_name` | VARCHAR |
| `%p` | Server port | `server_port` | INTEGER |
| `%D` | Request duration (μs) | `time_us` | BIGINT |
| `%T` | Request duration (s) | `time_sec` | BIGINT |
| `%P` | Process ID | `process_id` | INTEGER |

## Building

### Managing dependencies
DuckDB extensions use VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps
Now to build the extension, run:
```sh
make
```

The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/httpd_log/httpd_log.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. The extension is already linked into the binary.
- `httpd_log.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now you can use the `read_httpd_log()` table function:
```sql
D SELECT client_ip, method, path, status
  FROM read_httpd_log('test_data/sample.log')
  WHERE parse_error = false
  LIMIT 5;
┌─────────────┬─────────┬─────────────┬────────┐
│  client_ip  │ method  │    path     │ status │
│   varchar   │ varchar │   varchar   │ int32  │
├─────────────┼─────────┼─────────────┼────────┤
│ 192.168.1.1 │ GET     │ /index.html │    200 │
│ 192.168.1.2 │ POST    │ /api/login  │    200 │
│ ...         │ ...     │ ...         │    ... │
└─────────────┴─────────┴─────────────┴────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

## Installation

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL httpd_log;
LOAD httpd_log;
```

## Development

### Setting up CLion

#### Opening project
Configuring CLion with this extension requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

#### Debugging
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_extension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`.

## Architecture

The extension consists of several key components:

1. **httpd_log_table_function** - Main table function implementation
2. **httpd_log_parser** - Log line parsing for common/combined formats
3. **httpd_log_format_parser** - Apache LogFormat string parser and dynamic schema generator

The dynamic schema generation allows the extension to support arbitrary log formats by parsing Apache LogFormat directives and automatically creating the appropriate DuckDB column definitions.
