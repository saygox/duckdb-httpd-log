# read_httpd_conf Function

The `read_httpd_conf` function extracts LogFormat definitions from Apache httpd.conf files.

## Overview

This function parses Apache configuration files and returns all log format definitions as a table. It recognizes:

- `LogFormat` directives (with or without nickname)
- `CustomLog` directives with inline format strings
- `ErrorLogFormat` directives

## Usage

```sql
-- Read all format definitions from httpd.conf
SELECT * FROM read_httpd_conf('/etc/httpd/conf/httpd.conf');

-- Filter by log type
SELECT * FROM read_httpd_conf('/etc/httpd/conf/httpd.conf')
WHERE log_type = 'access';

-- Find named formats only
SELECT nickname, format_string
FROM read_httpd_conf('/etc/httpd/conf/httpd.conf')
WHERE format_type = 'named';
```

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `log_type` | VARCHAR | `'access'` or `'error'` |
| `format_type` | VARCHAR | `'named'`, `'default'`, or `'inline'` |
| `nickname` | VARCHAR | Format nickname (NULL if not named) |
| `format_string` | VARCHAR | The LogFormat string |
| `config_file` | VARCHAR | Source configuration file path |
| `line_number` | INTEGER | Line number in the config file |

### format_type Values

| Value | Description | Example Directive |
|-------|-------------|-------------------|
| `named` | LogFormat with a nickname | `LogFormat "%h %l %u %t" common` |
| `default` | LogFormat without nickname | `LogFormat "%h %l %u %t"` |
| `inline` | CustomLog with inline format | `CustomLog logs/access.log "%h %l"` |

## Integration with read_httpd_log

The `read_httpd_conf` function is useful for:

1. **Discovering available formats** in your Apache configuration
2. **Debugging format mismatches** when logs don't parse correctly
3. **Understanding your log structure** before querying

### Example: Find and Use a Format

```sql
-- Step 1: List available formats
SELECT nickname, format_string
FROM read_httpd_conf('/etc/httpd/conf/httpd.conf')
WHERE format_type = 'named';

-- Step 2: Use the discovered format with read_httpd_log
SELECT * FROM read_httpd_log(
    'access.log',
    conf='/etc/httpd/conf/httpd.conf',
    format_type='combined'
);
```

### Example: Check for Duplicate Nicknames

Apache allows the same nickname to be defined multiple times (in different VirtualHost contexts):

```sql
SELECT nickname, COUNT(*) as count
FROM read_httpd_conf('/etc/httpd/conf/httpd.conf')
WHERE nickname IS NOT NULL
GROUP BY nickname
HAVING COUNT(*) > 1;
```

## Notes

- Line continuation with backslash (`\`) is supported
- Comments and empty lines are ignored
- The function only returns format definitions, not CustomLog file paths
