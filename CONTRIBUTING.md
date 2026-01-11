# Contributing to HttpdLog Extension

This guide covers development setup, building, testing, and releasing the extension.

## Prerequisites

### Managing Dependencies

DuckDB extensions use VCPKG for dependency management:

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

## Building

```sh
make
```

The main binaries that will be built are:

| Binary | Description |
|--------|-------------|
| `./build/release/duckdb` | DuckDB shell with the extension built-in |
| `./build/release/test/unittest` | Test runner with extension linked |
| `./build/release/extension/httpd_log/httpd_log.duckdb_extension` | Loadable extension binary |

## Local Development

### Using the Built Extension

#### Option 1: Use the DuckDB Binary with Extension Built-in

```sh
./build/release/duckdb
```

```sql
SELECT client_ip, method, path, status
FROM read_httpd_log('test/data/common/sample.log')
LIMIT 5;
```

#### Option 2: Load the Extension in Standard DuckDB

```sh
duckdb -unsigned
```

```sql
LOAD 'build/release/extension/httpd_log/httpd_log.duckdb_extension';
SELECT * FROM read_httpd_log('access.log');
```

## Running Tests

SQL tests are located in `./test/sql`. Run them with:

```sh
make test
```

## Cross-Platform Building

### Using DUCKDB_PLATFORM

```sh
# Build for different platforms
DUCKDB_PLATFORM=osx_arm64 make
DUCKDB_PLATFORM=osx_amd64 make
DUCKDB_PLATFORM=linux_amd64 make
```

**Note:** True cross-compilation requires proper toolchains or Docker containers. The `DUCKDB_PLATFORM` variable mainly affects build configuration.

### CI/CD Builds

GitHub Actions (`.github/workflows/MainDistributionPipeline.yml`) automatically builds for:

- Linux (x86_64, ARM64)
- macOS (x86_64, ARM64)
- Windows (x86_64)
- WebAssembly (WASM)

### Building with Docker

```sh
docker run --rm -v $(pwd):/workspace -w /workspace \
  ubuntu:22.04 bash -c "apt-get update && apt-get install -y build-essential cmake git && make"
```

## Releasing

### Creating a Release

1. Tag the release:
   ```bash
   git tag -a v1.0.0 -m "Release version 1.0.0"
   git push origin v1.0.0
   ```

2. GitHub Actions will automatically:
   - Build binaries for all platforms
   - Run all tests
   - Create a GitHub Release
   - Upload all binaries

### Versioning

Follow [Semantic Versioning](https://semver.org/):
- `v1.0.0` - Major release (breaking changes)
- `v1.1.0` - Minor release (new features, backward compatible)
- `v1.0.1` - Patch release (bug fixes)

## IDE Setup (CLion)

### Opening the Project

1. Ensure the DuckDB submodule is available
2. Open `./duckdb/CMakeLists.txt` as a project in CLion
3. Go to `Tools -> CMake -> Change Project Root` and set it to the repo root

### Debugging

1. In `CLion -> Settings -> Build, Execution, Deploy -> CMake`:
   - Set build path to `../build/{build type}`
   - Add CMake option: `-DDUCKDB_EXTENSION_CONFIGS=<path_to_extension_CMakeLists.txt>`

2. Configure unittest runner:
   - Go to `Run -> Edit Configurations`
   - Click `+ -> CMake Application`
   - Set target and executable to `unittest`
   - Add `--test-dir ../../.. [sql]` to Program Arguments for extension-specific tests

## Architecture

The extension consists of:

1. **httpd_log_table_function** - Main table function implementation
2. **httpd_log_parser** - Log line parsing for common/combined formats
3. **httpd_log_format_parser** - Apache LogFormat string parser and dynamic schema generator
4. **httpd_conf_reader** - Apache httpd.conf parser for LogFormat definitions

Dynamic schema generation allows support for arbitrary log formats by parsing Apache LogFormat directives and automatically creating appropriate DuckDB column definitions.
