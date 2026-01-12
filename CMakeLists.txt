cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME httpd_log)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it.

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/httpd_log_extension.cpp
    src/httpd_log_format_parser.cpp
    src/httpd_log_table_function.cpp
    src/httpd_log_buffered_reader.cpp
    src/httpd_log_multi_file_info.cpp
    src/httpd_log_file_reader.cpp
    src/httpd_conf_reader.cpp
)

# For WASM builds, statically link RE2 into the extension
if(WASM_LOADABLE_EXTENSIONS)
    set(RE2_DIR ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/re2)
    set(RE2_SOURCES
        ${RE2_DIR}/re2/bitmap256.cc
        ${RE2_DIR}/re2/compile.cc
        ${RE2_DIR}/re2/bitstate.cc
        ${RE2_DIR}/re2/dfa.cc
        ${RE2_DIR}/re2/filtered_re2.cc
        ${RE2_DIR}/re2/mimics_pcre.cc
        ${RE2_DIR}/re2/nfa.cc
        ${RE2_DIR}/re2/onepass.cc
        ${RE2_DIR}/re2/parse.cc
        ${RE2_DIR}/re2/perl_groups.cc
        ${RE2_DIR}/re2/prefilter.cc
        ${RE2_DIR}/re2/prefilter_tree.cc
        ${RE2_DIR}/re2/prog.cc
        ${RE2_DIR}/re2/re2.cc
        ${RE2_DIR}/re2/regexp.cc
        ${RE2_DIR}/re2/set.cc
        ${RE2_DIR}/re2/simplify.cc
        ${RE2_DIR}/re2/stringpiece.cc
        ${RE2_DIR}/re2/tostring.cc
        ${RE2_DIR}/re2/unicode_casefold.cc
        ${RE2_DIR}/re2/unicode_groups.cc
        ${RE2_DIR}/util/rune.cc
        ${RE2_DIR}/util/strutil.cc
    )
    include_directories(${RE2_DIR})
    list(APPEND EXTENSION_SOURCES ${RE2_SOURCES})
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
