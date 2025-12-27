#pragma once

#include "duckdb.hpp"
#include <regex>
#include <string>
#include <vector>

namespace duckdb {

// Represents a single field in the log format
struct FormatField {
	string directive;   // The format directive (e.g., "%h", "%t", "%{Referer}i")
	string column_name; // The corresponding column name (e.g., "client_ip", "timestamp")
	LogicalType type;   // The data type for this field
	bool is_quoted;     // Whether this field appears in quotes in the log format
	string modifier;    // Optional modifier (e.g., "Referer" in "%{Referer}i")

	FormatField(string directive_p, string column_name_p, LogicalType type_p, bool is_quoted_p = false,
	            string modifier_p = "")
	    : directive(std::move(directive_p)), column_name(std::move(column_name_p)), type(std::move(type_p)),
	      is_quoted(is_quoted_p), modifier(std::move(modifier_p)) {
	}
};

// Parsed format string information
struct ParsedFormat {
	vector<FormatField> fields;     // List of fields in the format
	string original_format_str;     // Original format string
	string regex_pattern;           // Generated regex pattern for parsing
	std::regex compiled_regex;      // Pre-compiled regex for performance

	ParsedFormat() = default;
	explicit ParsedFormat(string format_str) : original_format_str(std::move(format_str)) {
	}
};

class HttpdLogFormatParser {
public:
	// Parse an Apache LogFormat string into structured fields
	static ParsedFormat ParseFormatString(const string &format_str);

	// Get the column name for a given directive
	static string GetColumnName(const string &directive, const string &modifier = "");

	// Get the data type for a given directive
	static LogicalType GetDataType(const string &directive);

	// Generate a regex pattern from the format string
	static string GenerateRegexPattern(const ParsedFormat &parsed_format);

	// Generate DuckDB schema (column names and types) from parsed format
	// Adds standard columns: filename, parse_error, raw_line
	static void GenerateSchema(const ParsedFormat &parsed_format, vector<string> &names,
	                           vector<LogicalType> &return_types);

	// Parse a log line using the parsed format
	// Returns a vector of string values corresponding to the fields in parsed_format
	// Returns empty vector if parsing fails
	static vector<string> ParseLogLine(const string &line, const ParsedFormat &parsed_format);

	// Helper to parse timestamp from Apache log format
	static bool ParseTimestamp(const string &timestamp_str, timestamp_t &result);

	// Helper to parse request line into method, path, protocol
	static bool ParseRequest(const string &request, string &method, string &path, string &protocol);

private:
	// Map of standard Apache LogFormat directives to column names
	static const std::unordered_map<string, string> directive_to_column;

	// Map of directives to their data types
	static const std::unordered_map<string, LogicalTypeId> directive_to_type;
};

} // namespace duckdb
