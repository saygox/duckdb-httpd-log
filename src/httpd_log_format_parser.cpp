#include "httpd_log_format_parser.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/exception.hpp"
#include <sstream>

namespace duckdb {

// Apache LogFormat directives to column name mapping
const std::unordered_map<string, string> HttpdLogFormatParser::directive_to_column = {
    {"%h", "client_ip"},   // Remote hostname (IP address)
    {"%l", "ident"},       // Remote logname (from identd)
    {"%u", "auth_user"},   // Remote user (from auth)
    {"%t", "timestamp"},   // Time the request was received
    {"%r", "request"},     // First line of request (method path protocol)
    {"%>s", "status"},     // Status code
    {"%s", "status"},      // Status code (alternative)
    {"%b", "bytes"},       // Size of response in bytes (excluding headers)
    {"%B", "bytes"},       // Size of response in bytes (alternative)
    {"%m", "method"},      // Request method
    {"%U", "path"},        // URL path requested
    {"%H", "protocol"},    // Request protocol
    {"%v", "server_name"}, // Canonical server name
    {"%V", "server_name"}, // Server name (alternative)
    {"%p", "server_port"}, // Port the request was served on
    {"%P", "process_id"},  // Process ID of child that serviced request
    {"%D", "time_us"},     // Time taken to serve request in microseconds
    {"%T", "time_sec"},    // Time taken to serve request in seconds
};

// Directive to data type mapping
const std::unordered_map<string, LogicalTypeId> HttpdLogFormatParser::directive_to_type = {
    {"%h", LogicalTypeId::VARCHAR},   {"%l", LogicalTypeId::VARCHAR}, {"%u", LogicalTypeId::VARCHAR},
    {"%t", LogicalTypeId::TIMESTAMP}, {"%r", LogicalTypeId::VARCHAR}, {"%>s", LogicalTypeId::INTEGER},
    {"%s", LogicalTypeId::INTEGER},   {"%b", LogicalTypeId::BIGINT},  {"%B", LogicalTypeId::BIGINT},
    {"%m", LogicalTypeId::VARCHAR},   {"%U", LogicalTypeId::VARCHAR}, {"%H", LogicalTypeId::VARCHAR},
    {"%v", LogicalTypeId::VARCHAR},   {"%V", LogicalTypeId::VARCHAR}, {"%p", LogicalTypeId::INTEGER},
    {"%P", LogicalTypeId::INTEGER},   {"%D", LogicalTypeId::BIGINT},  {"%T", LogicalTypeId::BIGINT},
};

string HttpdLogFormatParser::GetColumnName(const string &directive, const string &modifier) {
	// Handle special case for %{...}i and %{...}o (request/response headers)
	if (directive == "%i" || directive == "%o") {
		if (!modifier.empty()) {
			// Convert header name to lowercase column name
			string col_name = modifier;
			std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);

			// Replace hyphens with underscores
			std::replace(col_name.begin(), col_name.end(), '-', '_');

			return col_name;
		}
	}

	// Look up standard directive
	auto it = directive_to_column.find(directive);
	if (it != directive_to_column.end()) {
		return it->second;
	}

	// Default: use directive as-is (remove %)
	string col_name = directive;
	if (!col_name.empty() && col_name[0] == '%') {
		col_name = col_name.substr(1);
	}
	return "field_" + col_name;
}

LogicalType HttpdLogFormatParser::GetDataType(const string &directive) {
	// Special case for headers - always VARCHAR
	if (directive == "%i" || directive == "%o") {
		return LogicalType::VARCHAR;
	}

	// Look up standard directive type
	auto it = directive_to_type.find(directive);
	if (it != directive_to_type.end()) {
		return LogicalType(it->second);
	}

	// Default to VARCHAR for unknown directives
	return LogicalType::VARCHAR;
}

ParsedFormat HttpdLogFormatParser::ParseFormatString(const string &format_str) {
	ParsedFormat result(format_str);

	size_t pos = 0;
	bool in_quotes = false;

	while (pos < format_str.length()) {
		char c = format_str[pos];

		// Track quote state
		if (c == '"') {
			in_quotes = !in_quotes;
			pos++;
			continue;
		}

		// Look for format directives (%)
		if (c == '%' && pos + 1 < format_str.length()) {
			string directive;
			string modifier;
			size_t start_pos = pos;

			// Check for modifiers like %{...}i or %{...}o
			if (pos + 1 < format_str.length() && format_str[pos + 1] == '{') {
				// Find the closing }
				size_t close_pos = format_str.find('}', pos + 2);
				if (close_pos != string::npos && close_pos + 1 < format_str.length()) {
					modifier = format_str.substr(pos + 2, close_pos - pos - 2);
					char type_char = format_str[close_pos + 1];
					directive = "%" + string(1, type_char);
					pos = close_pos + 2;
				} else {
					// Malformed directive, skip
					pos++;
					continue;
				}
			} else {
				// Standard directive (1-2 characters after %)
				directive = format_str.substr(pos, 2);

				// Check for %>s or other multi-char directives
				if (pos + 2 < format_str.length() && format_str[pos + 1] == '>') {
					directive = format_str.substr(pos, 3);
					pos += 3;
				} else {
					pos += 2;
				}
			}

			// Get column name and type for this directive
			string column_name = GetColumnName(directive, modifier);
			LogicalType type = GetDataType(directive);

			// Add field
			result.fields.emplace_back(directive, column_name, type, in_quotes, modifier);
		} else {
			// Skip non-directive characters
			pos++;
		}
	}

	// Generate regex pattern from the parsed fields
	result.regex_pattern = GenerateRegexPattern(result);

	// Compile the regex pattern once for performance using RE2
	duckdb_re2::RE2::Options options;
	options.set_log_errors(false);
	result.compiled_regex = make_uniq<duckdb_re2::RE2>(result.regex_pattern, options);
	if (!result.compiled_regex->ok()) {
		throw InvalidInputException("Invalid regex pattern: " + result.compiled_regex->error());
	}

	return result;
}

string HttpdLogFormatParser::GenerateRegexPattern(const ParsedFormat &parsed_format) {
	std::ostringstream pattern;
	pattern << "^";

	size_t pos = 0;
	const string &format_str = parsed_format.original_format_str;
	size_t field_idx = 0;

	bool in_quotes = false;

	while (pos < format_str.length()) {
		char c = format_str[pos];

		// Handle quotes
		if (c == '"') {
			in_quotes = !in_quotes;
			pattern << "\"";
			pos++;
			continue;
		}

		// Handle directives
		if (c == '%' && field_idx < parsed_format.fields.size()) {
			const auto &field = parsed_format.fields[field_idx];

			// Skip the directive in the original format string
			if (!field.modifier.empty()) {
				// Skip %{modifier}X pattern
				size_t close_pos = format_str.find('}', pos);
				if (close_pos != string::npos) {
					pos = close_pos + 2; // Skip past }X
				}
			} else {
				pos += field.directive.length();
			}

			// Add regex pattern based on field type
			if (field.is_quoted) {
				pattern << "([^\"]*)"; // Match anything except quotes
			} else if (field.directive == "%t") {
				pattern << "\\[([^\\]]+)\\]"; // Timestamp in brackets
			} else {
				pattern << "(\\S+)"; // Match non-whitespace
			}

			field_idx++;
		} else if (c == ' ' || c == '\t') {
			// Whitespace - match one or more spaces
			pattern << "\\s+";
			pos++;
			// Skip additional whitespace
			while (pos < format_str.length() && (format_str[pos] == ' ' || format_str[pos] == '\t')) {
				pos++;
			}
		} else if (c == '[') {
			pattern << "\\[";
			pos++;
		} else if (c == ']') {
			pattern << "\\]";
			pos++;
		} else {
			// Literal character
			if (c == '.' || c == '*' || c == '+' || c == '?' || c == '^' || c == '$' || c == '(' || c == ')' ||
			    c == '{' || c == '}' || c == '|' || c == '\\') {
				pattern << '\\';
			}
			pattern << c;
			pos++;
		}
	}

	return pattern.str();
}

void HttpdLogFormatParser::GenerateSchema(const ParsedFormat &parsed_format, vector<string> &names,
                                          vector<LogicalType> &return_types) {
	names.clear();
	return_types.clear();

	// Add columns from the parsed format
	for (const auto &field : parsed_format.fields) {
		// Special handling for %t (timestamp) - add both timestamp and timestamp_raw
		if (field.directive == "%t") {
			names.push_back("timestamp");
			return_types.push_back(LogicalType::TIMESTAMP);
			names.push_back("timestamp_raw");
			return_types.push_back(LogicalType::VARCHAR);
		}
		// Special handling for %r (request) - decompose into method, path, protocol
		else if (field.directive == "%r") {
			names.push_back("method");
			return_types.push_back(LogicalType::VARCHAR);
			names.push_back("path");
			return_types.push_back(LogicalType::VARCHAR);
			names.push_back("protocol");
			return_types.push_back(LogicalType::VARCHAR);
		}
		// Regular field
		else {
			names.push_back(field.column_name);
			return_types.push_back(field.type);
		}
	}

	// Add standard metadata columns
	names.push_back("filename");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("parse_error");
	return_types.push_back(LogicalType::BOOLEAN);

	names.push_back("raw_line");
	return_types.push_back(LogicalType::VARCHAR);
}

bool HttpdLogFormatParser::ParseTimestamp(const string &timestamp_str, timestamp_t &result) {
	// Apache log timestamp format: "10/Oct/2000:13:55:36 -0700"
	// Format: DD/MMM/YYYY:HH:MM:SS TZ
	std::istringstream iss(timestamp_str);
	int day, year, hour, minute, second;
	char month_str[4];
	char sep1, sep2, sep3, sep4, sep5;
	std::string tz_str;

	// Parse: DD/MMM/YYYY:HH:MM:SS
	iss >> day >> sep1 >> month_str[0] >> month_str[1] >> month_str[2] >> sep2 >> year >> sep3 >> hour >> sep4 >>
	    minute >> sep5 >> second >> tz_str;

	if (!iss || sep1 != '/' || sep2 != '/' || sep3 != ':' || sep4 != ':' || sep5 != ':') {
		return false;
	}

	month_str[3] = '\0';

	// Convert month string to number
	static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
	int month = 0;
	for (int i = 0; i < 12; i++) {
		if (strcmp(month_str, months[i]) == 0) {
			month = i + 1;
			break;
		}
	}

	if (month == 0) {
		return false;
	}

	// Parse timezone offset
	int tz_sign = (tz_str[0] == '-') ? -1 : 1;
	int tz_hours = std::stoi(tz_str.substr(1, 2));
	int tz_minutes = std::stoi(tz_str.substr(3, 2));
	int64_t tz_offset_seconds = tz_sign * (tz_hours * 3600 + tz_minutes * 60);

	// Create DuckDB date
	date_t date = Date::FromDate(year, month, day);

	// Create DuckDB time
	dtime_t time = Time::FromTime(hour, minute, second, 0);

	// Combine into timestamp and adjust for timezone to UTC
	timestamp_t ts = Timestamp::FromDatetime(date, time);

	// Adjust for timezone (subtract offset to get UTC)
	int64_t epoch_us = Timestamp::GetEpochMicroSeconds(ts);
	epoch_us -= tz_offset_seconds * Interval::MICROS_PER_SEC;
	result = Timestamp::FromEpochMicroSeconds(epoch_us);

	return true;
}

bool HttpdLogFormatParser::ParseRequest(const string &request, string &method, string &path, string &protocol) {
	// Request format: "GET /index.html HTTP/1.0"
	std::istringstream iss(request);

	if (!(iss >> method >> path >> protocol)) {
		return false;
	}

	return true;
}

vector<string> HttpdLogFormatParser::ParseLogLine(const string &line, const ParsedFormat &parsed_format) {
	vector<string> result;

	// Use the pre-compiled RE2 for performance
	int num_groups = parsed_format.compiled_regex->NumberOfCapturingGroups();

	// Create StringPiece arguments for RE2
	duckdb_re2::StringPiece input(line);
	vector<duckdb_re2::RE2::Arg> args(num_groups);
	vector<duckdb_re2::RE2::Arg*> arg_ptrs(num_groups);
	vector<duckdb_re2::StringPiece> matches(num_groups);

	for (int i = 0; i < num_groups; i++) {
		args[i] = &matches[i];
		arg_ptrs[i] = &args[i];
	}

	// Perform the match
	if (!duckdb_re2::RE2::FullMatchN(input, *parsed_format.compiled_regex, arg_ptrs.data(), num_groups)) {
		// Parsing failed - return empty vector
		return result;
	}

	// Extract matched groups
	result.reserve(num_groups);
	for (int i = 0; i < num_groups; i++) {
		result.push_back(matches[i].as_string());
	}

	return result;
}

} // namespace duckdb
