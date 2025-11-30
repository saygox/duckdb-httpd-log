#include "httpd_log_format_parser.hpp"
#include <regex>
#include <sstream>

namespace duckdb {

// Apache LogFormat directives to column name mapping
const std::unordered_map<string, string> HttpdLogFormatParser::directive_to_column = {
    {"%h", "client_ip"},        // Remote hostname (IP address)
    {"%l", "ident"},            // Remote logname (from identd)
    {"%u", "auth_user"},        // Remote user (from auth)
    {"%t", "timestamp"},        // Time the request was received
    {"%r", "request"},          // First line of request (method path protocol)
    {"%>s", "status"},          // Status code
    {"%s", "status"},           // Status code (alternative)
    {"%b", "bytes"},            // Size of response in bytes (excluding headers)
    {"%B", "bytes"},            // Size of response in bytes (alternative)
    {"%m", "method"},           // Request method
    {"%U", "path"},             // URL path requested
    {"%H", "protocol"},         // Request protocol
    {"%v", "server_name"},      // Canonical server name
    {"%V", "server_name"},      // Server name (alternative)
    {"%p", "server_port"},      // Port the request was served on
    {"%P", "process_id"},       // Process ID of child that serviced request
    {"%D", "time_us"},          // Time taken to serve request in microseconds
    {"%T", "time_sec"},         // Time taken to serve request in seconds
};

// Directive to data type mapping
const std::unordered_map<string, LogicalTypeId> HttpdLogFormatParser::directive_to_type = {
    {"%h", LogicalTypeId::VARCHAR},
    {"%l", LogicalTypeId::VARCHAR},
    {"%u", LogicalTypeId::VARCHAR},
    {"%t", LogicalTypeId::TIMESTAMP},
    {"%r", LogicalTypeId::VARCHAR},
    {"%>s", LogicalTypeId::INTEGER},
    {"%s", LogicalTypeId::INTEGER},
    {"%b", LogicalTypeId::BIGINT},
    {"%B", LogicalTypeId::BIGINT},
    {"%m", LogicalTypeId::VARCHAR},
    {"%U", LogicalTypeId::VARCHAR},
    {"%H", LogicalTypeId::VARCHAR},
    {"%v", LogicalTypeId::VARCHAR},
    {"%V", LogicalTypeId::VARCHAR},
    {"%p", LogicalTypeId::INTEGER},
    {"%P", LogicalTypeId::INTEGER},
    {"%D", LogicalTypeId::BIGINT},
    {"%T", LogicalTypeId::BIGINT},
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
			if (field.directive.find('{') != string::npos) {
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
				pattern << "([^\"]*)";  // Match anything except quotes
			} else if (field.directive == "%t") {
				pattern << "\\[([^\\]]+)\\]";  // Timestamp in brackets
			} else {
				pattern << "(\\S+)";  // Match non-whitespace
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
			if (c == '.' || c == '*' || c == '+' || c == '?' || c == '^' || c == '$' ||
			    c == '(' || c == ')' || c == '{' || c == '}' || c == '|' || c == '\\') {
				pattern << '\\';
			}
			pattern << c;
			pos++;
		}
	}

	return pattern.str();
}

} // namespace duckdb
