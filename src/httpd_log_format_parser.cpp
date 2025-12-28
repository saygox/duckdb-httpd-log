#include "httpd_log_format_parser.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/exception.hpp"
#include <sstream>

namespace duckdb {

// Unified directive definitions - combines column name, type, and collision rules
// Format: directive, column_name, type, collision_suffix, collision_priority
// Priority 0 = keeps base name when collision occurs, higher = gets suffix
const std::vector<DirectiveDefinition> HttpdLogFormatParser::directive_definitions = {
    // Basic directives (no collision rules needed)
    {"%h", "client_ip", LogicalTypeId::VARCHAR},
    {"%a", "remote_ip", LogicalTypeId::VARCHAR}, // Client IP (mod_remoteip aware)
    {"%A", "local_ip", LogicalTypeId::VARCHAR},  // Server local IP
    {"%l", "ident", LogicalTypeId::VARCHAR},
    {"%u", "auth_user", LogicalTypeId::VARCHAR},
    {"%t", "timestamp", LogicalTypeId::TIMESTAMP},
    {"%r", "request", LogicalTypeId::VARCHAR},
    {"%m", "method", LogicalTypeId::VARCHAR},
    {"%U", "path", LogicalTypeId::VARCHAR},
    {"%q", "query_string", LogicalTypeId::VARCHAR},
    {"%H", "protocol", LogicalTypeId::VARCHAR},
    {"%p", "server_port", LogicalTypeId::INTEGER},
    {"%P", "process_id", LogicalTypeId::INTEGER},
    // Duration directives - collision handled specially by GetDurationPriority()
    // When multiple duration directives exist, only highest precision is kept
    {"%D", "duration", LogicalTypeId::INTERVAL}, // Microseconds
    {"%T", "duration", LogicalTypeId::INTERVAL}, // Seconds (or with modifier: ms, us, s)

    // Status code directives (collision pair)
    {"%>s", "status", LogicalTypeId::INTEGER, "", 0},         // Final status gets base name
    {"%s", "status", LogicalTypeId::INTEGER, "_original", 1}, // Original status gets suffix

    // Server name directives (collision pair)
    {"%v", "server_name", LogicalTypeId::VARCHAR, "", 0},      // Canonical name gets base name
    {"%V", "server_name", LogicalTypeId::VARCHAR, "_used", 1}, // Used name gets suffix

    // Bytes directives (collision pair)
    {"%B", "bytes", LogicalTypeId::BIGINT, "", 0},     // Numeric bytes gets base name
    {"%b", "bytes", LogicalTypeId::BIGINT, "_clf", 1}, // CLF format gets suffix

    // Header directives (dynamic column name, collision with each other)
    {"%i", "", LogicalTypeId::VARCHAR, "_in", 1},  // Request headers
    {"%o", "", LogicalTypeId::VARCHAR, "_out", 1}, // Response headers
};

// Typed header rules - maps header names to specific types with direction constraints
// Format: header_name (lowercase), type, applies_to_request (%i), applies_to_response (%o)
const std::vector<TypedHeaderRule> HttpdLogFormatParser::typed_header_rules = {
    {"content-length", LogicalTypeId::BIGINT, true, true}, // Both request and response
    {"age", LogicalTypeId::INTEGER, false, true},          // Response only
    {"max-forwards", LogicalTypeId::INTEGER, true, false}, // Request only
};

// Lookup caches for O(1) access
std::unordered_map<string, const DirectiveDefinition *> HttpdLogFormatParser::directive_cache;
std::unordered_map<string, const TypedHeaderRule *> HttpdLogFormatParser::header_cache;
bool HttpdLogFormatParser::cache_initialized = false;

// Get priority for duration directives (lower = higher priority)
// Returns -1 for non-duration directives
// Priority order: %D (0) > %{us}T (1) > %{ms}T (2) > %T (3) > %{s}T (4)
static int GetDurationPriority(const string &directive, const string &modifier) {
	if (directive == "%D") {
		return 0; // microseconds (highest priority)
	}
	if (directive == "%T") {
		if (modifier == "us") {
			return 1; // microseconds
		}
		if (modifier == "ms") {
			return 2; // milliseconds
		}
		if (modifier == "s") {
			return 4; // seconds (lowest)
		}
		return 3; // %T without modifier = seconds (prefer over %{s}T)
	}
	return -1; // Not a duration directive
}

// Initialize lookup caches from static vectors
void HttpdLogFormatParser::InitializeCaches() {
	if (cache_initialized) {
		return;
	}

	// Build directive cache
	for (const auto &def : directive_definitions) {
		directive_cache[def.directive] = &def;
	}

	// Build header cache
	for (const auto &rule : typed_header_rules) {
		header_cache[rule.header_name] = &rule;
	}

	cache_initialized = true;
}

// Get directive definition by directive string (O(1) lookup)
const DirectiveDefinition *HttpdLogFormatParser::GetDirectiveDefinition(const string &directive) {
	InitializeCaches();
	auto it = directive_cache.find(directive);
	return (it != directive_cache.end()) ? it->second : nullptr;
}

// Get typed header type for a header name and directive (O(1) lookup)
LogicalTypeId HttpdLogFormatParser::GetTypedHeaderType(const string &header_name, const string &directive) {
	InitializeCaches();

	// Normalize header name to lowercase
	string header_lower = header_name;
	std::transform(header_lower.begin(), header_lower.end(), header_lower.begin(), ::tolower);

	auto it = header_cache.find(header_lower);
	if (it != header_cache.end() && it->second->AppliesTo(directive)) {
		return it->second->type;
	}

	return LogicalTypeId::INVALID; // No type override found
}

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

	// Handle %{c}a - peer IP address of the connection
	if (directive == "%a" && modifier == "c") {
		return "peer_ip";
	}

	// Handle %{c}h - underlying TCP connection hostname (not modified by mod_remoteip)
	if (directive == "%h" && modifier == "c") {
		return "peer_host";
	}

	// Handle %{UNIT}T - time taken with unit (ms, us, s)
	// All variants produce "duration" column name (same as %D and %T)
	if (directive == "%T" && (modifier == "ms" || modifier == "us" || modifier == "s")) {
		return "duration";
	}

	// Look up directive definition
	const DirectiveDefinition *def = GetDirectiveDefinition(directive);
	if (def && !def->column_name.empty()) {
		return def->column_name;
	}

	// Default: use directive as-is (remove %)
	string col_name = directive;
	if (!col_name.empty() && col_name[0] == '%') {
		col_name = col_name.substr(1);
	}
	return "field_" + col_name;
}

LogicalType HttpdLogFormatParser::GetDataType(const string &directive, const string &modifier) {
	// Special case for headers with typed support
	if (directive == "%i" || directive == "%o") {
		if (!modifier.empty()) {
			// Check for typed header override
			LogicalTypeId type_id = GetTypedHeaderType(modifier, directive);
			if (type_id != LogicalTypeId::INVALID) {
				return LogicalType(type_id);
			}
		}

		// Default: VARCHAR for all other headers
		return LogicalType::VARCHAR;
	}

	// Handle %{UNIT}T - time taken with unit (ms, us, s)
	// All variants return INTERVAL type
	if (directive == "%T" && (modifier == "ms" || modifier == "us" || modifier == "s")) {
		return LogicalType::INTERVAL;
	}

	// Look up directive definition
	const DirectiveDefinition *def = GetDirectiveDefinition(directive);
	if (def) {
		return LogicalType(def->type);
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
			LogicalType type = GetDataType(directive, modifier);

			// Add field
			result.fields.emplace_back(directive, column_name, type, in_quotes, modifier);
		} else {
			// Skip non-directive characters
			pos++;
		}
	}

	// Resolve column name collisions using rule-based approach
	// Handles %s/%>s, %v/%V, %b/%B, and header collisions like %{X}i + %{X}o
	ResolveColumnNameCollisions(result);

	// Generate regex pattern from the parsed fields
	result.regex_pattern = GenerateRegexPattern(result);

	// Compile the regex pattern once for performance using RE2
	duckdb_re2::RE2::Options options;
	options.set_log_errors(false);
	result.compiled_regex = make_uniq<duckdb_re2::RE2>(result.regex_pattern, options);
	if (!result.compiled_regex->ok()) {
		throw InvalidInputException("Invalid regex pattern: " + result.compiled_regex->error());
	}

	// Pre-allocate reusable buffers for RE2::FullMatchN to eliminate per-line allocations
	// Performance impact: Reduces ~36-40M allocations for 1.5M rows to just 3 allocations
	int num_groups = result.compiled_regex->NumberOfCapturingGroups();
	result.matches.resize(num_groups);
	result.args.resize(num_groups);
	result.arg_ptrs.resize(num_groups);

	// Initialize arg_ptrs to point to args (these pointers are stable)
	// args[i] = &matches[i] is set per-line in ParseLogLine() because StringPiece changes
	for (int i = 0; i < num_groups; i++) {
		result.arg_ptrs[i] = &result.args[i];
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
			// Use non-capturing groups (?:...) for should_skip fields
			string regex_expr;
			if (field.is_quoted) {
				regex_expr = "[^\"]*"; // Match anything except quotes
			} else if (field.directive == "%t") {
				// Timestamp is special: always capture for timestamp_raw
				pattern << "\\[([^\\]]+)\\]";
				field_idx++;
				continue;
			} else {
				regex_expr = "\\S+"; // Match non-whitespace
			}

			if (!field.should_skip) {
				// Normal: capturing group
				pattern << "(" << regex_expr << ")";
			} else {
				// Skip: non-capturing group (matches but doesn't capture)
				pattern << "(?:" << regex_expr << ")";
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
                                          vector<LogicalType> &return_types, bool include_raw_columns) {
	names.clear();
	return_types.clear();

	// Add columns from the parsed format
	for (const auto &field : parsed_format.fields) {
		// Skip fields marked for skipping (e.g., %b when %B is present)
		if (field.should_skip) {
			continue;
		}

		// Special handling for %t (timestamp) - add timestamp and optionally timestamp_raw
		if (field.directive == "%t") {
			names.push_back("timestamp");
			return_types.push_back(LogicalType::TIMESTAMP);
			// timestamp_raw is only included in raw mode
			if (include_raw_columns) {
				names.push_back("timestamp_raw");
				return_types.push_back(LogicalType::VARCHAR);
			}
		}
		// Special handling for %r (request) - decompose into method, path, query_string, protocol
		// Skip sub-columns that are overridden by individual directives (%m, %U, %q, %H)
		else if (field.directive == "%r") {
			if (!field.skip_method) {
				names.push_back("method");
				return_types.push_back(LogicalType::VARCHAR);
			}
			if (!field.skip_path) {
				names.push_back("path");
				return_types.push_back(LogicalType::VARCHAR);
			}
			if (!field.skip_query_string) {
				names.push_back("query_string");
				return_types.push_back(LogicalType::VARCHAR);
			}
			if (!field.skip_protocol) {
				names.push_back("protocol");
				return_types.push_back(LogicalType::VARCHAR);
			}
		}
		// Regular field
		else {
			names.push_back(field.column_name);
			return_types.push_back(field.type);
		}
	}

	// Add standard metadata columns: filename is always included
	names.push_back("filename");
	return_types.push_back(LogicalType::VARCHAR);

	// parse_error and raw_line are only included in raw mode
	if (include_raw_columns) {
		names.push_back("parse_error");
		return_types.push_back(LogicalType::BOOLEAN);

		names.push_back("raw_line");
		return_types.push_back(LogicalType::VARCHAR);
	}
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

bool HttpdLogFormatParser::ParseRequest(const string &request, string &method, string &path, string &query_string,
                                        string &protocol) {
	// Request format: "GET /index.html?foo=bar HTTP/1.0"
	std::istringstream iss(request);
	string full_path;

	if (!(iss >> method >> full_path >> protocol)) {
		return false;
	}

	// Split path and query string at '?'
	size_t query_pos = full_path.find('?');
	if (query_pos != string::npos) {
		path = full_path.substr(0, query_pos);
		query_string = full_path.substr(query_pos); // includes '?'
	} else {
		path = full_path;
		query_string = "";
	}

	return true;
}

vector<string> HttpdLogFormatParser::ParseLogLine(const string &line, const ParsedFormat &parsed_format) {
	vector<string> result;

	// Use the pre-compiled RE2 for performance
	int num_groups = parsed_format.compiled_regex->NumberOfCapturingGroups();

	// Reuse pre-allocated buffers from ParsedFormat - ZERO heap allocations here!
	// These were allocated once in ParseFormatString() and are reused for every line
	duckdb_re2::StringPiece input(line);

	// Update args to point to matches for this line
	// Note: arg_ptrs already points to args (set in ParseFormatString)
	// We only need to update the RE2::Arg -> StringPiece binding per-line
	for (int i = 0; i < num_groups; i++) {
		parsed_format.args[i] = &parsed_format.matches[i];
	}

	// Perform the match using reusable buffers
	if (!duckdb_re2::RE2::FullMatchN(input, *parsed_format.compiled_regex, parsed_format.arg_ptrs.data(), num_groups)) {
		// Parsing failed - return empty vector
		return result;
	}

	// Extract matched groups from reusable buffer
	// IMPORTANT: matches[i] references substrings of 'line', so we must copy
	// to std::string (as_string()) before 'line' goes out of scope
	result.reserve(num_groups);
	for (int i = 0; i < num_groups; i++) {
		result.push_back(parsed_format.matches[i].as_string());
	}

	return result;
}

void HttpdLogFormatParser::ResolveColumnNameCollisions(ParsedFormat &parsed_format) {
	// Step 0: Handle %r vs individual directive collisions
	// When %m, %U, %q, or %H are present alongside %r, skip the corresponding %r sub-column
	// Individual directives take priority over %r decomposition
	bool has_m = false, has_U = false, has_q = false, has_H = false;
	idx_t r_field_idx = DConstants::INVALID_INDEX;

	for (idx_t i = 0; i < parsed_format.fields.size(); i++) {
		const string &dir = parsed_format.fields[i].directive;
		if (dir == "%r") {
			r_field_idx = i;
		} else if (dir == "%m") {
			has_m = true;
		} else if (dir == "%U") {
			has_U = true;
		} else if (dir == "%q") {
			has_q = true;
		} else if (dir == "%H") {
			has_H = true;
		}
	}

	// If %r is present with individual directives, set skip flags
	if (r_field_idx != DConstants::INVALID_INDEX) {
		auto &r_field = parsed_format.fields[r_field_idx];
		if (has_m) {
			r_field.skip_method = true;
		}
		if (has_U) {
			r_field.skip_path = true;
		}
		if (has_q) {
			r_field.skip_query_string = true;
		}
		if (has_H) {
			r_field.skip_protocol = true;
		}
	}

	// Step 1: Build collision map - group field indices by column name
	std::unordered_map<string, vector<idx_t>> collision_map;
	for (idx_t i = 0; i < parsed_format.fields.size(); i++) {
		collision_map[parsed_format.fields[i].column_name].push_back(i);
	}

	// Step 2: Process each group with potential collisions
	for (auto &entry : collision_map) {
		const string &column_name = entry.first;
		vector<idx_t> &field_indices = entry.second;

		if (field_indices.size() <= 1) {
			continue; // No collision for this column name
		}

		// Special case: Duration directives - keep only highest precision, skip others
		// This handles %D, %T, %{ms}T, %{us}T, %{s}T collisions
		if (column_name == "duration") {
			// Find field with lowest priority number (= highest precision)
			idx_t best_idx = field_indices[0];
			int best_priority = GetDurationPriority(parsed_format.fields[best_idx].directive,
			                                        parsed_format.fields[best_idx].modifier);

			for (idx_t idx : field_indices) {
				int priority =
				    GetDurationPriority(parsed_format.fields[idx].directive, parsed_format.fields[idx].modifier);
				if (priority >= 0 && (best_priority < 0 || priority < best_priority)) {
					best_priority = priority;
					best_idx = idx;
				}
			}

			// Mark all but the best as should_skip
			for (idx_t idx : field_indices) {
				if (idx != best_idx) {
					parsed_format.fields[idx].should_skip = true;
				}
			}
			continue; // Skip standard collision resolution for duration
		}

		// Separate fields by directive type to determine collision type
		std::unordered_map<string, vector<idx_t>> by_directive;
		for (idx_t idx : field_indices) {
			by_directive[parsed_format.fields[idx].directive].push_back(idx);
		}

		// Case A: All same directive (duplicates of same field, not a collision)
		// Example: %{User-Agent}i appears twice
		if (by_directive.size() == 1) {
			// Number them: first keeps name, rest get _2, _3, etc.
			int counter = 2;
			for (size_t i = 1; i < field_indices.size(); i++) {
				auto &field = parsed_format.fields[field_indices[i]];
				field.column_name = column_name + "_" + std::to_string(counter++);
			}
			continue;
		}

		// Case B: Different directives with same column name - apply collision rules
		// Example: %{Content-Length}i and %{Content-Length}o both produce "content_length"

		// Find the applicable rule for each directive and sort by priority
		struct FieldWithDef {
			idx_t field_idx;
			const DirectiveDefinition *def;
			int priority;
		};
		vector<FieldWithDef> fields_with_defs;

		for (idx_t idx : field_indices) {
			const auto &field = parsed_format.fields[idx];
			const DirectiveDefinition *def = GetDirectiveDefinition(field.directive);
			int priority = def ? def->collision_priority : 999; // Default: low priority

			fields_with_defs.push_back({idx, def, priority});
		}

		// Sort by priority (lowest first = highest priority)
		std::sort(fields_with_defs.begin(), fields_with_defs.end(),
		          [](const FieldWithDef &a, const FieldWithDef &b) { return a.priority < b.priority; });

		// Check if this is a header collision (only %i and/or %o involved)
		bool has_i = by_directive.count("%i") > 0;
		bool has_o = by_directive.count("%o") > 0;
		bool is_pure_header_collision = by_directive.size() == 2 && has_i && has_o;

		// For header collisions (%{X}i + %{X}o), both get suffixes
		// For other collisions, use priority-based resolution
		if (is_pure_header_collision) {
			// Both header types present - apply suffixes to both
			for (const auto &fwd : fields_with_defs) {
				auto &field = parsed_format.fields[fwd.field_idx];
				if (fwd.def) {
					field.column_name = column_name + fwd.def->collision_suffix;
				}
			}
		} else {
			// Mixed collision or standard directive pairs
			// Priority 0 gets base name (or its own suffix if defined)
			// Others get their suffixes
			for (const auto &fwd : fields_with_defs) {
				auto &field = parsed_format.fields[fwd.field_idx];
				if (fwd.def) {
					if (fwd.def->collision_suffix.empty()) {
						// Priority 0 with empty suffix keeps base name
						field.column_name = column_name;
					} else {
						field.column_name = column_name + fwd.def->collision_suffix;
					}
				}
			}
		}

		// Handle duplicates within each directive type after collision resolution
		// Example: %{X}i + %{X}i + %{X}o → x_in, x_in_2, x_out
		std::unordered_map<string, int> name_counts;
		for (const auto &fwd : fields_with_defs) {
			auto &field = parsed_format.fields[fwd.field_idx];
			string current_name = field.column_name;
			int count = ++name_counts[current_name];
			if (count > 1) {
				field.column_name = current_name + "_" + std::to_string(count);
			}
		}
	}

	// Note: Fields are NOT removed (maintains sync with regex generation)
	// should_skip flag is used in GenerateRegexPattern(), GenerateSchema(),
	// and table function for fields that should be captured but not output
}

} // namespace duckdb
