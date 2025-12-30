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
    // Request line directives (original/final collision pair)
    {"%>r", "request", LogicalTypeId::VARCHAR, "", 0},          // Final request gets base name
    {"%r", "request", LogicalTypeId::VARCHAR, "_original", 1},  // Original request (default) gets suffix
    {"%<r", "request", LogicalTypeId::VARCHAR, "_original", 1}, // Explicit original gets suffix

    {"%m", "method", LogicalTypeId::VARCHAR},

    // URL path directives (original/final collision pair)
    {"%>U", "path", LogicalTypeId::VARCHAR, "", 0},          // Final path gets base name
    {"%U", "path", LogicalTypeId::VARCHAR, "_original", 1},  // Original path (default) gets suffix
    {"%<U", "path", LogicalTypeId::VARCHAR, "_original", 1}, // Explicit original gets suffix
    {"%q", "query_string", LogicalTypeId::VARCHAR},
    {"%H", "protocol", LogicalTypeId::VARCHAR},
    {"%p", "server_port", LogicalTypeId::INTEGER},
    {"%k", "keepalive_count", LogicalTypeId::INTEGER},
    {"%X", "connection_status", LogicalTypeId::VARCHAR},
    // Process/Thread ID directives - collision handled specially
    // %P and %{pid}P both produce process_id, %P takes priority
    {"%P", "process_id", LogicalTypeId::INTEGER},
    // Duration directives - collision handled specially by GetDurationPriority()
    // When multiple duration directives exist, only highest precision is kept
    // Also supports original/final modifiers
    {"%>D", "duration", LogicalTypeId::INTERVAL, "", 0},          // Final duration (us) gets base name
    {"%D", "duration", LogicalTypeId::INTERVAL, "_original", 1},  // Original duration (us, default) gets suffix
    {"%<D", "duration", LogicalTypeId::INTERVAL, "_original", 1}, // Explicit original duration (us)
    {"%>T", "duration", LogicalTypeId::INTERVAL, "", 0},          // Final duration (s) gets base name
    {"%T", "duration", LogicalTypeId::INTERVAL, "_original", 1},  // Original duration (s, default) gets suffix
    {"%<T", "duration", LogicalTypeId::INTERVAL, "_original", 1}, // Explicit original duration (s)

    // Status code directives (original/final collision pair)
    {"%>s", "status", LogicalTypeId::INTEGER, "", 0},          // Final status gets base name
    {"%s", "status", LogicalTypeId::INTEGER, "_original", 1},  // Original status (default) gets suffix
    {"%<s", "status", LogicalTypeId::INTEGER, "_original", 1}, // Explicit original status gets suffix

    // Server name directives (collision pair)
    {"%v", "server_name", LogicalTypeId::VARCHAR, "", 0},      // Canonical name gets base name
    {"%V", "server_name", LogicalTypeId::VARCHAR, "_used", 1}, // Used name gets suffix

    // Bytes directives (%b and %B are equivalent after "-" to 0 conversion)
    // When both present, first occurrence is kept, second is skipped
    {"%B", "bytes", LogicalTypeId::BIGINT},
    {"%b", "bytes", LogicalTypeId::BIGINT},

    // mod_logio byte counting directives (separate from %b/%B)
    {"%I", "bytes_received", LogicalTypeId::BIGINT},
    {"%O", "bytes_sent", LogicalTypeId::BIGINT},
    {"%S", "bytes_transferred", LogicalTypeId::BIGINT},

    // Filename, request log ID, and handler
    {"%f", "filename", LogicalTypeId::VARCHAR},
    {"%L", "request_log_id", LogicalTypeId::VARCHAR},
    {"%R", "handler", LogicalTypeId::VARCHAR},

    // Header directives (dynamic column name, unique priorities for collision resolution)
    // Priority 0 = final (e.g., %>s), Priority 1 = original (e.g., %s), Priority 2+ = dynamic directives
    {"%i", "", LogicalTypeId::VARCHAR, "_in", 2},  // Request headers
    {"%o", "", LogicalTypeId::VARCHAR, "_out", 3}, // Response headers

    // Cookie, Environment, Note directives (dynamic column name)
    {"%C", "", LogicalTypeId::VARCHAR, "_cookie", 4},
    {"%e", "", LogicalTypeId::VARCHAR, "_env", 5},
    {"%n", "", LogicalTypeId::VARCHAR, "_note", 6},

    // Trailer directives (dynamic column name)
    {"%^ti", "", LogicalTypeId::VARCHAR, "_trail_in", 7},
    {"%^to", "", LogicalTypeId::VARCHAR, "_trail_out", 8},
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

// Convert strftime format specifier to regex pattern
static string StrftimeToRegex(const string &format) {
	string regex;
	size_t i = 0;
	while (i < format.length()) {
		if (format[i] == '%' && i + 1 < format.length()) {
			char next = format[i + 1];
			string spec;

			// Check for %- prefix (non-padded)
			if (next == '-' && i + 2 < format.length()) {
				spec = format.substr(i, 3);
				i += 3;
			} else {
				spec = format.substr(i, 2);
				i += 2;
			}

			// Map strftime specifiers to regex patterns
			if (spec == "%Y") {
				regex += "\\d{4}"; // Year with century
			} else if (spec == "%y") {
				regex += "\\d{2}"; // Year without century
			} else if (spec == "%m") {
				regex += "\\d{2}"; // Month 01-12
			} else if (spec == "%-m") {
				regex += "\\d{1,2}"; // Month 1-12
			} else if (spec == "%d") {
				regex += "\\d{2}"; // Day 01-31
			} else if (spec == "%-d") {
				regex += "\\d{1,2}"; // Day 1-31
			} else if (spec == "%e") {
				regex += "[\\s\\d]\\d"; // Day with space padding
			} else if (spec == "%b" || spec == "%h") {
				regex += "[A-Za-z]{3}"; // Abbreviated month name
			} else if (spec == "%B") {
				regex += "[A-Za-z]+"; // Full month name
			} else if (spec == "%H") {
				regex += "\\d{2}"; // Hour 00-23
			} else if (spec == "%-H") {
				regex += "\\d{1,2}"; // Hour 0-23
			} else if (spec == "%I") {
				regex += "\\d{2}"; // Hour 01-12
			} else if (spec == "%-I") {
				regex += "\\d{1,2}"; // Hour 1-12
			} else if (spec == "%M") {
				regex += "\\d{2}"; // Minute 00-59
			} else if (spec == "%S") {
				regex += "\\d{2}"; // Second 00-59
			} else if (spec == "%f") {
				regex += "\\d{6}"; // Microseconds
			} else if (spec == "%z") {
				regex += "[+-]\\d{4}"; // UTC offset +HHMM
			} else if (spec == "%Z") {
				regex += "[A-Za-z/_]+"; // Time zone name
			} else if (spec == "%T") {
				regex += "\\d{2}:\\d{2}:\\d{2}"; // %H:%M:%S
			} else if (spec == "%R") {
				regex += "\\d{2}:\\d{2}"; // %H:%M
			} else if (spec == "%j") {
				regex += "\\d{3}"; // Day of year
			} else if (spec == "%a") {
				regex += "[A-Za-z]{3}"; // Abbreviated weekday
			} else if (spec == "%A") {
				regex += "[A-Za-z]+"; // Full weekday
			} else if (spec == "%p" || spec == "%P") {
				regex += "[AaPp][Mm]"; // AM/PM
			} else if (spec == "%n") {
				regex += "\\n"; // Newline
			} else if (spec == "%t") {
				regex += "\\t"; // Tab
			} else if (spec == "%%") {
				regex += "%"; // Literal %
			} else {
				// Unknown specifier - match any non-whitespace
				regex += "\\S+";
			}
		} else {
			// Literal character - escape regex metacharacters
			char c = format[i];
			if (c == '.' || c == '*' || c == '+' || c == '?' || c == '^' || c == '$' || c == '(' || c == ')' ||
			    c == '[' || c == ']' || c == '{' || c == '}' || c == '|' || c == '\\') {
				regex += '\\';
			}
			regex += c;
			i++;
		}
	}
	return regex;
}

// Get priority for duration directives (lower = higher priority)
// Returns -1 for non-duration directives
// Priority order: %D variants (0-1) > %{us}T (2) > %{ms}T (3) > %T (4) > %{s}T (5)
// Final (>) variants get same precision priority but win in collision resolution via suffix
static int GetDurationPriority(const string &directive, const string &modifier) {
	// Handle %D variants (microseconds - highest precision)
	if (directive == "%D" || directive == "%>D" || directive == "%<D") {
		return 0;
	}
	// Handle %T variants
	if (directive == "%T" || directive == "%>T" || directive == "%<T") {
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
	// Handle special case for directives with dynamic column names from modifier:
	// %{...}i (request headers), %{...}o (response headers),
	// %{...}C (cookies), %{...}e (env vars), %{...}n (notes),
	// %{...}^ti (request trailers), %{...}^to (response trailers)
	if (directive == "%i" || directive == "%o" || directive == "%C" || directive == "%e" || directive == "%n" ||
	    directive == "%^ti" || directive == "%^to") {
		if (!modifier.empty()) {
			// Convert modifier to lowercase column name
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

	// Handle %{UNIT}T variants - time taken with unit (ms, us, s)
	// All variants produce "duration" column name (same as %D and %T)
	// For < > modifiers, the directive definition handles the suffix
	if ((directive == "%T" || directive == "%>T" || directive == "%<T") &&
	    (modifier == "ms" || modifier == "us" || modifier == "s")) {
		// Look up directive definition for proper suffix handling
		const DirectiveDefinition *def = GetDirectiveDefinition(directive);
		if (def) {
			return def->column_name;
		}
		return "duration";
	}

	// Handle %{format}P - process/thread ID variants
	if (directive == "%P") {
		if (modifier == "pid" || modifier.empty()) {
			return "process_id";
		} else if (modifier == "tid") {
			return "thread_id";
		} else if (modifier == "hextid") {
			return "thread_id_hex";
		}
	}

	// Handle %{format}p - port variants
	if (directive == "%p") {
		if (modifier == "canonical" || modifier.empty()) {
			return "server_port";
		} else if (modifier == "local") {
			return "local_port";
		} else if (modifier == "remote") {
			return "remote_port";
		}
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

	// Cookie, Environment, Note, and Trailer directives - always VARCHAR
	if (directive == "%C" || directive == "%e" || directive == "%n" || directive == "%^ti" || directive == "%^to") {
		return LogicalType::VARCHAR;
	}

	// Handle %{UNIT}T variants - time taken with unit (ms, us, s)
	// All variants return INTERVAL type
	if ((directive == "%T" || directive == "%>T" || directive == "%<T") &&
	    (modifier == "ms" || modifier == "us" || modifier == "s")) {
		return LogicalType::INTERVAL;
	}

	// Handle %{format}P - process/thread ID variants
	if (directive == "%P") {
		if (modifier == "pid" || modifier.empty()) {
			return LogicalType::INTEGER;
		} else if (modifier == "tid") {
			return LogicalType::BIGINT; // Thread IDs can be large
		} else if (modifier == "hextid") {
			return LogicalType::VARCHAR; // Hex format
		}
	}

	// Handle %{format}p - port variants (all INTEGER)
	if (directive == "%p" && (modifier == "canonical" || modifier == "local" || modifier == "remote")) {
		return LogicalType::INTEGER;
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

			// Skip optional status code condition: %400,501{...} or %!200,304{...}
			// These conditions filter logging by HTTP status code, but we ignore them
			// and just parse the directive itself
			size_t directive_start = pos + 1;
			if (directive_start < format_str.length()) {
				// Skip '!' if present (negation)
				if (format_str[directive_start] == '!') {
					directive_start++;
				}
				// Skip digits and commas (status codes like 400,501)
				while (directive_start < format_str.length() &&
				       (isdigit(format_str[directive_start]) || format_str[directive_start] == ',')) {
					directive_start++;
				}
			}

			// Check for modifiers like %{...}i, %{...}o, %{...}^ti, %{...}^to, %{...}t
			if (directive_start < format_str.length() && format_str[directive_start] == '{') {
				// Find the closing }
				size_t close_pos = format_str.find('}', directive_start + 1);
				if (close_pos != string::npos && close_pos + 1 < format_str.length()) {
					modifier = format_str.substr(directive_start + 1, close_pos - directive_start - 1);
					// Check for ^ti or ^to trailer directives
					if (format_str[close_pos + 1] == '^' && close_pos + 3 < format_str.length()) {
						directive = "%" + format_str.substr(close_pos + 1, 3); // %^ti or %^to
						pos = close_pos + 4;
					} else {
						// Single char directive (i, o, C, e, n, t, etc.)
						char type_char = format_str[close_pos + 1];
						directive = "%" + string(1, type_char);
						pos = close_pos + 2;
					}
				} else {
					// Malformed directive, skip
					pos++;
					continue;
				}
			} else {
				// Standard directive (1-2 characters after %) or with status condition
				// For status conditions like %200s, directive_start points past the condition
				size_t dir_start = (directive_start == pos + 1) ? pos : directive_start;

				// Check for %<X or %>X (original/final modifiers)
				if (dir_start + 1 < format_str.length() && format_str[dir_start] == '%' &&
				    (format_str[dir_start + 1] == '>' || format_str[dir_start + 1] == '<')) {
					directive = format_str.substr(dir_start, 3); // e.g., %>s, %<s, %>U, %<U
					pos = dir_start + 3;
				} else if (directive_start > pos + 1) {
					// Status condition present, directive starts at directive_start
					directive = "%" + string(1, format_str[directive_start]);
					pos = directive_start + 1;
				} else {
					directive = format_str.substr(pos, 2);
					pos += 2;
				}
			}

			// Get column name and type for this directive
			string column_name = GetColumnName(directive, modifier);
			LogicalType type = GetDataType(directive, modifier);

			// Add field
			result.fields.emplace_back(directive, column_name, type, in_quotes, modifier);

			// Set timestamp format type for %t directives
			if (directive == "%t") {
				auto &field = result.fields.back();
				if (modifier.empty()) {
					field.timestamp_type = TimestampFormatType::APACHE_DEFAULT;
				} else if (modifier == "sec") {
					field.timestamp_type = TimestampFormatType::EPOCH_SEC;
				} else if (modifier == "msec") {
					field.timestamp_type = TimestampFormatType::EPOCH_MSEC;
				} else if (modifier == "usec") {
					field.timestamp_type = TimestampFormatType::EPOCH_USEC;
				} else if (modifier == "msec_frac") {
					field.timestamp_type = TimestampFormatType::FRAC_MSEC;
				} else if (modifier == "usec_frac") {
					field.timestamp_type = TimestampFormatType::FRAC_USEC;
				} else {
					// Strip begin: or end: prefix if present
					// end: timestamps are final (get base name), begin: or no prefix are original
					string strftime_fmt = modifier;
					if (strftime_fmt.substr(0, 6) == "begin:") {
						strftime_fmt = strftime_fmt.substr(6);
						field.is_end_timestamp = false;
					} else if (strftime_fmt.substr(0, 4) == "end:") {
						strftime_fmt = strftime_fmt.substr(4);
						field.is_end_timestamp = true;
					}
					field.timestamp_type = TimestampFormatType::STRFTIME;
					field.strftime_format = strftime_fmt;
				}
			}
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
				// Timestamp directives - pattern depends on format type
				// NOTE: For timestamp groups, ALL %t fields must be captured (not non-capturing)
				// because we need all values to combine them into a single timestamp.
				// This differs from other should_skip fields which use non-capturing groups.
				string ts_regex;
				switch (field.timestamp_type) {
				case TimestampFormatType::APACHE_DEFAULT:
					// Plain %t: bracketed Apache format [DD/MMM/YYYY:HH:MM:SS TZ]
					pattern << "\\[([^\\]]+)\\]";
					field_idx++;
					continue;
				case TimestampFormatType::EPOCH_SEC:
				case TimestampFormatType::EPOCH_MSEC:
				case TimestampFormatType::EPOCH_USEC:
					ts_regex = "\\d+"; // Epoch timestamps are just digits
					break;
				case TimestampFormatType::FRAC_MSEC:
					ts_regex = "\\d{3}"; // Millisecond fraction: 3 digits
					break;
				case TimestampFormatType::FRAC_USEC:
					ts_regex = "\\d{6}"; // Microsecond fraction: 6 digits
					break;
				case TimestampFormatType::STRFTIME:
					ts_regex = StrftimeToRegex(field.strftime_format);
					break;
				}
				// Always use capturing group for %t (even if should_skip) because
				// we need to combine values from timestamp groups
				pattern << "(" << ts_regex << ")";
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

		// Special handling for %t (timestamp)
		if (field.directive == "%t") {
			names.push_back(field.column_name);
			return_types.push_back(LogicalType::TIMESTAMP);
		}
		// Special handling for %r variants (request) - decompose into method, path, query_string, protocol
		// Skip sub-columns that are overridden by individual directives (%m, %U, %q, %H)
		else if (field.directive == "%r" || field.directive == "%>r" || field.directive == "%<r") {
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

	// Add standard metadata columns: log_file is always included
	names.push_back("log_file");
	return_types.push_back(LogicalType::VARCHAR);

	// line_number, parse_error and raw_line are only included in raw mode
	if (include_raw_columns) {
		names.push_back("line_number");
		return_types.push_back(LogicalType::BIGINT);

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

	// If no compiled regex (unknown format), return empty to indicate parse error
	if (!parsed_format.compiled_regex) {
		return result;
	}

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

// Helper to check if a directive is a request line variant (%r, %>r, %<r)
static bool IsRequestLineDirective(const string &dir) {
	return dir == "%r" || dir == "%>r" || dir == "%<r";
}

// Helper to check if a directive is a path variant (%U, %>U, %<U)
static bool IsPathDirective(const string &dir) {
	return dir == "%U" || dir == "%>U" || dir == "%<U";
}

void HttpdLogFormatParser::ResolveColumnNameCollisions(ParsedFormat &parsed_format) {
	// Step 0: Handle %r variants vs individual directive collisions
	// When %m, %U variants, %q, or %H are present alongside %r variants, skip the corresponding sub-column
	// Individual directives take priority over %r decomposition
	bool has_m = false, has_U = false, has_q = false, has_H = false;
	idx_t r_field_idx = DConstants::INVALID_INDEX;

	for (idx_t i = 0; i < parsed_format.fields.size(); i++) {
		const string &dir = parsed_format.fields[i].directive;
		if (IsRequestLineDirective(dir)) {
			r_field_idx = i;
		} else if (dir == "%m") {
			has_m = true;
		} else if (IsPathDirective(dir)) {
			has_U = true;
		} else if (dir == "%q") {
			has_q = true;
		} else if (dir == "%H") {
			has_H = true;
		}
	}

	// If %r variant is present with individual directives, set skip flags
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

	// Step 0.5: Group consecutive %t directives into timestamp groups
	// Multiple %t/%{format}t can be combined into a single timestamp column
	// begin: and end: timestamps are kept in separate groups
	{
		int current_group_id = 0;
		bool in_timestamp_group = false;
		bool current_group_is_end = false; // Track if current group is for end: timestamps

		for (idx_t i = 0; i < parsed_format.fields.size(); i++) {
			auto &field = parsed_format.fields[i];

			if (field.directive == "%t") {
				// Check if we need to start a new group due to begin/end mismatch
				bool should_start_new_group =
				    !in_timestamp_group || (in_timestamp_group && field.is_end_timestamp != current_group_is_end);

				if (should_start_new_group) {
					// End previous group if exists
					if (in_timestamp_group) {
						current_group_id++;
					}

					// Start new group
					in_timestamp_group = true;
					current_group_is_end = field.is_end_timestamp;
					field.timestamp_group_id = current_group_id;

					// Create new timestamp group
					TimestampGroup group;
					group.field_indices.push_back(i);

					// Set flags based on format type
					switch (field.timestamp_type) {
					case TimestampFormatType::APACHE_DEFAULT:
						group.has_plain_t = true;
						break;
					case TimestampFormatType::EPOCH_SEC:
					case TimestampFormatType::EPOCH_MSEC:
					case TimestampFormatType::EPOCH_USEC:
						group.has_epoch_component = true;
						break;
					case TimestampFormatType::FRAC_MSEC:
					case TimestampFormatType::FRAC_USEC:
						group.has_frac_component = true;
						break;
					case TimestampFormatType::STRFTIME:
						group.has_strftime_component = true;
						break;
					}

					parsed_format.timestamp_groups.push_back(group);
				} else {
					// Continue existing group - this %t is part of the current group
					field.timestamp_group_id = current_group_id;
					field.should_skip = true; // Skip in schema, will be combined

					// Update group with this field
					auto &group = parsed_format.timestamp_groups.back();
					group.field_indices.push_back(i);

					// Update flags
					switch (field.timestamp_type) {
					case TimestampFormatType::APACHE_DEFAULT:
						group.has_plain_t = true;
						break;
					case TimestampFormatType::EPOCH_SEC:
					case TimestampFormatType::EPOCH_MSEC:
					case TimestampFormatType::EPOCH_USEC:
						group.has_epoch_component = true;
						break;
					case TimestampFormatType::FRAC_MSEC:
					case TimestampFormatType::FRAC_USEC:
						group.has_frac_component = true;
						break;
					case TimestampFormatType::STRFTIME:
						group.has_strftime_component = true;
						break;
					}
				}
			} else {
				// Non-%t directive encountered
				if (in_timestamp_group) {
					// End current group and prepare for next one
					in_timestamp_group = false;
					current_group_id++;
				}
			}
		}
	}

	// Step 0.6: Rename timestamp columns for begin/end collision resolution
	// When both begin: and end: timestamps exist, end: gets "timestamp", begin: gets "timestamp_original"
	{
		bool has_end_timestamp = false;
		bool has_begin_timestamp = false;

		// Check what types of timestamp groups we have
		for (const auto &field : parsed_format.fields) {
			if (field.directive == "%t" && !field.should_skip) {
				if (field.is_end_timestamp) {
					has_end_timestamp = true;
				} else {
					has_begin_timestamp = true;
				}
			}
		}

		// If both exist, rename begin: timestamps to timestamp_original
		if (has_end_timestamp && has_begin_timestamp) {
			for (auto &field : parsed_format.fields) {
				if (field.directive == "%t" && !field.should_skip && !field.is_end_timestamp) {
					field.column_name = "timestamp_original";
				}
			}
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
		// This handles %D, %T, %{ms}T, %{us}T, %{s}T and their < > variants
		if (column_name == "duration" || column_name == "duration_original") {
			// Find field with lowest priority number (= highest precision)
			idx_t best_idx = field_indices[0];
			int best_priority =
			    GetDurationPriority(parsed_format.fields[best_idx].directive, parsed_format.fields[best_idx].modifier);

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

		// Special case: Process ID - %P and %{pid}P are equivalent
		// %P (no modifier) takes priority, %{pid}P gets skipped
		if (column_name == "process_id") {
			idx_t best_idx = field_indices[0];
			bool found_bare = false;

			// Find %P without modifier (preferred)
			for (idx_t idx : field_indices) {
				const auto &field = parsed_format.fields[idx];
				if (field.directive == "%P" && field.modifier.empty()) {
					best_idx = idx;
					found_bare = true;
					break;
				}
			}

			// If no bare %P, use first %{pid}P
			if (!found_bare) {
				for (idx_t idx : field_indices) {
					const auto &field = parsed_format.fields[idx];
					if (field.directive == "%P" && field.modifier == "pid") {
						best_idx = idx;
						break;
					}
				}
			}

			// Mark all but the best as should_skip
			for (idx_t idx : field_indices) {
				if (idx != best_idx) {
					parsed_format.fields[idx].should_skip = true;
				}
			}
			continue;
		}

		// Special case: Server Port - %p and %{canonical}p are equivalent
		// %p (no modifier) takes priority, %{canonical}p gets skipped
		if (column_name == "server_port") {
			idx_t best_idx = field_indices[0];
			bool found_bare = false;

			// Find %p without modifier (preferred)
			for (idx_t idx : field_indices) {
				const auto &field = parsed_format.fields[idx];
				if (field.directive == "%p" && field.modifier.empty()) {
					best_idx = idx;
					found_bare = true;
					break;
				}
			}

			// If no bare %p, use first %{canonical}p
			if (!found_bare) {
				for (idx_t idx : field_indices) {
					const auto &field = parsed_format.fields[idx];
					if (field.directive == "%p" && field.modifier == "canonical") {
						best_idx = idx;
						break;
					}
				}
			}

			// Mark all but the best as should_skip
			for (idx_t idx : field_indices) {
				if (idx != best_idx) {
					parsed_format.fields[idx].should_skip = true;
				}
			}
			continue;
		}

		// Special case: Bytes - %b and %B are equivalent (both produce "bytes")
		// "-" in %b is converted to 0, making them identical in value
		// Keep first occurrence, skip the rest
		if (column_name == "bytes") {
			// Mark all but the first as should_skip
			for (size_t i = 1; i < field_indices.size(); i++) {
				parsed_format.fields[field_indices[i]].should_skip = true;
			}
			continue;
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

		// Simple priority-based resolution
		// Lowest priority keeps base name, others get their defined suffixes
		// (All priorities are unique, so there's always a clear winner)
		bool first = true;
		for (const auto &fwd : fields_with_defs) {
			auto &field = parsed_format.fields[fwd.field_idx];
			if (first) {
				// Lowest priority keeps base name
				field.column_name = column_name;
				first = false;
			} else if (fwd.def && !fwd.def->collision_suffix.empty()) {
				// Others get their defined suffix
				field.column_name = column_name + fwd.def->collision_suffix;
			} else {
				// Fallback: append priority as suffix
				field.column_name = column_name + "_" + std::to_string(fwd.priority);
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

string HttpdLogFormatParser::DetectFormat(const vector<string> &sample_lines, ParsedFormat &parsed_format) {
	if (sample_lines.empty()) {
		// No sample lines - return unknown with raw-only schema
		parsed_format = ParsedFormat("");
		return "unknown";
	}

	// Define format strings for common and combined
	static const string combined_format = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"";
	static const string common_format = "%h %l %u %t \"%r\" %>s %b";

	// Try combined first (it's a superset of common, so if combined matches, use it)
	ParsedFormat combined_parsed = ParseFormatString(combined_format);
	int combined_matches = 0;
	for (const auto &line : sample_lines) {
		if (line.empty()) {
			continue;
		}
		vector<string> values = ParseLogLine(line, combined_parsed);
		if (!values.empty()) {
			combined_matches++;
		}
	}

	// If most lines match combined format, use it
	if (combined_matches > 0 && combined_matches >= static_cast<int>(sample_lines.size()) / 2) {
		parsed_format = std::move(combined_parsed);
		return "combined";
	}

	// Try common format
	ParsedFormat common_parsed = ParseFormatString(common_format);
	int common_matches = 0;
	for (const auto &line : sample_lines) {
		if (line.empty()) {
			continue;
		}
		vector<string> values = ParseLogLine(line, common_parsed);
		if (!values.empty()) {
			common_matches++;
		}
	}

	// If most lines match common format, use it
	if (common_matches > 0 && common_matches >= static_cast<int>(sample_lines.size()) / 2) {
		parsed_format = std::move(common_parsed);
		return "common";
	}

	// Neither format matched - return unknown with empty parsed format
	parsed_format = ParsedFormat("");
	return "unknown";
}

} // namespace duckdb
