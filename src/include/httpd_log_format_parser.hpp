#pragma once

#include "duckdb.hpp"
#include "re2/re2.h"
#include <string>
#include <vector>

namespace duckdb {

// Unified directive definition - combines column name, type, and collision rules
struct DirectiveDefinition {
	string directive;        // The format directive (e.g., "%h", "%t", "%i")
	string column_name;      // Default column name (e.g., "client_ip", empty for headers)
	LogicalTypeId type;      // Data type (e.g., VARCHAR, INTEGER, TIMESTAMP)
	string collision_suffix; // Suffix when collision occurs (e.g., "_in", "_out")
	int collision_priority;  // Resolution priority (0 = highest, keeps base name)

	DirectiveDefinition(string directive_p, string column_name_p, LogicalTypeId type_p, string collision_suffix_p = "",
	                    int collision_priority_p = 0)
	    : directive(std::move(directive_p)), column_name(std::move(column_name_p)), type(type_p),
	      collision_suffix(std::move(collision_suffix_p)), collision_priority(collision_priority_p) {
	}
};

// Typed header rule - maps header names to specific types with direction constraints
struct TypedHeaderRule {
	string header_name;       // Header name (lowercase normalized, e.g., "content-length")
	LogicalTypeId type;       // Override type (e.g., BIGINT, INTEGER)
	bool applies_to_request;  // Applies to %i (request headers)
	bool applies_to_response; // Applies to %o (response headers)

	TypedHeaderRule(string header_p, LogicalTypeId type_p, bool request_p, bool response_p)
	    : header_name(std::move(header_p)), type(type_p), applies_to_request(request_p),
	      applies_to_response(response_p) {
	}

	// Check if this rule applies to the given directive
	bool AppliesTo(const string &directive) const {
		if (directive == "%i") {
			return applies_to_request;
		}
		if (directive == "%o") {
			return applies_to_response;
		}
		return false;
	}
};

// Represents a single field in the log format
struct FormatField {
	string directive;   // The format directive (e.g., "%h", "%t", "%{Referer}i")
	string column_name; // The corresponding column name (e.g., "client_ip", "timestamp")
	LogicalType type;   // The data type for this field
	bool is_quoted;     // Whether this field appears in quotes in the log format
	string modifier;    // Optional modifier (e.g., "Referer" in "%{Referer}i")
	bool should_skip;   // Whether this field should be skipped (used for %b/%B merging)

	// %r sub-column skip flags: when individual directives (%m, %U, %q, %H) override %r
	bool skip_method;       // Skip method column from %r (when %m is present)
	bool skip_path;         // Skip path column from %r (when %U is present)
	bool skip_query_string; // Skip query_string column from %r (when %q is present)
	bool skip_protocol;     // Skip protocol column from %r (when %H is present)

	FormatField(string directive_p, string column_name_p, LogicalType type_p, bool is_quoted_p = false,
	            string modifier_p = "", bool should_skip_p = false)
	    : directive(std::move(directive_p)), column_name(std::move(column_name_p)), type(std::move(type_p)),
	      is_quoted(is_quoted_p), modifier(std::move(modifier_p)), should_skip(should_skip_p),
	      skip_method(false), skip_path(false), skip_query_string(false), skip_protocol(false) {
	}
};

// Parsed format string information
struct ParsedFormat {
	vector<FormatField> fields;                 // List of fields in the format
	string original_format_str;                 // Original format string
	string regex_pattern;                       // Generated regex pattern for parsing
	unique_ptr<duckdb_re2::RE2> compiled_regex; // Pre-compiled RE2 for performance

	// Reusable buffers for RE2::FullMatchN to eliminate per-line heap allocations
	// These buffers are allocated once in ParseFormatString() and reused across
	// all ParseLogLine() calls, reducing 36-40M allocations to just 3.
	//
	// THREAD SAFETY: Marked mutable because buffers are physically modified but
	// ParseLogLine() is logically const (doesn't change format definition).
	// Currently safe because MaxThreads() = 1 (single-threaded execution).
	//
	// FUTURE PARALLELIZATION: Before enabling multi-threading, these buffers
	// MUST be moved to thread-local state (LocalTableFunctionState) to avoid
	// data races. See implementation plan for detailed migration strategy.
	//
	// MOVE WARNING: ParsedFormat must not be moved after initialization because
	// arg_ptrs contains pointers to elements of args. Currently safe because
	// ParsedFormat is stored in BindData and never moved.
	mutable vector<duckdb_re2::StringPiece> matches;
	mutable vector<duckdb_re2::RE2::Arg> args;
	mutable vector<duckdb_re2::RE2::Arg *> arg_ptrs;

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
	static LogicalType GetDataType(const string &directive, const string &modifier = "");

	// Generate a regex pattern from the format string
	static string GenerateRegexPattern(const ParsedFormat &parsed_format);

	// Generate DuckDB schema (column names and types) from parsed format
	// Adds standard columns: filename, and optionally parse_error/raw_line if include_raw_columns=true
	static void GenerateSchema(const ParsedFormat &parsed_format, vector<string> &names,
	                           vector<LogicalType> &return_types, bool include_raw_columns = true);

	// Parse a log line using the parsed format
	// Returns a vector of string values corresponding to the fields in parsed_format
	// Returns empty vector if parsing fails
	static vector<string> ParseLogLine(const string &line, const ParsedFormat &parsed_format);

	// Helper to parse timestamp from Apache log format
	static bool ParseTimestamp(const string &timestamp_str, timestamp_t &result);

	// Helper to parse request line into method, path, query_string, protocol
	static bool ParseRequest(const string &request, string &method, string &path, string &query_string,
	                         string &protocol);

private:
	// Unified directive definitions - combines column name, type, and collision rules
	static const std::vector<DirectiveDefinition> directive_definitions;

	// Typed header rules - maps header names to specific types with direction constraints
	static const std::vector<TypedHeaderRule> typed_header_rules;

	// Lookup caches for O(1) access (built on first use)
	static std::unordered_map<string, const DirectiveDefinition *> directive_cache;
	static std::unordered_map<string, const TypedHeaderRule *> header_cache;
	static bool cache_initialized;

	// Initialize lookup caches from directive_definitions and typed_header_rules
	static void InitializeCaches();

	// Get directive definition by directive string (O(1) lookup)
	static const DirectiveDefinition *GetDirectiveDefinition(const string &directive);

	// Get typed header type for a header name and directive (O(1) lookup)
	static LogicalTypeId GetTypedHeaderType(const string &header_name, const string &directive);

	// Resolve column name collisions using rule-based approach
	// Handles both different directives producing same name (e.g., %{X}i + %{X}o)
	// and duplicate same directives (e.g., %{X}i + %{X}i)
	static void ResolveColumnNameCollisions(ParsedFormat &parsed_format);
};

} // namespace duckdb
