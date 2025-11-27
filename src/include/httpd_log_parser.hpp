#pragma once

#include <string>
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

// Parsed result of Apache Common/Combined Log Format
struct HttpdLogEntry {
	std::string client_ip;
	std::string ident;
	std::string auth_user;
	timestamp_t timestamp;
	bool has_timestamp;
	std::string timestamp_raw;
	std::string method;
	std::string path;
	std::string protocol;
	int32_t status;
	bool has_status;
	int64_t bytes;
	bool has_bytes;
	std::string referer;      // Combined format only
	std::string user_agent;   // Combined format only
	bool parse_error;
	std::string raw_line;

	HttpdLogEntry() : has_timestamp(false), has_status(false), has_bytes(false), parse_error(false) {}
};

class HttpdLogParser {
public:
	// Parse a single line in Apache Common Log Format
	// Format: %h %l %u %t "%r" %>s %b
	// Example: 192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326
	static HttpdLogEntry ParseLine(const std::string &line);

	// Parse a single line in Apache Combined Log Format
	// Format: %h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-agent}i"
	// Example: 192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326 "http://www.example.com/" "Mozilla/5.0"
	static HttpdLogEntry ParseCombinedLine(const std::string &line);

private:
	// Helper functions
	static bool ParseTimestamp(const std::string &timestamp_str, timestamp_t &result);
	static bool ParseRequest(const std::string &request, std::string &method, std::string &path, std::string &protocol);
	static std::string Trim(const std::string &str);
	static std::string ExtractQuotedField(const std::string &line, size_t &pos);
};

} // namespace duckdb
