#include "httpd_log_parser.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/interval.hpp"
#include <sstream>
#include <regex>
#include <ctime>

namespace duckdb {

std::string HttpdLogParser::Trim(const std::string &str) {
	size_t start = str.find_first_not_of(" \t\r\n");
	if (start == std::string::npos) {
		return "";
	}
	size_t end = str.find_last_not_of(" \t\r\n");
	return str.substr(start, end - start + 1);
}

bool HttpdLogParser::ParseTimestamp(const std::string &timestamp_str, timestamp_t &result) {
	// Format: 10/Oct/2000:13:55:36 -0700
	// We need to parse this into a timestamp_t

	static const char *month_names[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
	                                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

	std::regex timestamp_regex(R"((\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})\s*([-+]\d{4}))");
	std::smatch match;

	if (!std::regex_match(timestamp_str, match, timestamp_regex)) {
		return false;
	}

	int day = std::stoi(match[1].str());
	std::string month_str = match[2].str();
	int year = std::stoi(match[3].str());
	int hour = std::stoi(match[4].str());
	int minute = std::stoi(match[5].str());
	int second = std::stoi(match[6].str());
	std::string tz_str = match[7].str();

	// Convert month name to number
	int month = 0;
	for (int i = 0; i < 12; i++) {
		if (month_str == month_names[i]) {
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
	// Convert to epoch microseconds, adjust, then convert back
	int64_t epoch_us = Timestamp::GetEpochMicroSeconds(ts);
	epoch_us -= tz_offset_seconds * Interval::MICROS_PER_SEC;
	result = Timestamp::FromEpochMicroSeconds(epoch_us);

	return true;
}

bool HttpdLogParser::ParseRequest(const std::string &request, std::string &method, std::string &path,
                                  std::string &protocol) {
	// Request format: "GET /index.html HTTP/1.0"
	std::istringstream iss(request);

	if (!(iss >> method >> path >> protocol)) {
		return false;
	}

	return true;
}

HttpdLogEntry HttpdLogParser::ParseLine(const std::string &line) {
	HttpdLogEntry entry;
	entry.raw_line = line;

	// Use regex to parse Common Log Format
	// Format: %h %l %u %t "%r" %>s %b
	// Example: 192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326

	// Note: Using regular string literal instead of raw string literal to avoid quote issues
	std::regex log_regex("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([^\\]]+)\\]\\s+\"([^\"]*)\"\\s+(\\S+)\\s+(\\S+)");

	std::smatch match;
	if (!std::regex_match(line, match, log_regex)) {
		entry.parse_error = true;
		return entry;
	}

	// Extract fields
	entry.client_ip = match[1].str();
	entry.ident = match[2].str();
	entry.auth_user = match[3].str();
	entry.timestamp_raw = match[4].str();
	std::string request = match[5].str();
	std::string status_str = match[6].str();
	std::string bytes_str = match[7].str();

	// Parse timestamp
	entry.has_timestamp = ParseTimestamp(entry.timestamp_raw, entry.timestamp);

	// Parse request line
	if (!ParseRequest(request, entry.method, entry.path, entry.protocol)) {
		// Request parsing failed, keep the raw values empty
	}

	// Parse status code
	try {
		if (status_str != "-") {
			entry.status = std::stoi(status_str);
			entry.has_status = true;
		}
	} catch (...) {
		// Status parsing failed
	}

	// Parse bytes
	try {
		if (bytes_str != "-") {
			entry.bytes = std::stoll(bytes_str);
			entry.has_bytes = true;
		}
	} catch (...) {
		// Bytes parsing failed
	}

	return entry;
}

std::string HttpdLogParser::ExtractQuotedField(const std::string &line, size_t &pos) {
	// Skip leading whitespace
	while (pos < line.length() && (line[pos] == ' ' || line[pos] == '\t')) {
		pos++;
	}

	if (pos >= line.length() || line[pos] != '"') {
		return "";
	}

	pos++; // Skip opening quote
	size_t start = pos;

	// Find closing quote
	while (pos < line.length() && line[pos] != '"') {
		if (line[pos] == '\\' && pos + 1 < line.length()) {
			pos += 2; // Skip escaped character
		} else {
			pos++;
		}
	}

	if (pos >= line.length()) {
		return ""; // No closing quote found
	}

	std::string result = line.substr(start, pos - start);
	pos++; // Skip closing quote
	return result;
}

HttpdLogEntry HttpdLogParser::ParseCombinedLine(const std::string &line) {
	HttpdLogEntry entry;
	entry.raw_line = line;

	// Use regex to parse Combined Log Format
	// Format: %h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-agent}i"
	// Example: 192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326
	// "http://www.example.com/" "Mozilla/5.0"

	std::regex log_regex("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([^\\]]+)\\]\\s+\"([^\"]*)\"\\s+(\\S+)\\s+(\\S+)\\s+\"([^"
	                     "\"]*)\"\\s+\"([^\"]*)\"");

	std::smatch match;
	if (!std::regex_match(line, match, log_regex)) {
		entry.parse_error = true;
		return entry;
	}

	// Extract common fields
	entry.client_ip = match[1].str();
	entry.ident = match[2].str();
	entry.auth_user = match[3].str();
	entry.timestamp_raw = match[4].str();
	std::string request = match[5].str();
	std::string status_str = match[6].str();
	std::string bytes_str = match[7].str();
	entry.referer = match[8].str();
	entry.user_agent = match[9].str();

	// Parse timestamp
	entry.has_timestamp = ParseTimestamp(entry.timestamp_raw, entry.timestamp);

	// Parse request line
	if (!ParseRequest(request, entry.method, entry.path, entry.protocol)) {
		// Request parsing failed, keep the raw values empty
	}

	// Parse status code
	try {
		if (status_str != "-") {
			entry.status = std::stoi(status_str);
			entry.has_status = true;
		}
	} catch (...) {
		// Status parsing failed
	}

	// Parse bytes
	try {
		if (bytes_str != "-") {
			entry.bytes = std::stoll(bytes_str);
			entry.has_bytes = true;
		}
	} catch (...) {
		// Bytes parsing failed
	}

	return entry;
}

} // namespace duckdb
