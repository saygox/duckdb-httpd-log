#include "httpd_log_table_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include <chrono>
#include <sstream>
#include <unordered_set>

namespace duckdb {

// Parse a strftime-formatted timestamp string into timestamp_t
// Returns true on success, false on failure
static bool ParseStrftimeTimestamp(const string &value, const string &format, timestamp_t &result,
                                   int &parsed_tz_offset_seconds) {
	// Parse the timestamp according to strftime format
	// This is a simplified parser that handles common formats

	std::istringstream iss(value);
	int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
	int tz_offset = 0;
	bool has_timezone = false;
	parsed_tz_offset_seconds = 0;

	size_t val_pos = 0;
	size_t fmt_pos = 0;

	while (fmt_pos < format.length() && val_pos < value.length()) {
		if (format[fmt_pos] == '%' && fmt_pos + 1 < format.length()) {
			char spec = format[fmt_pos + 1];

			// Handle %- prefix for non-padded
			if (spec == '-' && fmt_pos + 2 < format.length()) {
				spec = format[fmt_pos + 2];
				fmt_pos += 3;
			} else {
				fmt_pos += 2;
			}

			switch (spec) {
			case 'Y': // 4-digit year
				if (val_pos + 4 <= value.length()) {
					year = std::stoi(value.substr(val_pos, 4));
					val_pos += 4;
				}
				break;
			case 'y': // 2-digit year
				if (val_pos + 2 <= value.length()) {
					year = std::stoi(value.substr(val_pos, 2));
					year += (year >= 70) ? 1900 : 2000;
					val_pos += 2;
				}
				break;
			case 'm': // Month 01-12
				if (val_pos + 2 <= value.length()) {
					month = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'd': // Day 01-31
				if (val_pos + 2 <= value.length()) {
					day = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'e': // Day with space padding
				// Skip leading space if present
				if (value[val_pos] == ' ') {
					val_pos++;
				}
				if (val_pos + 1 <= value.length()) {
					day = std::stoi(
					    value.substr(val_pos, value[val_pos + 1] >= '0' && value[val_pos + 1] <= '9' ? 2 : 1));
					val_pos += (value[val_pos + 1] >= '0' && value[val_pos + 1] <= '9') ? 2 : 1;
				}
				break;
			case 'b':
			case 'h': { // Abbreviated month name
				static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
				                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
				for (int i = 0; i < 12; i++) {
					if (value.substr(val_pos, 3) == months[i]) {
						month = i + 1;
						val_pos += 3;
						break;
					}
				}
				break;
			}
			case 'H': // Hour 00-23
				if (val_pos + 2 <= value.length()) {
					hour = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'I': // Hour 01-12
				if (val_pos + 2 <= value.length()) {
					hour = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'M': // Minute 00-59
				if (val_pos + 2 <= value.length()) {
					minute = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'S': // Second 00-59
				if (val_pos + 2 <= value.length()) {
					second = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'T': // %H:%M:%S
				if (val_pos + 8 <= value.length()) {
					hour = std::stoi(value.substr(val_pos, 2));
					minute = std::stoi(value.substr(val_pos + 3, 2));
					second = std::stoi(value.substr(val_pos + 6, 2));
					val_pos += 8;
				}
				break;
			case 'z': // Timezone offset +HHMM
				if (val_pos + 5 <= value.length()) {
					int tz_sign = (value[val_pos] == '-') ? -1 : 1;
					int tz_hours = std::stoi(value.substr(val_pos + 1, 2));
					int tz_minutes = std::stoi(value.substr(val_pos + 3, 2));
					tz_offset = tz_sign * (tz_hours * 3600 + tz_minutes * 60);
					has_timezone = true;
					val_pos += 5;
				}
				break;
			case 'Z': // Timezone name - skip
				while (val_pos < value.length() && value[val_pos] != ' ') {
					val_pos++;
				}
				break;
			case '%': // Literal %
				if (value[val_pos] == '%') {
					val_pos++;
				}
				break;
			default:
				// Unknown specifier, skip chars in value until we find something
				break;
			}
		} else {
			// Literal character - must match
			if (format[fmt_pos] == value[val_pos]) {
				fmt_pos++;
				val_pos++;
			} else {
				// Mismatch
				return false;
			}
		}
	}

	// Validate parsed values
	if (year == 0 || month == 0 || day == 0) {
		return false;
	}

	try {
		date_t date = Date::FromDate(year, month, day);
		dtime_t time = Time::FromTime(hour, minute, second, 0);
		result = Timestamp::FromDatetime(date, time);

		// Adjust for timezone if present
		if (has_timezone) {
			int64_t epoch_us = Timestamp::GetEpochMicroSeconds(result);
			epoch_us -= tz_offset * Interval::MICROS_PER_SEC;
			result = Timestamp::FromEpochMicroSeconds(epoch_us);
		}

		parsed_tz_offset_seconds = tz_offset;
		return true;
	} catch (...) {
		return false;
	}
}

// Parse timezone offset from a strftime %z format value (e.g., "-0700", "+0000")
static bool ParseTimezoneOffset(const string &value, int &tz_offset_seconds) {
	if (value.length() != 5) {
		return false;
	}
	if (value[0] != '+' && value[0] != '-') {
		return false;
	}
	try {
		int sign = (value[0] == '-') ? -1 : 1;
		int hours = std::stoi(value.substr(1, 2));
		int minutes = std::stoi(value.substr(3, 2));
		tz_offset_seconds = sign * (hours * 3600 + minutes * 60);
		return true;
	} catch (...) {
		return false;
	}
}

// Combine multiple timestamp components from a timestamp group into a single timestamp
static bool CombineTimestampGroup(const ParsedFormat &parsed_format, const TimestampGroup &group,
                                  const vector<string> &parsed_values, idx_t &value_idx, timestamp_t &result,
                                  string &raw_combined) {
	int64_t base_epoch_us = 0;
	int64_t frac_us = 0;
	int tz_offset_seconds = 0;
	bool has_base = false;
	bool has_tz = false;
	string raw_parts;

	// First pass: collect all strftime components to combine them
	string combined_strftime_value;
	string combined_strftime_format;
	bool has_strftime_components = false;

	for (idx_t i = 0; i < group.field_indices.size(); i++) {
		idx_t field_idx = group.field_indices[i];
		const auto &field = parsed_format.fields[field_idx];
		const string &value = parsed_values[value_idx + i];

		if (i > 0) {
			raw_parts += " "; // Separator for raw concatenation
		}
		raw_parts += value;

		switch (field.timestamp_type) {
		case TimestampFormatType::APACHE_DEFAULT: {
			// Parse standard Apache format (includes timezone)
			timestamp_t ts;
			if (HttpdLogFormatParser::ParseTimestamp(value, ts)) {
				base_epoch_us = Timestamp::GetEpochMicroSeconds(ts);
				has_base = true;
				// Note: ParseTimestamp already converts to UTC
			}
			break;
		}
		case TimestampFormatType::EPOCH_SEC: {
			try {
				int64_t val = std::stoll(value);
				base_epoch_us = val * Interval::MICROS_PER_SEC;
				has_base = true;
			} catch (...) {
			}
			break;
		}
		case TimestampFormatType::EPOCH_MSEC: {
			try {
				int64_t val = std::stoll(value);
				base_epoch_us = val * Interval::MICROS_PER_MSEC;
				has_base = true;
			} catch (...) {
			}
			break;
		}
		case TimestampFormatType::EPOCH_USEC: {
			try {
				base_epoch_us = std::stoll(value);
				has_base = true;
			} catch (...) {
			}
			break;
		}
		case TimestampFormatType::FRAC_MSEC: {
			try {
				int64_t val = std::stoll(value);
				frac_us = val * Interval::MICROS_PER_MSEC;
			} catch (...) {
			}
			break;
		}
		case TimestampFormatType::FRAC_USEC: {
			try {
				frac_us = std::stoll(value);
			} catch (...) {
			}
			break;
		}
		case TimestampFormatType::STRFTIME: {
			// Collect strftime components for combined parsing
			if (has_strftime_components) {
				combined_strftime_value += " ";
				combined_strftime_format += " ";
			}
			combined_strftime_value += value;
			combined_strftime_format += field.strftime_format;
			has_strftime_components = true;
			break;
		}
		}
	}

	// Parse combined strftime components if any
	if (has_strftime_components && !has_base) {
		timestamp_t ts;
		int parsed_tz;
		if (ParseStrftimeTimestamp(combined_strftime_value, combined_strftime_format, ts, parsed_tz)) {
			base_epoch_us = Timestamp::GetEpochMicroSeconds(ts);
			has_base = true;
			// Note: ParseStrftimeTimestamp already applies timezone to the result,
			// so we don't set has_tz here to avoid double application
		} else {
			// If combined parse failed, check if we only have %z
			if (combined_strftime_format == "%z") {
				int tz;
				if (ParseTimezoneOffset(combined_strftime_value, tz)) {
					tz_offset_seconds = tz;
					has_tz = true;
				}
			}
		}
	}

	// Advance value_idx past all values in this group
	value_idx += group.field_indices.size();
	raw_combined = raw_parts;

	if (has_base) {
		// Apply timezone offset if we have one and base timestamp doesn't already include it
		int64_t final_epoch_us = base_epoch_us + frac_us;
		if (has_tz) {
			// Subtract timezone offset to convert to UTC
			final_epoch_us -= tz_offset_seconds * Interval::MICROS_PER_SEC;
		}
		result = Timestamp::FromEpochMicroSeconds(final_epoch_us);
		return true;
	}

	return false;
}

unique_ptr<FunctionData> HttpdLogTableFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	// Check arguments
	if (input.inputs.size() < 1 || input.inputs.size() > 2) {
		throw BinderException("read_httpd_log requires 1 or 2 arguments: file path/glob pattern and optional "
		                      "format_type (default: 'common')");
	}

	if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_httpd_log first argument must be a string (file path or glob pattern)");
	}

	string path_pattern = input.inputs[0].GetValue<string>();

	// Get format_type parameter (default to 'common')
	string format_type = "common";
	bool format_type_specified = false;
	// Check for named parameter first
	auto format_param = input.named_parameters.find("format_type");
	if (format_param != input.named_parameters.end()) {
		if (format_param->second.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("read_httpd_log format_type parameter must be a string");
		}
		format_type = format_param->second.GetValue<string>();
		format_type_specified = true;
	} else if (input.inputs.size() >= 2) {
		// Fall back to positional parameter
		if (input.inputs[1].type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("read_httpd_log second argument (format_type) must be a string");
		}
		format_type = input.inputs[1].GetValue<string>();
		format_type_specified = true;
	}

	// Get format_str parameter (optional, for custom format strings)
	string format_str = "";
	bool format_str_specified = false;
	auto format_str_param = input.named_parameters.find("format_str");
	if (format_str_param != input.named_parameters.end()) {
		if (format_str_param->second.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("read_httpd_log format_str parameter must be a string");
		}
		format_str = format_str_param->second.GetValue<string>();
		format_str_specified = true;
	} else if (input.inputs.size() >= 3) {
		// Fall back to positional parameter (third argument)
		if (input.inputs[2].type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("read_httpd_log third argument (format_str) must be a string");
		}
		format_str = input.inputs[2].GetValue<string>();
		format_str_specified = true;
	}

	// Convert format_type to format_str if format_str is not specified
	// This makes format_str the primary parameter
	if (!format_str_specified) {
		// Map format_type to corresponding LogFormat string
		if (format_type == "common") {
			format_str = "%h %l %u %t \"%r\" %>s %b";
		} else if (format_type == "combined") {
			format_str = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"";
		} else {
			throw BinderException("Invalid format_type '%s'. Supported formats: 'common', 'combined'. Or use "
			                      "format_str for custom formats.",
			                      format_type);
		}
	}
	// If both are specified, format_str takes precedence (format_type is ignored)

	// Process 'raw' parameter (default: false)
	bool raw_mode = false;
	auto raw_param = input.named_parameters.find("raw");
	if (raw_param != input.named_parameters.end()) {
		if (raw_param->second.type().id() != LogicalTypeId::BOOLEAN) {
			throw BinderException("raw parameter must be a BOOLEAN");
		}
		raw_mode = BooleanValue::Get(raw_param->second);
	}

	// Parse the format string to extract field definitions
	ParsedFormat parsed_format = HttpdLogFormatParser::ParseFormatString(format_str);

	// Determine the actual format type from format_str
	// This allows format_str to be the primary parameter
	string actual_format_type;
	if (format_str == "%h %l %u %t \"%r\" %>s %b") {
		actual_format_type = "common";
	} else if (format_str == "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"") {
		actual_format_type = "combined";
	} else {
		// Custom format string
		actual_format_type = "custom";
	}

	// Expand glob pattern to get list of files
	auto &fs = FileSystem::GetFileSystem(context);
	// Use nullptr for FileOpener - the FileSystem will use defaults
	auto file_info_list = fs.Glob(path_pattern, nullptr);

	// Extract file paths from OpenFileInfo
	vector<string> files;
	for (auto &file_info : file_info_list) {
		files.push_back(file_info.path);
	}

	if (files.empty()) {
		throw BinderException("No files found matching pattern: %s", path_pattern);
	}

	// Generate schema dynamically from parsed format
	HttpdLogFormatParser::GenerateSchema(parsed_format, names, return_types, raw_mode);

	return make_uniq<BindData>(files, actual_format_type, format_str, std::move(parsed_format), raw_mode);
}

unique_ptr<GlobalTableFunctionState> HttpdLogTableFunction::Init(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<BindData>();
	auto state = make_uniq<GlobalState>();

	if (!bind_data.files.empty()) {
		state->current_file_idx = 0;
		state->current_file = bind_data.files[0];

		// Open first file with buffered reader
		auto &fs = FileSystem::GetFileSystem(context);
		state->buffered_reader = make_uniq<HttpdLogBufferedReader>(fs, state->current_file);
	} else {
		state->finished = true;
	}

	return std::move(state);
}

void HttpdLogTableFunction::Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<BindData>();
	auto &state = data.global_state->Cast<GlobalState>();

	if (state.finished) {
		return;
	}

	idx_t output_idx = 0;
	constexpr idx_t BATCH_SIZE = STANDARD_VECTOR_SIZE;

	auto &fs = FileSystem::GetFileSystem(context);

	// Read lines from current file and parse them
	while (output_idx < BATCH_SIZE) {
		// Read a line from the current file
		string line;
		bool has_line = false;

		if (state.buffered_reader) {
			auto start = std::chrono::high_resolution_clock::now();
			has_line = state.buffered_reader->ReadLine(line);
			auto end = std::chrono::high_resolution_clock::now();
			state.time_file_io += std::chrono::duration<double>(end - start).count();
		}

		// If no line read, move to next file
		if (!has_line) {
			state.buffered_reader.reset();
			state.current_file_idx++;

			// Profiling: increment files processed counter
			state.files_processed++;

			if (state.current_file_idx >= bind_data.files.size()) {
				state.finished = true;
				break;
			}

			state.current_file = bind_data.files[state.current_file_idx];
			state.buffered_reader = make_uniq<HttpdLogBufferedReader>(fs, state.current_file);
			continue;
		}

		// Skip empty lines
		if (line.empty()) {
			continue;
		}

		// Profiling: count bytes scanned (line size + newline character)
		state.bytes_scanned += line.size() + 1;

		// Parse the line using dynamic parser
		auto start_regex = std::chrono::high_resolution_clock::now();
		vector<string> parsed_values = HttpdLogFormatParser::ParseLogLine(line, bind_data.parsed_format);
		auto end_regex = std::chrono::high_resolution_clock::now();
		state.time_regex += std::chrono::duration<double>(end_regex - start_regex).count();
		bool parse_error = parsed_values.empty();

		// Profiling: count parse errors and total rows
		if (parse_error) {
			state.parse_errors++;
		}
		state.total_rows++;

		// Skip error rows when raw_mode is false
		if (parse_error && !bind_data.raw_mode) {
			continue;
		}

		// Fill output chunk dynamically based on parsed format
		auto start_parse = std::chrono::high_resolution_clock::now();
		idx_t col_idx = 0;
		idx_t value_idx = 0;

		// Track which timestamp groups have been processed
		std::unordered_set<int> processed_ts_groups;

		for (idx_t field_idx = 0; field_idx < bind_data.parsed_format.fields.size(); field_idx++) {
			const auto &field = bind_data.parsed_format.fields[field_idx];

			// Skip fields marked for skipping (e.g., %b when %B is present, or secondary %t in a group)
			if (field.should_skip) {
				// For %t directives that are part of a timestamp group:
				// - The group leader will advance value_idx for all fields in the group
				// - Secondary %t fields (should_skip=true) don't advance value_idx here
				// For non-%t skipped fields: advance value_idx
				if (field.directive != "%t") {
					value_idx++;
				}
				// Note: %t with should_skip does NOT advance value_idx here because
				// the group leader already advanced it for all fields in the group
				continue;
			}

			if (parse_error) {
				// Set all columns to NULL or empty on parse error
				if (field.directive == "%t") {
					// Handle timestamp group
					int group_id = field.timestamp_group_id;
					if (group_id >= 0 && processed_ts_groups.count(group_id) == 0) {
						processed_ts_groups.insert(group_id);
						// Skip all values in this group
						const auto &group = bind_data.parsed_format.timestamp_groups[group_id];
						value_idx += group.field_indices.size();
					} else if (group_id < 0) {
						// Single %t not in a group
						value_idx++;
					}
					// timestamp column
					FlatVector::SetNull(output.data[col_idx], output_idx, true);
					col_idx++;
					// timestamp_raw column (only in raw mode)
					if (bind_data.raw_mode) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
					}
				} else if (field.directive == "%r" || field.directive == "%>r" || field.directive == "%<r") {
					value_idx++; // Advance past the request value
					// method, path, query_string, protocol columns (respecting skip flags)
					if (!field.skip_method) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
					}
					if (!field.skip_path) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
					}
					if (!field.skip_query_string) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
					}
					if (!field.skip_protocol) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
					}
				} else {
					value_idx++; // Advance past this value
					// Regular field
					if (field.type.id() == LogicalTypeId::VARCHAR) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
					} else {
						FlatVector::SetNull(output.data[col_idx], output_idx, true);
					}
					col_idx++;
				}
			} else {
				// Process parsed value based on field type
				if (field.directive == "%t") {
					// Handle timestamp - check if part of a group
					int group_id = field.timestamp_group_id;

					if (group_id >= 0 && processed_ts_groups.count(group_id) == 0) {
						// First field in a timestamp group - process entire group
						processed_ts_groups.insert(group_id);
						const auto &group = bind_data.parsed_format.timestamp_groups[group_id];

						timestamp_t ts;
						string raw_combined;
						if (CombineTimestampGroup(bind_data.parsed_format, group, parsed_values, value_idx, ts,
						                          raw_combined)) {
							FlatVector::GetData<timestamp_t>(output.data[col_idx])[output_idx] = ts;
						} else {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
						col_idx++;

						// timestamp_raw (only in raw mode)
						if (bind_data.raw_mode) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], raw_combined);
							col_idx++;
						}
					} else if (group_id < 0) {
						// Single %t not in a group - use original logic
						const string &value = parsed_values[value_idx];
						value_idx++;

						timestamp_t ts;
						if (HttpdLogFormatParser::ParseTimestamp(value, ts)) {
							FlatVector::GetData<timestamp_t>(output.data[col_idx])[output_idx] = ts;
						} else {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
						col_idx++;

						// timestamp_raw (only in raw mode)
						if (bind_data.raw_mode) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], value);
							col_idx++;
						}
					}
					// If group_id >= 0 but already processed, skip (handled by should_skip)
				} else if (field.directive == "%r" || field.directive == "%>r" || field.directive == "%<r") {
					const string &value = parsed_values[value_idx];
					value_idx++;
					// Parse request line into method, path, query_string, protocol
					// Skip sub-columns that are overridden by individual directives
					string method, path, query_string, protocol;
					if (HttpdLogFormatParser::ParseRequest(value, method, path, query_string, protocol)) {
						if (!field.skip_method) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], method);
							col_idx++;
						}
						if (!field.skip_path) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], path);
							col_idx++;
						}
						if (!field.skip_query_string) {
							// Empty query_string becomes NULL (no query parameters)
							if (query_string.empty()) {
								FlatVector::SetNull(output.data[col_idx], output_idx, true);
							} else {
								FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
								    StringVector::AddString(output.data[col_idx], query_string);
							}
							col_idx++;
						}
						if (!field.skip_protocol) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], protocol);
							col_idx++;
						}
					} else {
						if (!field.skip_method) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], "");
							col_idx++;
						}
						if (!field.skip_path) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], "");
							col_idx++;
						}
						if (!field.skip_query_string) {
							// Parse failure: query_string is NULL
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
							col_idx++;
						}
						if (!field.skip_protocol) {
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], "");
							col_idx++;
						}
					}
				} else {
					// Regular field - convert based on type
					const string &value = parsed_values[value_idx];
					value_idx++;

					if (field.type.id() == LogicalTypeId::VARCHAR) {
						// Special handling for connection status (%X)
						if (field.directive == "%X") {
							string status_str;
							if (value == "X") {
								status_str = "aborted";
							} else if (value == "+") {
								status_str = "keepalive";
							} else if (value == "-") {
								status_str = "close";
							} else {
								status_str = value; // Unknown value, keep as-is
							}
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], status_str);
						} else {
							// Convert "-" to NULL for VARCHAR columns (CLF convention for "no data")
							if (value == "-") {
								FlatVector::SetNull(output.data[col_idx], output_idx, true);
							} else {
								FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
								    StringVector::AddString(output.data[col_idx], value);
							}
						}
					} else if (field.type.id() == LogicalTypeId::INTEGER) {
						try {
							// Handle "-" as NULL for INTEGER fields
							if (value == "-") {
								FlatVector::SetNull(output.data[col_idx], output_idx, true);
							} else {
								int32_t int_val = std::stoi(value);
								FlatVector::GetData<int32_t>(output.data[col_idx])[output_idx] = int_val;
							}
						} catch (...) {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
					} else if (field.type.id() == LogicalTypeId::BIGINT) {
						try {
							// Handle "-": bytes columns get 0, others get NULL
							if (value == "-") {
								static const std::unordered_set<string> bytes_columns = {
								    "bytes", "bytes_clf", "bytes_received", "bytes_sent", "bytes_transferred"};
								if (bytes_columns.count(field.column_name)) {
									FlatVector::GetData<int64_t>(output.data[col_idx])[output_idx] = 0;
								} else {
									FlatVector::SetNull(output.data[col_idx], output_idx, true);
								}
							} else {
								int64_t int_val = std::stoll(value);
								FlatVector::GetData<int64_t>(output.data[col_idx])[output_idx] = int_val;
							}
						} catch (...) {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
					} else if (field.type.id() == LogicalTypeId::INTERVAL) {
						try {
							// Handle "-" as NULL for duration fields (status condition may cause "-")
							if (value == "-") {
								FlatVector::SetNull(output.data[col_idx], output_idx, true);
							} else {
								int64_t int_val = std::stoll(value);
								// Convert to microseconds based on directive/modifier:
								// %D = microseconds (no conversion needed)
								// %T = seconds
								// %{us}T = microseconds
								// %{ms}T = milliseconds
								// %{s}T = seconds
								if (field.directive == "%T") {
									if (field.modifier == "ms") {
										// Milliseconds to microseconds
										int_val *= Interval::MICROS_PER_MSEC;
									} else if (field.modifier == "us") {
										// Already in microseconds, no conversion
									} else {
										// %T or %{s}T: seconds to microseconds
										int_val *= Interval::MICROS_PER_SEC;
									}
								}
								// %D is already in microseconds
								// Convert microseconds to interval
								FlatVector::GetData<interval_t>(output.data[col_idx])[output_idx] =
								    Interval::FromMicro(int_val);
							}
						} catch (...) {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
					}
					col_idx++;
				}
			}
		}

		auto end_parse = std::chrono::high_resolution_clock::now();
		state.time_parsing += std::chrono::duration<double>(end_parse - start_parse).count();

		// Add metadata columns at the end
		// log_file (always included)
		FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
		    StringVector::AddString(output.data[col_idx], state.current_file);
		col_idx++;

		// parse_error and raw_line (only in raw mode)
		if (bind_data.raw_mode) {
			// parse_error
			FlatVector::GetData<bool>(output.data[col_idx])[output_idx] = parse_error;
			col_idx++;

			// raw_line (only set if parse_error is true)
			if (parse_error) {
				FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
				    StringVector::AddString(output.data[col_idx], line);
			} else {
				FlatVector::SetNull(output.data[col_idx], output_idx, true);
			}
		}

		output_idx++;
	}

	output.SetCardinality(output_idx);
}

InsertionOrderPreservingMap<string> HttpdLogTableFunction::DynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;

	if (!input.global_state) {
		return result;
	}

	auto &global_state = input.global_state->Cast<GlobalState>();

	// Add profiling statistics
	result["Total Rows"] = to_string(global_state.total_rows);
	result["Bytes Scanned"] = to_string(global_state.bytes_scanned);
	result["Files Processed"] = to_string(global_state.files_processed);

	if (global_state.parse_errors > 0) {
		result["Parse Errors"] = to_string(global_state.parse_errors);
	}

	// Add timing breakdown
	if (global_state.time_file_io > 0) {
		result["Time File I/O (s)"] = StringUtil::Format("%.6f", global_state.time_file_io);
	}
	if (global_state.time_regex > 0) {
		result["Time Regex (s)"] = StringUtil::Format("%.6f", global_state.time_regex);
	}
	if (global_state.time_parsing > 0) {
		result["Time Parsing (s)"] = StringUtil::Format("%.6f", global_state.time_parsing);
	}
	if (global_state.buffer_refills > 0) {
		result["Buffer Refills"] = to_string(global_state.buffer_refills);
	}

	return result;
}

void HttpdLogTableFunction::RegisterFunction(ExtensionLoader &loader) {
	// Create table function with optional format_type, format_str, and raw parameters
	TableFunction read_httpd_log("read_httpd_log", {LogicalType::VARCHAR}, Function, Bind, Init);
	read_httpd_log.named_parameters["format_type"] = LogicalType::VARCHAR;
	read_httpd_log.named_parameters["format_str"] = LogicalType::VARCHAR;
	read_httpd_log.named_parameters["raw"] = LogicalType::BOOLEAN;

	// Register profiling callback for EXPLAIN ANALYZE
	read_httpd_log.dynamic_to_string = DynamicToString;

	// Register the function
	loader.RegisterFunction(read_httpd_log);
}

} // namespace duckdb
