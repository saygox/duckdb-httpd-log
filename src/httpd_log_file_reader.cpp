#include "httpd_log_file_reader.hpp"
#include "httpd_log_multi_file_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/main/client_context.hpp"
#include <sstream>
#include <unordered_set>

namespace duckdb {

// Parse a strftime-formatted timestamp string into timestamp_t
static bool ParseStrftimeTimestamp(const string &value, const string &format, timestamp_t &result,
                                   int &parsed_tz_offset_seconds) {
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

			if (spec == '-' && fmt_pos + 2 < format.length()) {
				spec = format[fmt_pos + 2];
				fmt_pos += 3;
			} else {
				fmt_pos += 2;
			}

			switch (spec) {
			case 'Y':
				if (val_pos + 4 <= value.length()) {
					year = std::stoi(value.substr(val_pos, 4));
					val_pos += 4;
				}
				break;
			case 'y':
				if (val_pos + 2 <= value.length()) {
					year = std::stoi(value.substr(val_pos, 2));
					year += (year >= 70) ? 1900 : 2000;
					val_pos += 2;
				}
				break;
			case 'm':
				if (val_pos + 2 <= value.length()) {
					month = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'd':
				if (val_pos + 2 <= value.length()) {
					day = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'e':
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
			case 'h': {
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
			case 'H':
				if (val_pos + 2 <= value.length()) {
					hour = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'I':
				if (val_pos + 2 <= value.length()) {
					hour = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'M':
				if (val_pos + 2 <= value.length()) {
					minute = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'S':
				if (val_pos + 2 <= value.length()) {
					second = std::stoi(value.substr(val_pos, 2));
					val_pos += 2;
				}
				break;
			case 'T':
				if (val_pos + 8 <= value.length()) {
					hour = std::stoi(value.substr(val_pos, 2));
					minute = std::stoi(value.substr(val_pos + 3, 2));
					second = std::stoi(value.substr(val_pos + 6, 2));
					val_pos += 8;
				}
				break;
			case 'z':
				if (val_pos + 5 <= value.length()) {
					int tz_sign = (value[val_pos] == '-') ? -1 : 1;
					int tz_hours = std::stoi(value.substr(val_pos + 1, 2));
					int tz_minutes = std::stoi(value.substr(val_pos + 3, 2));
					tz_offset = tz_sign * (tz_hours * 3600 + tz_minutes * 60);
					has_timezone = true;
					val_pos += 5;
				}
				break;
			case 'Z':
				while (val_pos < value.length() && value[val_pos] != ' ') {
					val_pos++;
				}
				break;
			case '%':
				if (value[val_pos] == '%') {
					val_pos++;
				}
				break;
			default:
				break;
			}
		} else {
			if (format[fmt_pos] == value[val_pos]) {
				fmt_pos++;
				val_pos++;
			} else {
				return false;
			}
		}
	}

	if (year == 0 || month == 0 || day == 0) {
		return false;
	}

	try {
		date_t date = Date::FromDate(year, month, day);
		dtime_t time = Time::FromTime(hour, minute, second, 0);
		result = Timestamp::FromDatetime(date, time);

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

static bool CombineTimestampGroup(const ParsedFormat &parsed_format, const TimestampGroup &group,
                                  const vector<string> &parsed_values, idx_t &value_idx, timestamp_t &result,
                                  string &raw_combined) {
	int64_t base_epoch_us = 0;
	int64_t frac_us = 0;
	int tz_offset_seconds = 0;
	bool has_base = false;
	bool has_tz = false;
	string raw_parts;

	string combined_strftime_value;
	string combined_strftime_format;
	bool has_strftime_components = false;

	for (idx_t i = 0; i < group.field_indices.size(); i++) {
		idx_t field_idx = group.field_indices[i];
		const auto &field = parsed_format.fields[field_idx];
		const string &value = parsed_values[value_idx + i];

		if (i > 0) {
			raw_parts += " ";
		}
		raw_parts += value;

		switch (field.timestamp_type) {
		case TimestampFormatType::APACHE_DEFAULT: {
			timestamp_t ts;
			if (HttpdLogFormatParser::ParseTimestamp(value, ts)) {
				base_epoch_us = Timestamp::GetEpochMicroSeconds(ts);
				has_base = true;
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

	if (has_strftime_components && !has_base) {
		timestamp_t ts;
		int parsed_tz;
		if (ParseStrftimeTimestamp(combined_strftime_value, combined_strftime_format, ts, parsed_tz)) {
			base_epoch_us = Timestamp::GetEpochMicroSeconds(ts);
			has_base = true;
		} else {
			if (combined_strftime_format == "%z") {
				int tz;
				if (ParseTimezoneOffset(combined_strftime_value, tz)) {
					tz_offset_seconds = tz;
					has_tz = true;
				}
			}
		}
	}

	value_idx += group.field_indices.size();
	raw_combined = raw_parts;

	if (has_base) {
		int64_t final_epoch_us = base_epoch_us + frac_us;
		if (has_tz) {
			final_epoch_us -= tz_offset_seconds * Interval::MICROS_PER_SEC;
		}
		result = Timestamp::FromEpochMicroSeconds(final_epoch_us);
		return true;
	}

	return false;
}

HttpdLogFileReader::HttpdLogFileReader(ClientContext &context, OpenFileInfo file_p, const HttpdLogBindData &bind_data_p)
    : BaseFileReader(std::move(file_p)), bind_data(bind_data_p) {
	// Initialize the buffered reader
	auto &fs = FileSystem::GetFileSystem(context);
	buffered_reader = make_uniq<HttpdLogBufferedReader>(fs, file.path);

	// Populate the columns vector (required for MultiFileReader schema matching)
	// This mirrors the logic in HttpdLogFormatParser::GenerateSchema
	const auto &parsed_format = bind_data.parsed_format;
	bool raw_mode = bind_data.raw_mode;

	for (const auto &field : parsed_format.fields) {
		if (field.should_skip) {
			continue;
		}

		if (field.directive == "%t") {
			columns.emplace_back("timestamp", LogicalType::TIMESTAMP);
			if (raw_mode) {
				columns.emplace_back("timestamp_raw", LogicalType::VARCHAR);
			}
		} else if (field.directive == "%r" || field.directive == "%>r" || field.directive == "%<r") {
			if (!field.skip_method) {
				columns.emplace_back("method", LogicalType::VARCHAR);
			}
			if (!field.skip_path) {
				columns.emplace_back("path", LogicalType::VARCHAR);
			}
			if (!field.skip_query_string) {
				columns.emplace_back("query_string", LogicalType::VARCHAR);
			}
			if (!field.skip_protocol) {
				columns.emplace_back("protocol", LogicalType::VARCHAR);
			}
		} else {
			columns.emplace_back(field.column_name, field.type);
		}
	}

	// Add standard metadata columns
	columns.emplace_back("log_file", LogicalType::VARCHAR);

	if (raw_mode) {
		columns.emplace_back("parse_error", LogicalType::BOOLEAN);
		columns.emplace_back("raw_line", LogicalType::VARCHAR);
	}
}

bool HttpdLogFileReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
                                           LocalTableFunctionState &lstate) {
	// httpd_log has no intra-file parallelism (unlike Parquet row groups)
	// TryInitializeScan should return true only once per file
	if (scan_initialized || finished) {
		return false;
	}
	scan_initialized = true;
	return true;
}

void HttpdLogFileReader::Scan(ClientContext &context, GlobalTableFunctionState &global_state,
                              LocalTableFunctionState &local_state, DataChunk &output) {
	if (finished) {
		return;
	}

	idx_t output_idx = 0;
	constexpr idx_t BATCH_SIZE = STANDARD_VECTOR_SIZE;
	const auto &parsed_format = bind_data.parsed_format;
	bool raw_mode = bind_data.raw_mode;

	// Use column_ids from BaseFileReader (set by MultiFileColumnMapper)
	auto &local_column_ids = column_ids;

	while (output_idx < BATCH_SIZE && !finished) {
		string line;
		bool has_line = buffered_reader->ReadLine(line);

		if (!has_line) {
			finished = true;
			break;
		}

		if (line.empty()) {
			continue;
		}

		// Parse the line
		vector<string> parsed_values = HttpdLogFormatParser::ParseLogLine(line, parsed_format);
		bool parse_error = parsed_values.empty();

		// Skip error rows when raw_mode is false
		if (parse_error && !raw_mode) {
			continue;
		}

		// Fill output chunk based on column_ids (projection pushdown)
		for (idx_t col_out_idx = 0; col_out_idx < local_column_ids.size(); col_out_idx++) {
			auto local_idx = MultiFileLocalIndex(col_out_idx);
			auto local_id = local_column_ids[local_idx];
			idx_t schema_col_id = local_id.GetId();

			// Write the column value to output.data[col_out_idx]
			WriteColumnValue(output.data[col_out_idx], output_idx, schema_col_id, parsed_values, line, parse_error);
		}

		output_idx++;
	}

	output.SetCardinality(output_idx);
}

void HttpdLogFileReader::WriteColumnValue(Vector &vec, idx_t row_idx, idx_t schema_col_id,
                                          const vector<string> &parsed_values, const string &line, bool parse_error) {
	const auto &parsed_format = bind_data.parsed_format;
	bool raw_mode = bind_data.raw_mode;

	// Build a mapping from schema column ID to field/sub-column
	// This needs to iterate through fields to find the right one
	idx_t current_schema_col = 0;
	idx_t value_idx = 0;
	std::unordered_set<int> processed_ts_groups;

	for (idx_t field_idx = 0; field_idx < parsed_format.fields.size(); field_idx++) {
		const auto &field = parsed_format.fields[field_idx];

		if (field.should_skip) {
			if (field.directive != "%t") {
				value_idx++;
			}
			continue;
		}

		if (field.directive == "%t") {
			int group_id = field.timestamp_group_id;

			if (group_id >= 0 && processed_ts_groups.count(group_id) == 0) {
				processed_ts_groups.insert(group_id);
				const auto &group = parsed_format.timestamp_groups[group_id];

				// timestamp column
				if (current_schema_col == schema_col_id) {
					if (parse_error) {
						FlatVector::SetNull(vec, row_idx, true);
					} else {
						idx_t temp_value_idx = value_idx;
						timestamp_t ts;
						string raw_combined;
						if (CombineTimestampGroup(parsed_format, group, parsed_values, temp_value_idx, ts,
						                          raw_combined)) {
							FlatVector::GetData<timestamp_t>(vec)[row_idx] = ts;
						} else {
							FlatVector::SetNull(vec, row_idx, true);
						}
					}
					return;
				}
				current_schema_col++;

				// timestamp_raw column (only in raw mode)
				if (raw_mode) {
					if (current_schema_col == schema_col_id) {
						if (parse_error) {
							FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, "");
						} else {
							idx_t temp_value_idx = value_idx;
							timestamp_t ts;
							string raw_combined;
							CombineTimestampGroup(parsed_format, group, parsed_values, temp_value_idx, ts,
							                      raw_combined);
							FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, raw_combined);
						}
						return;
					}
					current_schema_col++;
				}

				value_idx += group.field_indices.size();
			} else if (group_id < 0) {
				// Single %t not in a group
				if (current_schema_col == schema_col_id) {
					if (parse_error) {
						FlatVector::SetNull(vec, row_idx, true);
					} else {
						const string &value = parsed_values[value_idx];
						timestamp_t ts;
						if (HttpdLogFormatParser::ParseTimestamp(value, ts)) {
							FlatVector::GetData<timestamp_t>(vec)[row_idx] = ts;
						} else {
							FlatVector::SetNull(vec, row_idx, true);
						}
					}
					return;
				}
				current_schema_col++;

				if (raw_mode) {
					if (current_schema_col == schema_col_id) {
						if (parse_error) {
							FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, "");
						} else {
							FlatVector::GetData<string_t>(vec)[row_idx] =
							    StringVector::AddString(vec, parsed_values[value_idx]);
						}
						return;
					}
					current_schema_col++;
				}

				value_idx++;
			}
		} else if (field.directive == "%r" || field.directive == "%>r" || field.directive == "%<r") {
			string method, path, query_string, protocol;
			bool parsed = false;
			if (!parse_error) {
				parsed =
				    HttpdLogFormatParser::ParseRequest(parsed_values[value_idx], method, path, query_string, protocol);
			}

			if (!field.skip_method) {
				if (current_schema_col == schema_col_id) {
					if (parse_error || !parsed) {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, "");
					} else {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, method);
					}
					return;
				}
				current_schema_col++;
			}
			if (!field.skip_path) {
				if (current_schema_col == schema_col_id) {
					if (parse_error || !parsed) {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, "");
					} else {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, path);
					}
					return;
				}
				current_schema_col++;
			}
			if (!field.skip_query_string) {
				if (current_schema_col == schema_col_id) {
					if (parse_error || !parsed || query_string.empty()) {
						FlatVector::SetNull(vec, row_idx, true);
					} else {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, query_string);
					}
					return;
				}
				current_schema_col++;
			}
			if (!field.skip_protocol) {
				if (current_schema_col == schema_col_id) {
					if (parse_error || !parsed) {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, "");
					} else {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, protocol);
					}
					return;
				}
				current_schema_col++;
			}
			value_idx++;
		} else {
			// Regular field
			if (current_schema_col == schema_col_id) {
				if (parse_error) {
					if (field.type.id() == LogicalTypeId::VARCHAR) {
						FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, "");
					} else {
						FlatVector::SetNull(vec, row_idx, true);
					}
				} else {
					const string &value = parsed_values[value_idx];
					WriteRegularFieldValue(vec, row_idx, field, value);
				}
				return;
			}
			current_schema_col++;
			value_idx++;
		}
	}

	// Special columns: log_file, parse_error, raw_line
	// log_file
	if (current_schema_col == schema_col_id) {
		FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, file.path);
		return;
	}
	current_schema_col++;

	if (raw_mode) {
		// parse_error
		if (current_schema_col == schema_col_id) {
			FlatVector::GetData<bool>(vec)[row_idx] = parse_error;
			return;
		}
		current_schema_col++;

		// raw_line
		if (current_schema_col == schema_col_id) {
			if (parse_error) {
				FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, line);
			} else {
				FlatVector::SetNull(vec, row_idx, true);
			}
			return;
		}
	}
}

void HttpdLogFileReader::WriteRegularFieldValue(Vector &vec, idx_t row_idx, const FormatField &field,
                                                const string &value) {
	if (field.type.id() == LogicalTypeId::VARCHAR) {
		if (field.directive == "%X") {
			// Connection status
			string status_str;
			if (value == "X") {
				status_str = "aborted";
			} else if (value == "+") {
				status_str = "keepalive";
			} else if (value == "-") {
				status_str = "close";
			} else {
				status_str = value;
			}
			FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, status_str);
		} else {
			if (value == "-") {
				FlatVector::SetNull(vec, row_idx, true);
			} else {
				FlatVector::GetData<string_t>(vec)[row_idx] = StringVector::AddString(vec, value);
			}
		}
	} else if (field.type.id() == LogicalTypeId::INTEGER) {
		try {
			if (value == "-") {
				FlatVector::SetNull(vec, row_idx, true);
			} else {
				int32_t int_val = std::stoi(value);
				FlatVector::GetData<int32_t>(vec)[row_idx] = int_val;
			}
		} catch (...) {
			FlatVector::SetNull(vec, row_idx, true);
		}
	} else if (field.type.id() == LogicalTypeId::BIGINT) {
		try {
			if (value == "-") {
				static const std::unordered_set<string> bytes_columns = {"bytes", "bytes_clf", "bytes_received",
				                                                         "bytes_sent", "bytes_transferred"};
				if (bytes_columns.count(field.column_name)) {
					FlatVector::GetData<int64_t>(vec)[row_idx] = 0;
				} else {
					FlatVector::SetNull(vec, row_idx, true);
				}
			} else {
				int64_t int_val = std::stoll(value);
				FlatVector::GetData<int64_t>(vec)[row_idx] = int_val;
			}
		} catch (...) {
			FlatVector::SetNull(vec, row_idx, true);
		}
	} else if (field.type.id() == LogicalTypeId::INTERVAL) {
		try {
			if (value == "-") {
				FlatVector::SetNull(vec, row_idx, true);
			} else {
				int64_t int_val = std::stoll(value);
				if (field.directive == "%T") {
					if (field.modifier == "ms") {
						int_val *= Interval::MICROS_PER_MSEC;
					} else if (field.modifier == "us") {
						// Already in microseconds
					} else {
						int_val *= Interval::MICROS_PER_SEC;
					}
				}
				FlatVector::GetData<interval_t>(vec)[row_idx] = Interval::FromMicro(int_val);
			}
		} catch (...) {
			FlatVector::SetNull(vec, row_idx, true);
		}
	}
}

} // namespace duckdb
