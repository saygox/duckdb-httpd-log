#include "httpd_log_table_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include <chrono>
#include <sstream>

namespace duckdb {

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

		for (const auto &field : bind_data.parsed_format.fields) {
			// Skip fields marked for skipping (e.g., %b when %B is present)
			if (field.should_skip) {
				continue;
			}

			if (parse_error) {
				// Set all columns to NULL or empty on parse error
				if (field.directive == "%t") {
					// timestamp column
					FlatVector::SetNull(output.data[col_idx], output_idx, true);
					col_idx++;
					// timestamp_raw column (only in raw mode)
					if (bind_data.raw_mode) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
					}
				} else if (field.directive == "%r") {
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
				const string &value = parsed_values[value_idx];
				value_idx++;

				if (field.directive == "%t") {
					// Parse timestamp
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
				} else if (field.directive == "%r") {
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
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], query_string);
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
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], "");
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
							FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
							    StringVector::AddString(output.data[col_idx], value);
						}
					} else if (field.type.id() == LogicalTypeId::INTEGER) {
						try {
							// Handle "-" as 0 for numeric fields
							if (value == "-") {
								FlatVector::GetData<int32_t>(output.data[col_idx])[output_idx] = 0;
							} else {
								int32_t int_val = std::stoi(value);
								FlatVector::GetData<int32_t>(output.data[col_idx])[output_idx] = int_val;
							}
						} catch (...) {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
					} else if (field.type.id() == LogicalTypeId::BIGINT) {
						try {
							// Handle "-" as 0 for numeric fields
							if (value == "-") {
								FlatVector::GetData<int64_t>(output.data[col_idx])[output_idx] = 0;
							} else {
								int64_t int_val = std::stoll(value);
								FlatVector::GetData<int64_t>(output.data[col_idx])[output_idx] = int_val;
							}
						} catch (...) {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
					} else if (field.type.id() == LogicalTypeId::INTERVAL) {
						try {
							// Handle "-" as 0 for duration fields
							if (value == "-") {
								FlatVector::GetData<interval_t>(output.data[col_idx])[output_idx] = {0, 0, 0};
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
