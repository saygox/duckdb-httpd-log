#include "httpd_log_table_function.hpp"
#include "httpd_log_parser.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
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
	HttpdLogFormatParser::GenerateSchema(parsed_format, names, return_types);

	return make_uniq<BindData>(files, actual_format_type, format_str, std::move(parsed_format));
}

unique_ptr<GlobalTableFunctionState> HttpdLogTableFunction::Init(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<BindData>();
	auto state = make_uniq<GlobalState>();

	if (!bind_data.files.empty()) {
		state->current_file_idx = 0;
		state->current_filename = bind_data.files[0];

		// Open first file
		auto &fs = FileSystem::GetFileSystem(context);
		state->file_handle = fs.OpenFile(state->current_filename, FileFlags::FILE_FLAGS_READ);
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

		if (state.file_handle) {
			// Read until newline
			std::stringstream ss;
			char c;
			bool found_newline = false;

			while (state.file_handle->Read(&c, 1) == 1) {
				if (c == '\n') {
					found_newline = true;
					break;
				}
				ss << c;
			}

			if (found_newline || ss.tellp() > 0) {
				line = ss.str();
				has_line = true;

				// Remove trailing \r if present
				if (!line.empty() && line.back() == '\r') {
					line.pop_back();
				}
			}
		}

		// If no line read, move to next file
		if (!has_line) {
			state.file_handle.reset();
			state.current_file_idx++;

			if (state.current_file_idx >= bind_data.files.size()) {
				state.finished = true;
				break;
			}

			state.current_filename = bind_data.files[state.current_file_idx];
			state.file_handle = fs.OpenFile(state.current_filename, FileFlags::FILE_FLAGS_READ);
			continue;
		}

		// Skip empty lines
		if (line.empty()) {
			continue;
		}

		// Parse the line using dynamic parser
		vector<string> parsed_values = HttpdLogFormatParser::ParseLogLine(line, bind_data.parsed_format);
		bool parse_error = parsed_values.empty();

		// Fill output chunk dynamically based on parsed format
		idx_t col_idx = 0;
		idx_t value_idx = 0;

		for (const auto &field : bind_data.parsed_format.fields) {
			if (parse_error) {
				// Set all columns to NULL or empty on parse error
				if (field.directive == "%t") {
					// timestamp column
					FlatVector::SetNull(output.data[col_idx], output_idx, true);
					col_idx++;
					// timestamp_raw column
					FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
					    StringVector::AddString(output.data[col_idx], "");
					col_idx++;
				} else if (field.directive == "%r") {
					// method, path, protocol columns
					FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
					    StringVector::AddString(output.data[col_idx], "");
					col_idx++;
					FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
					    StringVector::AddString(output.data[col_idx], "");
					col_idx++;
					FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
					    StringVector::AddString(output.data[col_idx], "");
					col_idx++;
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
					// timestamp_raw
					FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
					    StringVector::AddString(output.data[col_idx], value);
					col_idx++;
				} else if (field.directive == "%r") {
					// Parse request line into method, path, protocol
					string method, path, protocol;
					if (HttpdLogFormatParser::ParseRequest(value, method, path, protocol)) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], method);
						col_idx++;
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], path);
						col_idx++;
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], protocol);
						col_idx++;
					} else {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], "");
						col_idx++;
					}
				} else {
					// Regular field - convert based on type
					if (field.type.id() == LogicalTypeId::VARCHAR) {
						FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
						    StringVector::AddString(output.data[col_idx], value);
					} else if (field.type.id() == LogicalTypeId::INTEGER) {
						try {
							int32_t int_val = std::stoi(value);
							FlatVector::GetData<int32_t>(output.data[col_idx])[output_idx] = int_val;
						} catch (...) {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
					} else if (field.type.id() == LogicalTypeId::BIGINT) {
						try {
							// Handle "-" for %b directive (no bytes)
							if (value == "-") {
								FlatVector::SetNull(output.data[col_idx], output_idx, true);
							} else {
								int64_t int_val = std::stoll(value);
								FlatVector::GetData<int64_t>(output.data[col_idx])[output_idx] = int_val;
							}
						} catch (...) {
							FlatVector::SetNull(output.data[col_idx], output_idx, true);
						}
					}
					col_idx++;
				}
			}
		}

		// Add metadata columns at the end
		// filename
		FlatVector::GetData<string_t>(output.data[col_idx])[output_idx] =
		    StringVector::AddString(output.data[col_idx], state.current_filename);
		col_idx++;

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

		output_idx++;
	}

	output.SetCardinality(output_idx);
}

void HttpdLogTableFunction::RegisterFunction(ExtensionLoader &loader) {
	// Create table function with optional format_type and format_str parameters
	TableFunction read_httpd_log("read_httpd_log", {LogicalType::VARCHAR}, Function, Bind, Init);
	read_httpd_log.named_parameters["format_type"] = LogicalType::VARCHAR;
	read_httpd_log.named_parameters["format_str"] = LogicalType::VARCHAR;

	// Register the function
	loader.RegisterFunction(read_httpd_log);
}

} // namespace duckdb
