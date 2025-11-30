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
		throw BinderException("read_httpd_log requires 1 or 2 arguments: file path/glob pattern and optional format_type (default: 'common')");
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
			throw BinderException("Invalid format_type '%s'. Supported formats: 'common', 'combined'. Or use format_str for custom formats.", format_type);
		}
	}
	// If both are specified, format_str takes precedence (format_type is ignored)

	// Determine the actual format type from format_str
	// This allows format_str to be the primary parameter
	string actual_format_type;
	if (format_str == "%h %l %u %t \"%r\" %>s %b") {
		actual_format_type = "common";
	} else if (format_str == "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"") {
		actual_format_type = "combined";
	} else {
		// Custom format string - will be parsed in future implementation
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

	// Define output schema based on actual_format_type (derived from format_str)
	if (actual_format_type == "combined") {
		// Combined format: 15 columns (adds referer and user_agent)
		names = {"client_ip", "ident", "auth_user", "timestamp", "timestamp_raw",
		         "method", "path", "protocol", "status", "bytes",
		         "referer", "user_agent", "filename", "parse_error", "raw_line"};

		return_types = {
			LogicalType::VARCHAR,   // client_ip
			LogicalType::VARCHAR,   // ident
			LogicalType::VARCHAR,   // auth_user
			LogicalType::TIMESTAMP, // timestamp
			LogicalType::VARCHAR,   // timestamp_raw
			LogicalType::VARCHAR,   // method
			LogicalType::VARCHAR,   // path
			LogicalType::VARCHAR,   // protocol
			LogicalType::INTEGER,   // status
			LogicalType::BIGINT,    // bytes
			LogicalType::VARCHAR,   // referer
			LogicalType::VARCHAR,   // user_agent
			LogicalType::VARCHAR,   // filename
			LogicalType::BOOLEAN,   // parse_error
			LogicalType::VARCHAR    // raw_line
		};
	} else {
		// Common format: 13 columns
		names = {"client_ip", "ident", "auth_user", "timestamp", "timestamp_raw",
		         "method", "path", "protocol", "status", "bytes",
		         "filename", "parse_error", "raw_line"};

		return_types = {
			LogicalType::VARCHAR,   // client_ip
			LogicalType::VARCHAR,   // ident
			LogicalType::VARCHAR,   // auth_user
			LogicalType::TIMESTAMP, // timestamp
			LogicalType::VARCHAR,   // timestamp_raw
			LogicalType::VARCHAR,   // method
			LogicalType::VARCHAR,   // path
			LogicalType::VARCHAR,   // protocol
			LogicalType::INTEGER,   // status
			LogicalType::BIGINT,    // bytes
			LogicalType::VARCHAR,   // filename
			LogicalType::BOOLEAN,   // parse_error
			LogicalType::VARCHAR    // raw_line
		};
	}

	return make_uniq<BindData>(files, actual_format_type, format_str);
}

unique_ptr<GlobalTableFunctionState> HttpdLogTableFunction::Init(ClientContext &context, TableFunctionInitInput &input) {
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

		// Parse the line based on format_type
		HttpdLogEntry entry;
		if (bind_data.format_type == "combined") {
			entry = HttpdLogParser::ParseCombinedLine(line);
		} else {
			entry = HttpdLogParser::ParseLine(line);
		}

		// Fill output chunk
		// Column 0: client_ip
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[0])[output_idx] = StringVector::AddString(output.data[0], "");
		} else {
			FlatVector::GetData<string_t>(output.data[0])[output_idx] = StringVector::AddString(output.data[0], entry.client_ip);
		}

		// Column 1: ident
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[1])[output_idx] = StringVector::AddString(output.data[1], "");
		} else {
			FlatVector::GetData<string_t>(output.data[1])[output_idx] = StringVector::AddString(output.data[1], entry.ident);
		}

		// Column 2: auth_user
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[2])[output_idx] = StringVector::AddString(output.data[2], "");
		} else {
			FlatVector::GetData<string_t>(output.data[2])[output_idx] = StringVector::AddString(output.data[2], entry.auth_user);
		}

		// Column 3: timestamp
		if (entry.has_timestamp) {
			FlatVector::GetData<timestamp_t>(output.data[3])[output_idx] = entry.timestamp;
		} else {
			FlatVector::SetNull(output.data[3], output_idx, true);
		}

		// Column 4: timestamp_raw
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[4])[output_idx] = StringVector::AddString(output.data[4], "");
		} else {
			FlatVector::GetData<string_t>(output.data[4])[output_idx] = StringVector::AddString(output.data[4], entry.timestamp_raw);
		}

		// Column 5: method
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[5])[output_idx] = StringVector::AddString(output.data[5], "");
		} else {
			FlatVector::GetData<string_t>(output.data[5])[output_idx] = StringVector::AddString(output.data[5], entry.method);
		}

		// Column 6: path
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[6])[output_idx] = StringVector::AddString(output.data[6], "");
		} else {
			FlatVector::GetData<string_t>(output.data[6])[output_idx] = StringVector::AddString(output.data[6], entry.path);
		}

		// Column 7: protocol
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[7])[output_idx] = StringVector::AddString(output.data[7], "");
		} else {
			FlatVector::GetData<string_t>(output.data[7])[output_idx] = StringVector::AddString(output.data[7], entry.protocol);
		}

		// Column 8: status
		if (entry.has_status) {
			FlatVector::GetData<int32_t>(output.data[8])[output_idx] = entry.status;
		} else {
			FlatVector::SetNull(output.data[8], output_idx, true);
		}

		// Column 9: bytes
		if (entry.has_bytes) {
			FlatVector::GetData<int64_t>(output.data[9])[output_idx] = entry.bytes;
		} else {
			FlatVector::SetNull(output.data[9], output_idx, true);
		}

		// Combined format has 2 extra columns (referer, user_agent) before filename
		idx_t filename_col, parse_error_col, raw_line_col;
		if (bind_data.format_type == "combined") {
			// Column 10: referer
			if (entry.parse_error) {
				FlatVector::GetData<string_t>(output.data[10])[output_idx] = StringVector::AddString(output.data[10], "");
			} else {
				FlatVector::GetData<string_t>(output.data[10])[output_idx] = StringVector::AddString(output.data[10], entry.referer);
			}

			// Column 11: user_agent
			if (entry.parse_error) {
				FlatVector::GetData<string_t>(output.data[11])[output_idx] = StringVector::AddString(output.data[11], "");
			} else {
				FlatVector::GetData<string_t>(output.data[11])[output_idx] = StringVector::AddString(output.data[11], entry.user_agent);
			}

			filename_col = 12;
			parse_error_col = 13;
			raw_line_col = 14;
		} else {
			filename_col = 10;
			parse_error_col = 11;
			raw_line_col = 12;
		}

		// Column: filename
		FlatVector::GetData<string_t>(output.data[filename_col])[output_idx] = StringVector::AddString(output.data[filename_col], state.current_filename);

		// Column: parse_error
		FlatVector::GetData<bool>(output.data[parse_error_col])[output_idx] = entry.parse_error;

		// Column: raw_line (only set if parse_error is true)
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[raw_line_col])[output_idx] = StringVector::AddString(output.data[raw_line_col], entry.raw_line);
		} else {
			FlatVector::SetNull(output.data[raw_line_col], output_idx, true);
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
