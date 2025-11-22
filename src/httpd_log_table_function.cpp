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
	if (input.inputs.size() != 1) {
		throw BinderException("read_httpd_log requires exactly one argument: the file path or glob pattern");
	}

	if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_httpd_log argument must be a string (file path or glob pattern)");
	}

	string path_pattern = input.inputs[0].GetValue<string>();

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

	// Define output schema (13 columns as per requirements)
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

	return make_uniq<BindData>(files);
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

		// Parse the line
		HttpdLogEntry entry = HttpdLogParser::ParseLine(line);

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

		// Column 10: filename
		FlatVector::GetData<string_t>(output.data[10])[output_idx] = StringVector::AddString(output.data[10], state.current_filename);

		// Column 11: parse_error
		FlatVector::GetData<bool>(output.data[11])[output_idx] = entry.parse_error;

		// Column 12: raw_line (only set if parse_error is true)
		if (entry.parse_error) {
			FlatVector::GetData<string_t>(output.data[12])[output_idx] = StringVector::AddString(output.data[12], entry.raw_line);
		} else {
			FlatVector::SetNull(output.data[12], output_idx, true);
		}

		output_idx++;
	}

	output.SetCardinality(output_idx);
}

void HttpdLogTableFunction::RegisterFunction(ExtensionLoader &loader) {
	// Create table function
	TableFunction read_httpd_log("read_httpd_log", {LogicalType::VARCHAR}, Function, Bind, Init);

	// Register the function
	loader.RegisterFunction(read_httpd_log);
}

} // namespace duckdb
