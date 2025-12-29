#include "httpd_log_multi_file_info.hpp"
#include "httpd_log_file_reader.hpp"
#include "httpd_log_buffered_reader.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

// Read sample lines from the first file for format auto-detection
static vector<string> ReadSampleLines(ClientContext &context, const string &file_path, idx_t max_lines = 10) {
	vector<string> sample_lines;
	auto &fs = FileSystem::GetFileSystem(context);

	try {
		HttpdLogBufferedReader reader(fs, file_path);
		string line;
		while (sample_lines.size() < max_lines && reader.ReadLine(line)) {
			if (!line.empty()) {
				sample_lines.push_back(std::move(line));
			}
		}
	} catch (...) {
		// If we can't read the file, return empty sample
	}

	return sample_lines;
}

unique_ptr<MultiFileReaderInterface> HttpdLogMultiFileInfo::CreateInterface(ClientContext &context) {
	return make_uniq<HttpdLogMultiFileInfo>();
}

unique_ptr<BaseFileReaderOptions> HttpdLogMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                           optional_ptr<TableFunctionInfo> info) {
	return make_uniq<HttpdLogFileReaderOptions>();
}

bool HttpdLogMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &value,
                                        MultiFileOptions &file_options, BaseFileReaderOptions &options_p) {
	auto &options = options_p.Cast<HttpdLogFileReaderOptions>();

	if (value.IsNull()) {
		throw BinderException("Cannot use NULL as argument to key %s", key);
	}

	auto loption = StringUtil::Lower(key);

	if (loption == "format_type") {
		options.format_type = StringValue::Get(value);
		return true;
	}
	if (loption == "format_str") {
		options.format_str = StringValue::Get(value);
		return true;
	}
	if (loption == "raw") {
		options.raw_mode = BooleanValue::Get(value);
		return true;
	}

	return false;
}

bool HttpdLogMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                                            BaseFileReaderOptions &options, vector<string> &expected_names,
                                            vector<LogicalType> &expected_types) {
	// COPY is not supported for httpd logs
	return false;
}

unique_ptr<TableFunctionData> HttpdLogMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                        unique_ptr<BaseFileReaderOptions> options_p) {
	auto &options = options_p->Cast<HttpdLogFileReaderOptions>();
	auto bind_data = make_uniq<HttpdLogBindData>();

	bind_data->format_type = std::move(options.format_type);
	bind_data->format_str = std::move(options.format_str);
	bind_data->raw_mode = options.raw_mode;

	return std::move(bind_data);
}

void HttpdLogMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                       MultiFileBindData &bind_data) {
	auto &httpd_data = bind_data.bind_data->Cast<HttpdLogBindData>();

	// Determine format string from format_type or format_str
	string format_str = httpd_data.format_str;
	bool format_specified = !format_str.empty() || !httpd_data.format_type.empty();

	if (format_str.empty()) {
		if (!httpd_data.format_type.empty()) {
			// format_type was explicitly specified
			if (httpd_data.format_type == "common") {
				format_str = "%h %l %u %t \"%r\" %>s %b";
			} else if (httpd_data.format_type == "combined") {
				format_str = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"";
			} else {
				throw BinderException("Invalid format_type '%s'. Supported formats: 'common', 'combined'. "
				                      "Or use format_str for custom formats.",
				                      httpd_data.format_type);
			}
			httpd_data.format_str = format_str;
			// Parse the format string
			httpd_data.parsed_format = HttpdLogFormatParser::ParseFormatString(format_str);
		} else {
			// Neither format_type nor format_str specified - auto-detect from file content
			// Get all files to sample
			auto expanded_files = bind_data.file_list->GetAllFiles();
			if (expanded_files.empty()) {
				throw BinderException("No files found for httpd log reading");
			}

			// Read sample lines from files until we have enough samples
			// Try each file until we get at least some sample lines
			vector<string> sample_lines;
			for (const auto &file_info : expanded_files) {
				auto lines = ReadSampleLines(context, file_info.path, 10);
				sample_lines.insert(sample_lines.end(), lines.begin(), lines.end());
				if (sample_lines.size() >= 10) {
					break; // We have enough samples
				}
			}

			// Auto-detect format
			string detected_format = HttpdLogFormatParser::DetectFormat(sample_lines, httpd_data.parsed_format);
			httpd_data.format_type = detected_format;

			if (detected_format == "common") {
				httpd_data.format_str = "%h %l %u %t \"%r\" %>s %b";
			} else if (detected_format == "combined") {
				httpd_data.format_str = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"";
			} else {
				// Unknown format - use raw mode with minimal schema
				httpd_data.format_type = "unknown";
				httpd_data.format_str = "";
				httpd_data.raw_mode = true; // Force raw mode for unknown format
			}
		}
	} else {
		// format_str was explicitly specified - parse it
		httpd_data.parsed_format = HttpdLogFormatParser::ParseFormatString(format_str);
	}

	// Generate schema from parsed format
	HttpdLogFormatParser::GenerateSchema(httpd_data.parsed_format, names, return_types, httpd_data.raw_mode);

	// Let MultiFileReader handle options like filename, hive partitioning, etc.
	bind_data.multi_file_reader->BindOptions(bind_data.file_options, *bind_data.file_list, return_types, names,
	                                         bind_data.reader_bind);
}

optional_idx HttpdLogMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                               const MultiFileGlobalState &global_state,
                                               FileExpandResult expand_result) {
	// httpd_log has no intra-file parallelism (line-based format)
	// Max threads = number of files (one file per thread)
	return bind_data_p.file_list->GetTotalFileCount();
}

unique_ptr<GlobalTableFunctionState> HttpdLogMultiFileInfo::InitializeGlobalState(ClientContext &context,
                                                                                  MultiFileBindData &bind_data,
                                                                                  MultiFileGlobalState &global_state) {
	auto result = make_uniq<HttpdLogGlobalState>();

	// Store column_ids for projection pushdown (like read_file pattern)
	for (idx_t i = 0; i < global_state.column_indexes.size(); i++) {
		result->column_ids.push_back(global_state.column_indexes[i].GetPrimaryIndex());
	}

	return std::move(result);
}

unique_ptr<LocalTableFunctionState> HttpdLogMultiFileInfo::InitializeLocalState(ExecutionContext &context,
                                                                                GlobalTableFunctionState &gstate) {
	return make_uniq<LocalTableFunctionState>();
}

shared_ptr<BaseFileReader> HttpdLogMultiFileInfo::CreateReader(ClientContext &context,
                                                               GlobalTableFunctionState &gstate_p,
                                                               BaseUnionData &union_data,
                                                               const MultiFileBindData &bind_data_p) {
	auto &httpd_data = bind_data_p.bind_data->Cast<HttpdLogBindData>();
	return make_shared_ptr<HttpdLogFileReader>(context, union_data.file, httpd_data);
}

shared_ptr<BaseFileReader> HttpdLogMultiFileInfo::CreateReader(ClientContext &context,
                                                               GlobalTableFunctionState &gstate_p,
                                                               const OpenFileInfo &file, idx_t file_idx,
                                                               const MultiFileBindData &bind_data) {
	auto &httpd_data = bind_data.bind_data->Cast<HttpdLogBindData>();
	return make_shared_ptr<HttpdLogFileReader>(context, file, httpd_data);
}

shared_ptr<BaseFileReader> HttpdLogMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                               BaseFileReaderOptions &options_p,
                                                               const MultiFileOptions &file_options) {
	throw NotImplementedException("HttpdLogMultiFileInfo::CreateReader with options not implemented");
}

unique_ptr<NodeStatistics> HttpdLogMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) {
	// Estimate average log file has ~10000 lines
	return make_uniq<NodeStatistics>(file_count * 10000);
}

} // namespace duckdb
