#include "httpd_log_multi_file_info.hpp"
#include "httpd_log_file_reader.hpp"
#include "httpd_log_buffered_reader.hpp"
#include "httpd_conf_reader.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include <algorithm>

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
	if (loption == "conf") {
		options.conf = StringValue::Get(value);
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
	bind_data->conf = std::move(options.conf);
	bind_data->raw_mode = options.raw_mode;

	return std::move(bind_data);
}

void HttpdLogMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                       MultiFileBindData &bind_data) {
	auto &httpd_data = bind_data.bind_data->Cast<HttpdLogBindData>();

	// Helper lambda to read sample lines from log files
	auto get_sample_lines = [&]() -> vector<string> {
		auto expanded_files = bind_data.file_list->GetAllFiles();
		if (expanded_files.empty()) {
			throw BinderException("No files found for httpd log reading");
		}
		vector<string> sample_lines;
		for (const auto &file_info : expanded_files) {
			auto lines = ReadSampleLines(context, file_info.path, 10);
			sample_lines.insert(sample_lines.end(), lines.begin(), lines.end());
			if (sample_lines.size() >= 10) {
				break;
			}
		}
		return sample_lines;
	};

	// Helper lambda to try parsing with a format and return match count
	auto try_format = [](const vector<string> &sample_lines, const ParsedFormat &parsed) -> int {
		int matches = 0;
		for (const auto &line : sample_lines) {
			if (line.empty()) {
				continue;
			}
			auto values = HttpdLogFormatParser::ParseLogLine(line, parsed);
			if (!values.empty()) {
				matches++;
			}
		}
		return matches;
	};

	// 1. format_str specified - use it directly (highest priority, ignore conf)
	if (!httpd_data.format_str.empty()) {
		httpd_data.parsed_format = HttpdLogFormatParser::ParseFormatString(httpd_data.format_str);
		if (httpd_data.format_type.empty()) {
			httpd_data.format_type = "custom";
		}

	} else if (!httpd_data.conf.empty()) {
		// 2b. conf specified (format_str is empty)
		auto &fs = FileSystem::GetFileSystem(context);
		auto entries = HttpdConfReader::ParseConfigFile(httpd_data.conf, fs);

		// Sort by line_number to ensure consistent order
		std::sort(entries.begin(), entries.end(),
		          [](const HttpdConfReader::ConfigEntry &a, const HttpdConfReader::ConfigEntry &b) {
			          return a.line_number < b.line_number;
		          });

		// Get sample lines for format detection
		auto sample_lines = get_sample_lines();

		bool found = false;

		if (!httpd_data.format_type.empty()) {
			// format_type specified: search for nickname=format_type with format_type="named"
			for (const auto &entry : entries) {
				if (entry.format_type == "named" && entry.nickname == httpd_data.format_type &&
				    !entry.format_string.empty()) {
					auto parsed = HttpdLogFormatParser::ParseFormatString(entry.format_string);
					int matches = try_format(sample_lines, parsed);
					if (matches > 0 && matches >= static_cast<int>(sample_lines.size()) / 2) {
						httpd_data.parsed_format = std::move(parsed);
						httpd_data.format_str = entry.format_string;
						// format_type already set
						found = true;
						break;
					}
				}
			}
			if (!found) {
				throw BinderException("Format '%s' in conf file '%s' not found or does not match the log file format",
				                      httpd_data.format_type, httpd_data.conf);
			}
		} else {
			// format_type not specified: try default -> inline -> named order
			vector<string> try_order = {"default", "inline", "named"};

			for (const auto &type : try_order) {
				for (const auto &entry : entries) {
					if (entry.format_type == type && !entry.format_string.empty()) {
						auto parsed = HttpdLogFormatParser::ParseFormatString(entry.format_string);
						int matches = try_format(sample_lines, parsed);
						if (matches > 0 && matches >= static_cast<int>(sample_lines.size()) / 2) {
							httpd_data.parsed_format = std::move(parsed);
							httpd_data.format_str = entry.format_string;
							httpd_data.format_type = entry.nickname.empty() ? type : entry.nickname;
							found = true;
							break;
						}
					}
				}
				if (found) {
					break;
				}
			}
			if (!found) {
				throw BinderException("No matching format found in conf file '%s' for the log file", httpd_data.conf);
			}
		}

	} else if (!httpd_data.format_type.empty()) {
		// 2a. conf not specified, format_type specified - use built-in definitions
		if (httpd_data.format_type == "common") {
			httpd_data.format_str = "%h %l %u %t \"%r\" %>s %b";
		} else if (httpd_data.format_type == "combined") {
			httpd_data.format_str = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"";
		} else {
			throw BinderException("Invalid format_type '%s'. Supported formats: 'common', 'combined'. "
			                      "Or use format_str for custom formats, or conf for httpd.conf lookup.",
			                      httpd_data.format_type);
		}
		httpd_data.parsed_format = HttpdLogFormatParser::ParseFormatString(httpd_data.format_str);

	} else {
		// 2a. conf not specified, format_type not specified - auto-detect from file content
		auto sample_lines = get_sample_lines();

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

	// Generate schema from parsed format
	HttpdLogFormatParser::GenerateSchema(httpd_data.parsed_format, names, return_types, httpd_data.raw_mode);

	// Let MultiFileReader handle options like filename, hive partitioning, etc.
	bind_data.multi_file_reader->BindOptions(bind_data.file_options, *bind_data.file_list, return_types, names,
	                                         bind_data.reader_bind);
}

optional_idx HttpdLogMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                               const MultiFileGlobalState &global_state,
                                               FileExpandResult expand_result) {
	// Thread-safety: Now safe for parallel file reading because:
	// 1. scan_initialized/finished flags use std::atomic<bool> with compare_exchange
	// 2. RE2 parsing buffers are in HttpdLogLocalState (thread-local per thread)
	// 3. Each HttpdLogFileReader has its own buffered_reader instance
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		// Multiple files: allow parallel processing (one thread per file)
		return optional_idx();
	}
	// Single file: no intra-file parallelism (unlike Parquet row groups)
	return 1;
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
	// Return thread-local state with RE2 parsing buffers
	return make_uniq<HttpdLogLocalState>();
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
