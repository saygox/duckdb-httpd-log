#pragma once

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "httpd_log_format_parser.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// HttpdLogFileReaderOptions - Options for the reader
//===--------------------------------------------------------------------===//
class HttpdLogFileReaderOptions : public BaseFileReaderOptions {
public:
	string format_type;
	string format_str;
	bool raw_mode = false;
};

//===--------------------------------------------------------------------===//
// HttpdLogBindData - Bind data for the table function
//===--------------------------------------------------------------------===//
struct HttpdLogBindData : public TableFunctionData {
	string format_type;
	string format_str;
	ParsedFormat parsed_format;
	bool raw_mode = false;
};

//===--------------------------------------------------------------------===//
// HttpdLogGlobalState - Global state (column_ids stored here like read_file)
//===--------------------------------------------------------------------===//
struct HttpdLogGlobalState : public GlobalTableFunctionState {
	vector<idx_t> column_ids;
};

//===--------------------------------------------------------------------===//
// HttpdLogMultiFileInfo - MultiFileReaderInterface implementation
//===--------------------------------------------------------------------===//
struct HttpdLogMultiFileInfo : MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext &context);

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;

	bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;

	bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<string> &expected_names,
	                     vector<LogicalType> &expected_types) override;

	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;

	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                MultiFileBindData &bind_data) override;

	optional_idx MaxThreads(const MultiFileBindData &bind_data_p, const MultiFileGlobalState &global_state,
	                        FileExpandResult expand_result) override;

	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data,
	                                                           MultiFileGlobalState &global_state) override;

	unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &context,
	                                                         GlobalTableFunctionState &gstate) override;

	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        BaseUnionData &union_data, const MultiFileBindData &bind_data_p) override;

	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        const OpenFileInfo &file, idx_t file_idx,
	                                        const MultiFileBindData &bind_data) override;

	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                        BaseFileReaderOptions &options,
	                                        const MultiFileOptions &file_options) override;

	unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) override;
};

} // namespace duckdb
