#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include <memory>
#include <vector>

namespace duckdb {

class HttpdLogTableFunction {
public:
	// Register the read_httpd_log table function
	static void RegisterFunction(ExtensionLoader &loader);

private:
	// Bind data for the table function
	struct BindData : public TableFunctionData {
		vector<string> files;
		string format_type;
		explicit BindData(vector<string> files_p, string format_type_p)
			: files(std::move(files_p)), format_type(std::move(format_type_p)) {}
	};

	// Global state for reading files
	struct GlobalState : public GlobalTableFunctionState {
		idx_t current_file_idx;
		unique_ptr<FileHandle> file_handle;
		string current_filename;
		bool finished;

		GlobalState() : current_file_idx(0), finished(false) {}

		idx_t MaxThreads() const override {
			return 1; // Single-threaded for now
		}
	};

	// Table function operations
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	static void Function(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

} // namespace duckdb
