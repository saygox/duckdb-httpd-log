#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "httpd_log_format_parser.hpp"
#include "httpd_log_buffered_reader.hpp"
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
		string format_str;
		ParsedFormat parsed_format;
		bool raw_mode; // Whether to include parse_error/raw_line columns and error rows
		explicit BindData(vector<string> files_p, string format_type_p, string format_str_p,
		                  ParsedFormat parsed_format_p, bool raw_mode_p)
		    : files(std::move(files_p)), format_type(std::move(format_type_p)), format_str(std::move(format_str_p)),
		      parsed_format(std::move(parsed_format_p)), raw_mode(raw_mode_p) {
		}
	};

	// Global state for reading files
	struct GlobalState : public GlobalTableFunctionState {
		idx_t current_file_idx;
		unique_ptr<HttpdLogBufferedReader> buffered_reader;
		string current_filename;
		bool finished;

		// Profiling statistics
		idx_t total_rows = 0;      // Total number of rows read
		idx_t bytes_scanned = 0;   // Total bytes scanned
		idx_t files_processed = 0; // Number of files processed
		idx_t parse_errors = 0;    // Number of parse errors

		// Profiling statistics - timing breakdown
		double time_file_io = 0.0; // Time spent in file I/O
		double time_regex = 0.0;   // Time spent in regex matching
		double time_parsing = 0.0; // Time spent in parsing/conversion
		idx_t buffer_refills = 0;  // Number of buffer refills

		GlobalState() : current_file_idx(0), finished(false) {
		}

		idx_t MaxThreads() const override {
			return 1; // Single-threaded for now
		}
	};

	// Table function operations
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	static void Function(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Profiling: return dynamic statistics
	static InsertionOrderPreservingMap<string> DynamicToString(TableFunctionDynamicToStringInput &input);
};

} // namespace duckdb
