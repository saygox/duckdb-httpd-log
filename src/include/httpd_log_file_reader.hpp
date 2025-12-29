#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "httpd_log_format_parser.hpp"
#include "httpd_log_buffered_reader.hpp"

namespace duckdb {

// Forward declaration
struct HttpdLogBindData;
struct HttpdLogGlobalState;

//===--------------------------------------------------------------------===//
// HttpdLogFileReader - BaseFileReader implementation (like DirectFileReader)
//===--------------------------------------------------------------------===//
class HttpdLogFileReader : public BaseFileReader {
public:
	HttpdLogFileReader(ClientContext &context, OpenFileInfo file_p, const HttpdLogBindData &bind_data);

public:
	//! The bind data (contains parsed format)
	const HttpdLogBindData &bind_data;

	//! Buffered reader for the file
	unique_ptr<HttpdLogBufferedReader> buffered_reader;

	//! Whether scan has been initialized (TryInitializeScan returned true)
	bool scan_initialized = false;

	//! Whether we have finished reading this file
	bool finished = false;

public:
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;

	void Scan(ClientContext &context, GlobalTableFunctionState &global_state, LocalTableFunctionState &local_state,
	          DataChunk &chunk) override;

	string GetReaderType() const override {
		return "HTTPD_LOG";
	}

private:
	//! Write a column value based on schema column ID
	void WriteColumnValue(Vector &vec, idx_t row_idx, idx_t schema_col_id, const vector<string> &parsed_values,
	                      const string &line, bool parse_error);

	//! Write a regular field value (non-special columns)
	void WriteRegularFieldValue(Vector &vec, idx_t row_idx, const FormatField &field, const string &value);
};

} // namespace duckdb
