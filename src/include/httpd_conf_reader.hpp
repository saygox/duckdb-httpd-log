#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional_idx.hpp"
#include <memory>
#include <vector>

namespace duckdb {

class HttpdConfReader {
public:
	// Register the read_httpd_conf table function
	static void RegisterFunction(ExtensionLoader &loader);

	// Parsed config entry
	struct ConfigEntry {
		string log_type;      // "access" or "error"
		string format_type;   // "named", "default", "inline", "reference"
		string nickname;      // Nickname (nullable)
		string format_string; // Format string (nullable)
		string config_file;   // Source config file
		idx_t line_number;    // Line number in config file
	};

	// Parse a single config file and return all entries
	static vector<ConfigEntry> ParseConfigFile(const string &path, FileSystem &fs);

private:
	// Bind data for the table function
	struct BindData : public TableFunctionData {
		vector<ConfigEntry> entries; // All parsed entries from all files
	};

	// Global state for iterating through entries
	struct GlobalState : public GlobalTableFunctionState {
		idx_t current_idx = 0;

		idx_t MaxThreads() const override {
			return 1;
		}
	};

	// Table function operations
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	static void Function(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Parse a directive line and return true if valid entry was parsed
	static bool ParseDirectiveLine(const string &line, const string &directive, const string &config_file,
	                               idx_t line_number, ConfigEntry &out_entry);

	// Tokenize Apache config line (handles quoted strings and escapes)
	static vector<string> TokenizeLine(const string &line);
};

} // namespace duckdb
