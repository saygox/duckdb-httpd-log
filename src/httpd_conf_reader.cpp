#include "httpd_conf_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "httpd_log_buffered_reader.hpp"

namespace duckdb {

// Tokenize Apache config line - handles quoted strings with escaped quotes
vector<string> HttpdConfReader::TokenizeLine(const string &line) {
	vector<string> tokens;
	string current_token;
	bool in_quotes = false;
	bool escape_next = false;

	for (size_t i = 0; i < line.size(); i++) {
		char c = line[i];

		if (escape_next) {
			current_token += c;
			escape_next = false;
			continue;
		}

		if (c == '\\') {
			escape_next = true;
			continue;
		}

		if (c == '"') {
			if (in_quotes) {
				// End of quoted string - save token
				tokens.push_back(current_token);
				current_token.clear();
				in_quotes = false;
			} else {
				// Start of quoted string
				in_quotes = true;
			}
			continue;
		}

		if (!in_quotes && (c == ' ' || c == '\t')) {
			// Whitespace outside quotes - end current token if any
			if (!current_token.empty()) {
				tokens.push_back(current_token);
				current_token.clear();
			}
			continue;
		}

		current_token += c;
	}

	// Don't forget the last token
	if (!current_token.empty()) {
		tokens.push_back(current_token);
	}

	return tokens;
}

bool HttpdConfReader::ParseDirectiveLine(const string &line, const string &directive, const string &config_file,
                                          idx_t line_number, ConfigEntry &entry) {
	entry = ConfigEntry();
	entry.config_file = config_file;
	entry.line_number = line_number;

	// Get the part after the directive name
	string rest = line.substr(directive.size());

	// Tokenize the rest
	auto tokens = TokenizeLine(rest);
	if (tokens.empty()) {
		return false;
	}

	if (directive == "LogFormat") {
		entry.log_type = "access";
		// LogFormat "format" [nickname]
		entry.format_string = tokens[0];
		if (tokens.size() >= 2) {
			// Check if second token is not env= or similar
			if (tokens[1].find('=') == string::npos) {
				entry.nickname = tokens[1];
				entry.format_type = "named"; // LogFormat with nickname
			} else {
				entry.format_type = "default"; // LogFormat without nickname
			}
		} else {
			entry.format_type = "default"; // LogFormat without nickname
		}
	} else if (directive == "CustomLog") {
		entry.log_type = "access";
		// CustomLog "path" format|nickname [env=...]
		if (tokens.size() < 2) {
			return false;
		}
		// tokens[0] is the log path - we don't store it anymore

		// Second token could be:
		// - A quoted format string (already unquoted by tokenizer, but was quoted in source)
		// - A nickname (unquoted identifier)
		// We need to check if it was originally quoted by looking at the original line
		string second_token = tokens[1];

		// Find position of second token in original line to check if it was quoted
		// First, find the log path in the line
		size_t path_start = line.find('"');
		if (path_start != string::npos) {
			// Find the closing quote of the path (handle escaped quotes)
			size_t path_end = path_start + 1;
			while (path_end < line.size()) {
				if (line[path_end] == '"' && (path_end == 0 || line[path_end - 1] != '\\')) {
					break;
				}
				path_end++;
			}
			// Skip past closing quote and whitespace
			path_end++;
			while (path_end < line.size() && (line[path_end] == ' ' || line[path_end] == '\t')) {
				path_end++;
			}
			if (path_end < line.size() && line[path_end] == '"') {
				// Second argument is quoted - it's an inline format string
				entry.format_string = second_token;
				entry.format_type = "inline";
			} else {
				// Second argument is not quoted - it's a nickname reference
				// We skip these as they don't define new formats
				return false;
			}
		} else {
			return false; // Invalid entry - no quoted path
		}
	} else if (directive == "ErrorLogFormat") {
		entry.log_type = "error";
		// ErrorLogFormat "format"
		entry.format_string = tokens[0];
		entry.format_type = "default"; // ErrorLogFormat only has one form
	} else {
		return false;
	}

	return true;
}

vector<HttpdConfReader::ConfigEntry> HttpdConfReader::ParseConfigFile(const string &path, FileSystem &fs) {
	vector<ConfigEntry> entries;

	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	HttpdLogBufferedReader reader(fs, path);

	string line;
	idx_t line_number = 0;
	string continued_line;
	idx_t continued_line_start = 0;

	while (reader.ReadLine(line)) {
		line_number++;

		// Handle line continuation
		if (!continued_line.empty()) {
			continued_line += " " + line;
		} else {
			continued_line = line;
			continued_line_start = line_number;
		}

		// Check for line continuation
		if (!continued_line.empty() && continued_line.back() == '\\') {
			continued_line.pop_back(); // Remove the backslash
			continue;                  // Read next line
		}

		// Trim leading whitespace
		string trimmed = continued_line;
		StringUtil::Trim(trimmed);

		// Skip empty lines and comments
		if (trimmed.empty() || trimmed[0] == '#') {
			continued_line.clear();
			continue;
		}

		// Check for directives (case-insensitive)
		string upper = StringUtil::Upper(trimmed);

		ConfigEntry entry;
		bool parsed = false;
		if (upper.rfind("LOGFORMAT ", 0) == 0 || upper.rfind("LOGFORMAT\t", 0) == 0) {
			parsed = ParseDirectiveLine(trimmed, "LogFormat", path, continued_line_start, entry);
		} else if (upper.rfind("CUSTOMLOG ", 0) == 0 || upper.rfind("CUSTOMLOG\t", 0) == 0) {
			parsed = ParseDirectiveLine(trimmed, "CustomLog", path, continued_line_start, entry);
		} else if (upper.rfind("ERRORLOGFORMAT ", 0) == 0 || upper.rfind("ERRORLOGFORMAT\t", 0) == 0) {
			parsed = ParseDirectiveLine(trimmed, "ErrorLogFormat", path, continued_line_start, entry);
		} else if (upper.rfind("ERRORLOG ", 0) == 0 || upper.rfind("ERRORLOG\t", 0) == 0) {
			parsed = ParseDirectiveLine(trimmed, "ErrorLog", path, continued_line_start, entry);
		}

		if (parsed) {
			entries.push_back(std::move(entry));
		}

		continued_line.clear();
	}

	return entries;
}

unique_ptr<FunctionData> HttpdConfReader::Bind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto &fs = FileSystem::GetFileSystem(context);

	// Get the file path pattern
	auto path_pattern = input.inputs[0].GetValue<string>();

	// Expand glob pattern
	auto files = fs.GlobFiles(path_pattern, context, FileGlobOptions::ALLOW_EMPTY);

	// Parse all config files
	auto bind_data = make_uniq<BindData>();
	for (const auto &file : files) {
		auto entries = ParseConfigFile(file.path, fs);
		for (auto &entry : entries) {
			bind_data->entries.push_back(std::move(entry));
		}
	}

	// Define output schema
	names.emplace_back("log_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("format_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("nickname");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("format_string");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("config_file");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("line_number");
	return_types.emplace_back(LogicalType::INTEGER);

	return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> HttpdConfReader::Init(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GlobalState>();
}

void HttpdConfReader::Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<BindData>();
	auto &state = data.global_state->Cast<GlobalState>();

	idx_t output_idx = 0;
	const idx_t BATCH_SIZE = STANDARD_VECTOR_SIZE;

	while (output_idx < BATCH_SIZE && state.current_idx < bind_data.entries.size()) {
		const auto &entry = bind_data.entries[state.current_idx];

		// log_type
		FlatVector::GetData<string_t>(output.data[0])[output_idx] =
		    StringVector::AddString(output.data[0], entry.log_type);

		// format_type
		FlatVector::GetData<string_t>(output.data[1])[output_idx] =
		    StringVector::AddString(output.data[1], entry.format_type);

		// nickname
		if (entry.nickname.empty()) {
			FlatVector::SetNull(output.data[2], output_idx, true);
		} else {
			FlatVector::GetData<string_t>(output.data[2])[output_idx] =
			    StringVector::AddString(output.data[2], entry.nickname);
		}

		// format_string
		if (entry.format_string.empty()) {
			FlatVector::SetNull(output.data[3], output_idx, true);
		} else {
			FlatVector::GetData<string_t>(output.data[3])[output_idx] =
			    StringVector::AddString(output.data[3], entry.format_string);
		}

		// config_file
		FlatVector::GetData<string_t>(output.data[4])[output_idx] =
		    StringVector::AddString(output.data[4], entry.config_file);

		// line_number
		FlatVector::GetData<int32_t>(output.data[5])[output_idx] = static_cast<int32_t>(entry.line_number);

		output_idx++;
		state.current_idx++;
	}

	output.SetCardinality(output_idx);
}

void HttpdConfReader::RegisterFunction(ExtensionLoader &loader) {
	TableFunction func("read_httpd_conf", {LogicalType::VARCHAR}, Function, Bind, Init);
	loader.RegisterFunction(func);
}

} // namespace duckdb
