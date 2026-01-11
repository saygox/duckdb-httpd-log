#define DUCKDB_EXTENSION_MAIN

#include "httpd_log_extension.hpp"
#include "httpd_log_table_function.hpp"
#include "httpd_conf_reader.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the read_httpd_log table function
	HttpdLogTableFunction::RegisterFunction(loader);

	// Register the read_httpd_conf table function
	HttpdConfReader::RegisterFunction(loader);
}

void HttpdLogExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HttpdLogExtension::Name() {
	return "httpd_log";
}

std::string HttpdLogExtension::Version() const {
#ifdef EXT_VERSION_HTTPD_LOG
	return EXT_VERSION_HTTPD_LOG;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(httpd_log, loader) {
	duckdb::LoadInternal(loader);
}
}
