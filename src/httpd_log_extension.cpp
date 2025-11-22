#define DUCKDB_EXTENSION_MAIN

#include "httpd_log_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void HttpdLogScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "HttpdLog " + name.GetString() + " 🐥");
	});
}

inline void HttpdLogOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "HttpdLog " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto httpd_log_scalar_function = ScalarFunction("httpd_log", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HttpdLogScalarFun);
	loader.RegisterFunction(httpd_log_scalar_function);

	// Register another scalar function
	auto httpd_log_openssl_version_scalar_function = ScalarFunction("httpd_log_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, HttpdLogOpenSSLVersionScalarFun);
	loader.RegisterFunction(httpd_log_openssl_version_scalar_function);
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
