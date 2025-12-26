#pragma once
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class HttpdLogBufferedReader {
public:
	HttpdLogBufferedReader(FileSystem &fs, const string &path);
	bool ReadLine(string &result);
	bool Finished() const;

private:
	void RefillBuffer();

	unique_ptr<FileHandle> file_handle;
	static constexpr idx_t BUFFER_SIZE = 2097152; // 2MB
	unsafe_unique_array<char> buffer;
	idx_t buffer_offset = 0;
	idx_t buffer_size = 0;
	bool eof_reached = false;
};

} // namespace duckdb
