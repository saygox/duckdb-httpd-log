#include "httpd_log_buffered_reader.hpp"

namespace duckdb {

HttpdLogBufferedReader::HttpdLogBufferedReader(FileSystem &fs, const string &path) {
	file_handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	buffer = make_unsafe_uniq_array_uninitialized<char>(BUFFER_SIZE);
	RefillBuffer();
}

void HttpdLogBufferedReader::RefillBuffer() {
	if (eof_reached) {
		buffer_size = 0;
		return;
	}

	buffer_size = file_handle->Read(buffer.get(), BUFFER_SIZE);
	buffer_offset = 0;

	if (buffer_size < BUFFER_SIZE) {
		eof_reached = true;
	}
}

bool HttpdLogBufferedReader::ReadLine(string &result) {
	result.clear();

	while (true) {
		// バッファ内で改行を探す
		while (buffer_offset < buffer_size) {
			char c = buffer[buffer_offset++];

			if (c == '\n') {
				// 末尾の \r を削除
				if (!result.empty() && result.back() == '\r') {
					result.pop_back();
				}
				return true;
			}
			result += c;
		}

		// バッファが空になった場合
		if (buffer_size == 0 || (eof_reached && buffer_offset >= buffer_size)) {
			// EOF に到達
			return !result.empty();
		}

		// 新しいデータを読み込み
		RefillBuffer();
	}
}

bool HttpdLogBufferedReader::Finished() const {
	return eof_reached && buffer_offset >= buffer_size;
}

} // namespace duckdb
