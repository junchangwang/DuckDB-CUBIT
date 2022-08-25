//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/vacuum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

enum class LoadType { LOAD, INSTALL, FORCE_INSTALL };

struct LoadInfo : public ParseInfo {
public:
	LoadInfo() {
	}
	LoadType load_type;

public:
	const string &FileName() {
		return filename;
	}
	void SetFileName(const string &file_name) {
		filename = file_name;
	}
	unique_ptr<LoadInfo> Copy() const {
		auto result = make_unique<LoadInfo>();
		result->filename = filename;
		result->load_type = load_type;
		return result;
	}

private:
	string filename;
};

} // namespace duckdb
