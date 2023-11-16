//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/brace_expansion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class BraceExpansion {
public:
    static vector<string> brace_expansion(const string &pattern);
    
};

}   // namespace duckdb
