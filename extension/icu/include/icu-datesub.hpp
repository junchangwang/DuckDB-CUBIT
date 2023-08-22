//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datediff.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUDateSubFunctions(DatabaseInstance &instance);

} // namespace duckdb
