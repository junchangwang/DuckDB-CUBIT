//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-strptime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterICUStrptimeFunctions(DatabaseInstance &instance);
void RegisterICUStrptimeCasts(ClientContext &context);

} // namespace duckdb
