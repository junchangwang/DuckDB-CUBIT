//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/drop_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class DropStatement : public SQLStatement {
public:
	DropStatement();

	unique_ptr<DropInfo> info;

protected:
	DropStatement(const DropStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	bool Equals(const SQLStatement *other) const override;
};

} // namespace duckdb
