//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/show_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/show_select_info.hpp"

namespace duckdb {

class ShowStatement : public SQLStatement {
public:
	ShowStatement();

	unique_ptr<ShowSelectInfo> info;

protected:
	ShowStatement(const ShowStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	bool Equals(const SQLStatement *other) const override;
};

} // namespace duckdb
