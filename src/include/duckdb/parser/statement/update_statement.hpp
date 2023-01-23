//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class UpdateStatement : public SQLStatement {
public:
	UpdateStatement();

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;
	unique_ptr<TableRef> from_table;
	vector<string> columns;
	vector<unique_ptr<ParsedExpression>> expressions;
	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;
	//! CTEs
	CommonTableExpressionMap cte_map;

protected:
	UpdateStatement(const UpdateStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
	bool Equals(const SQLStatement *other) const override;
};

} // namespace duckdb
