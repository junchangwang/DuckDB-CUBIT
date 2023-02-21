#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {

DeleteStatement::DeleteStatement() : SQLStatement(StatementType::DELETE_STATEMENT) {
}

DeleteStatement::DeleteStatement(const DeleteStatement &other) : SQLStatement(other), table(other.table->Copy()) {
	if (other.condition) {
		condition = other.condition->Copy();
	}
	using_clauses.reserve(other.using_clauses.size());
	for (const auto &using_clause : other.using_clauses) {
		using_clauses.push_back(using_clause->Copy());
	}
	returning_list.reserve(other.returning_list.size());
	for (auto &expr : other.returning_list) {
		returning_list.emplace_back(expr->Copy());
	}
	cte_map = other.cte_map.Copy();
}

string DeleteStatement::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "DELETE FROM ";
	result += table->ToString();
	if (!using_clauses.empty()) {
		result += " USING ";
		for (idx_t i = 0; i < using_clauses.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += using_clauses[i]->ToString();
		}
	}
	if (condition) {
		result += " WHERE " + condition->ToString();
	}

	if (!returning_list.empty()) {
		result += " RETURNING ";
		for (idx_t i = 0; i < returning_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += returning_list[i]->ToString();
		}
	}
	return result;
}

unique_ptr<SQLStatement> DeleteStatement::Copy() const {
	return unique_ptr<DeleteStatement>(new DeleteStatement(*this));
}

bool DeleteStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const DeleteStatement &)*other_p;

	if (!condition && !other.condition) {
		// both dont have it
	} else if (!condition || !other.condition) {
		// one of them has it, other doesn't
		return false;
	} else if (!condition->Equals(other.condition.get())) {
		// both have it, but they are not the same
		return false;
	}

	if (!table && !other.table) {
		// both dont have it
	} else if (!table || !other.table) {
		// one of them has it, other doesn't
		return false;
	} else if (!table->Equals(other.table.get())) {
		// both have it, but they are not the same
		return false;
	}

	if (using_clauses.size() != other.using_clauses.size()) {
		return false;
	}
	for (idx_t i = 0; i < using_clauses.size(); i++) {
		auto &lhs = using_clauses[i];
		auto &rhs = other.using_clauses[i];
		if (!lhs->Equals(rhs.get())) {
			return false;
		}
	}

	if (returning_list.size() != other.returning_list.size()) {
		return false;
	}
	for (idx_t i = 0; i < returning_list.size(); i++) {
		auto &lhs = returning_list[i];
		auto &rhs = other.returning_list[i];
		if (!lhs->Equals(rhs.get())) {
			return false;
		}
	}

	return cte_map.Equals(other.cte_map);
}

} // namespace duckdb
