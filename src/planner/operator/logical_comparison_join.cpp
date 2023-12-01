#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/enum_util.hpp"
namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = EnumUtil::ToChars(join_type);
	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
		result = "LEFT " + result;
		break;
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
		result = "RIGHT " + result;
		break;
	default:
		break;
	}
	for (auto &condition : conditions) {
		result += "\n";
		auto expr =
		    make_uniq<BoundComparisonExpression>(condition.comparison, condition.left->Copy(), condition.right->Copy());
		result += expr->ToString();
	}

	return result;
}

} // namespace duckdb
