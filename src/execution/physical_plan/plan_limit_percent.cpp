#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimitPercent &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto limit = make_unique<PhysicalLimit>(op.types, op.limit_percent, op.offset_val, move(op.limit), move(op.offset),
	                                        op.estimated_cardinality);
	limit->children.push_back(move(plan));
	return move(limit);
}

} // namespace duckdb
