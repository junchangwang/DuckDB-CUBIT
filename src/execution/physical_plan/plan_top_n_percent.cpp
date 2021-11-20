#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTopNPercent &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto top_n =
	    make_unique<PhysicalTopN>(op.types, move(op.orders), op.limit_percent, op.offset, op.estimated_cardinality);
	top_n->children.push_back(move(plan));
	return move(top_n);
}

} // namespace duckdb
