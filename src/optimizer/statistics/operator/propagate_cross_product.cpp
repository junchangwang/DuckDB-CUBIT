#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb
{
unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalCrossProduct &cp, unique_ptr<LogicalOperator> *node_ptr)
{
	auto left_child = unique_ptr<LogicalOperator>((LogicalOperator*)cp.children[0].get());
	auto right_child = unique_ptr<LogicalOperator>((LogicalOperator*)cp.children[1].get());
	// first propagate statistics in the child node
	auto left_stats = PropagateStatistics(left_child);
	auto right_stats = PropagateStatistics(right_child);
	if (!left_stats || !right_stats)
	{
		return nullptr;
	}
	MultiplyCardinalities(left_stats, *right_stats);
	return left_stats;
}
} // namespace duckdb