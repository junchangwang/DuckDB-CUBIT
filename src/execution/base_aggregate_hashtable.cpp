#include "duckdb/execution/base_aggregate_hashtable.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/storage/virtual_buffer_manager.hpp"

namespace duckdb {

BaseAggregateHashTable::BaseAggregateHashTable(ClientContext &context, Allocator &allocator,
                                               const vector<AggregateObject> &aggregates,
                                               vector<LogicalType> payload_types_p)
    : allocator(allocator), buffer_manager(VirtualBufferManager::GetBufferManager(context)),
      payload_types(move(payload_types_p)) {
	filter_set.Initialize(context, aggregates, payload_types);
}

} // namespace duckdb
