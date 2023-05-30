//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/numpy/physical_numpy_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

namespace duckdb {

class PhysicalNumpyCollector : public PhysicalResultCollector {
public:
	PhysicalNumpyCollector(PreparedStatementData &data, bool parallel);
	bool parallel;

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;

public:
	static unique_ptr<PhysicalResultCollector> Create(ClientContext &context, PreparedStatementData &data);
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool ParallelSink() const override;
	bool SinkOrderDependent() const override;
};

} // namespace duckdb
