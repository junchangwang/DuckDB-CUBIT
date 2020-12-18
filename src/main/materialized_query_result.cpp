#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type) {
}

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, vector<LogicalType> types,
                                                 vector<string> names)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, move(types), names) {
}

MaterializedQueryResult::MaterializedQueryResult(string error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, error) {
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	auto &data = collection.GetChunkForRow(index).data[column];
	auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
	return data.GetValue(offset_in_chunk);
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[ Rows: " + std::to_string(collection.Count()) + "]\n";
		for (idx_t j = 0; j < collection.Count(); j++) {
			for (idx_t i = 0; i < collection.ColumnCount(); i++) {
				auto val = collection.GetValue(i, j);
				result += val.is_null ? "NULL" : val.ToString();
				result += "\t";
			}
			result += "\n";
		}
		result += "\n";
	} else {
		result = error + "\n";
	}
	return result;
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	if (!success) {
		return nullptr;
	}

	return collection.Fetch();
}

} // namespace duckdb
