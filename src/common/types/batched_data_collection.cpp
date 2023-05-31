#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

BatchedDataCollection::BatchedDataCollection(vector<LogicalType> types_p) : types(std::move(types_p)) {
}

void BatchedDataCollection::Append(DataChunk &input, idx_t batch_index) {
	D_ASSERT(batch_index != DConstants::INVALID_INDEX);
	optional_ptr<ColumnDataCollection> collection;
	if (last_collection.collection && last_collection.batch_index == batch_index) {
		// we are inserting into the same collection as before: use it directly
		collection = last_collection.collection;
	} else {
		// new collection: check if there is already an entry
		D_ASSERT(data.find(batch_index) == data.end());
		unique_ptr<ColumnDataCollection> new_collection;
		if (last_collection.collection) {
			new_collection = make_uniq<ColumnDataCollection>(*last_collection.collection);
		} else {
			new_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		}
		last_collection.collection = new_collection.get();
		last_collection.batch_index = batch_index;
		new_collection->InitializeAppend(last_collection.append_state);
		collection = new_collection.get();
		data.insert(make_pair(batch_index, std::move(new_collection)));
	}
	collection->Append(last_collection.append_state, input);
}

void BatchedDataCollection::Merge(BatchedDataCollection &other) {
	for (auto &entry : other.data) {
		if (data.find(entry.first) != data.end()) {
			throw InternalException(
			    "BatchedDataCollection::Merge error - batch index %d is present in both collections. This occurs when "
			    "batch indexes are not uniquely distributed over threads",
			    entry.first);
		}
		data[entry.first] = std::move(entry.second);
	}
	other.data.clear();
}

void BatchedDataCollection::InitializeScan(BatchedChunkScanState &state, const BatchedChunkIteratorRange &range) {
	state.range = range;
	if (state.range.begin == state.range.end) {
		return;
	}
	state.range.begin->second->InitializeScan(state.scan_state);
}

void BatchedDataCollection::InitializeScan(BatchedChunkScanState &state) {
	auto range = BatchRange();
	return InitializeScan(state, range);
}

void BatchedDataCollection::Scan(BatchedChunkScanState &state, DataChunk &output) {
	while (state.range.begin != state.range.end) {
		// check if there is a chunk remaining in this collection
		auto collection = state.range.begin->second.get();
		collection->Scan(state.scan_state, output);
		if (output.size() > 0) {
			return;
		}
		// there isn't! move to the next collection
		state.range.begin++;
		if (state.range.begin == state.range.end) {
			return;
		}
		state.range.begin->second->InitializeScan(state.scan_state);
	}
}

unique_ptr<ColumnDataCollection> BatchedDataCollection::FetchCollection() {
	unique_ptr<ColumnDataCollection> result;
	for (auto &entry : data) {
		if (!result) {
			result = std::move(entry.second);
		} else {
			result->Combine(*entry.second);
		}
	}
	data.clear();
	if (!result) {
		// empty result
		return make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	return result;
}

idx_t BatchedDataCollection::BatchCount() const {
	return data.size();
}

BatchedChunkIteratorRange BatchedDataCollection::BatchRange(idx_t begin_idx, idx_t end_idx) {
	D_ASSERT(begin_idx < end_idx);
	if (end_idx > data.size()) {
		// Limit the iterator to the end
		end_idx = DConstants::INVALID_INDEX;
	}
	BatchedChunkIteratorRange range;
	range.begin = data.begin();
	std::advance(range.begin, begin_idx);
	if (end_idx == DConstants::INVALID_INDEX) {
		range.end = data.end();
	} else {
		range.end = data.begin();
		std::advance(range.end, end_idx);
	}
	return range;
}

string BatchedDataCollection::ToString() const {
	string result;
	result += "Batched Data Collection\n";
	for (auto &entry : data) {
		result += "Batch Index - " + to_string(entry.first) + "\n";
		result += entry.second->ToString() + "\n\n";
	}
	return result;
}

void BatchedDataCollection::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
