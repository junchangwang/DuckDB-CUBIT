#include "duckdb/common/types/row/partitioned_tuple_data.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PartitionedTupleData::PartitionedTupleData(PartitionedTupleDataType type_p, BufferManager &buffer_manager_p,
                                           TupleDataLayout layout_p)
    : type(type_p), buffer_manager(buffer_manager_p), layout(std::move(layout_p)),
      allocators(make_shared<PartitionTupleDataAllocators>()) {
}

PartitionedTupleData::PartitionedTupleData(const PartitionedTupleData &other)
    : type(other.type), buffer_manager(other.buffer_manager), layout(other.layout) {
}

unique_ptr<PartitionedTupleData> PartitionedTupleData::CreateShared() {
	switch (type) {
	case PartitionedTupleDataType::RADIX:
		return make_unique<RadixPartitionedTupleData>((RadixPartitionedTupleData &)*this);
	default:
		throw NotImplementedException("CreateShared for this type of PartitionedTupleData");
	}
}

PartitionedTupleData::~PartitionedTupleData() {
}

PartitionedTupleDataType PartitionedTupleData::GetType() const {
	return type;
}

void PartitionedTupleData::InitializeAppendState(PartitionedTupleDataAppendState &state) const {
	state.partition_sel.Initialize();

	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}

	InitializeAppendStateInternal(state);
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Build the selection vector for the partitions
	unordered_map<idx_t, list_entry_t> partition_entries;
	BuildPartitionSel(state, input.size(), partition_entries);

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.Append(partition_pin_state, state.chunk_state, input);
		return;
	}

	TupleDataCollection::ToUnifiedFormat(state.chunk_state, input);

	// Compute the heap sizes for the whole chunk
	if (!layout.AllConstant()) {
		TupleDataCollection::ComputeHeapSizes(state.chunk_state, input, state.partition_sel, input.size());
	}

	// Build the buffer space
	BuildBufferSpace(state, partition_entries);

	// Now scatter everything in one go
	partitions[0]->Scatter(state.chunk_state, input, state.partition_sel, input.size());
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, TupleDataChunkState &input, idx_t count) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(input.row_locations, count, state.partition_indices);

	// Build the selection vector for the partitions
	unordered_map<idx_t, list_entry_t> partition_entries;
	BuildPartitionSel(state, count, partition_entries);

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];

		state.chunk_state.heap_sizes.Reference(input.heap_sizes);
		partition.Build(partition_pin_state, state.chunk_state, 0, count);
		partition.CopyRows(state.chunk_state, input, *FlatVector::IncrementalSelectionVector(), count);
		return;
	}

	// Build the buffer space
	state.chunk_state.heap_sizes.Slice(input.heap_sizes, state.partition_sel, count);
	state.chunk_state.heap_sizes.Flatten(count);
	BuildBufferSpace(state, partition_entries);

	// Copy the rows
	partitions[0]->CopyRows(state.chunk_state, input, state.partition_sel, count);
}

void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, idx_t count,
                                             unordered_map<idx_t, list_entry_t> &partition_entries) {
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	switch (state.partition_indices.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			const auto &partition_index = partition_indices[i];
			auto partition_entry = partition_entries.find(partition_index);
			if (partition_entry == partition_entries.end()) {
				partition_entries[partition_index] = list_entry_t(0, 1);
			} else {
				partition_entry->second.length++;
			}
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		partition_entries[partition_indices[0]] = list_entry_t(0, count);
		break;
	default:
		throw InternalException("Unexpected VectorType in PartitionedTupleData::Append");
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		return;
	}

	// Compute offsets from the counts
	idx_t offset = 0;
	for (auto &pc : partition_entries) {
		auto &partition_entry = pc.second;
		partition_entry.offset = offset;
		offset += partition_entry.length;
	}

	// Now initialize a single selection vector that acts as a selection vector for every partition
	auto &all_partitions_sel = state.partition_sel;
	for (idx_t i = 0; i < count; i++) {
		const auto &partition_index = partition_indices[i];
		auto &partition_offset = partition_entries[partition_index].offset;
		all_partitions_sel[partition_offset++] = i;
	}
}

void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state,
                                            const unordered_map<idx_t, list_entry_t> &partition_entries) {
	for (auto &pc : partition_entries) {
		const auto &partition_index = pc.first;

		// Partition, pin state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];

		// Length and offset for this partition
		const auto &partition_entry = pc.second;
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Build out the buffer space for this partition
		partition.Build(partition_pin_state, state.chunk_state, partition_offset, partition_length);
	}
}

void PartitionedTupleData::FlushAppendState(PartitionedTupleDataAppendState &state) {
	for (idx_t partition_index = 0; partition_index < partitions.size(); partition_index++) {
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.FinalizePinState(partition_pin_state);
	}
}

void PartitionedTupleData::Combine(PartitionedTupleData &other) {
	if (other.Count() == 0) {
		return;
	}

	// Now combine the state's partitions into this
	lock_guard<mutex> guard(lock);

	if (partitions.empty()) {
		// This is the first merge, we just copy them over
		partitions = std::move(other.partitions);
	} else {
		D_ASSERT(partitions.size() == other.partitions.size());
		// Combine the append state's partitions into this PartitionedTupleData
		for (idx_t i = 0; i < other.partitions.size(); i++) {
			partitions[i]->Combine(*other.partitions[i]);
		}
	}
}

void PartitionedTupleData::Repartition(PartitionedTupleData &new_partitioned_data) {
	D_ASSERT(layout.GetTypes() == new_partitioned_data.layout.GetTypes());

	PartitionedTupleDataAppendState append_state;
	new_partitioned_data.InitializeAppendState(append_state);

	const auto reverse = RepartitionReverseOrder();
	const idx_t start_idx = reverse ? partitions.size() : 0;
	const idx_t end_idx = reverse ? 0 : partitions.size();
	const int64_t update = reverse ? -1 : 1;
	const int64_t adjustment = reverse ? -1 : 0;

	for (idx_t partition_idx = start_idx; partition_idx != end_idx; partition_idx += update) {
		auto actual_partition_idx = partition_idx + adjustment;
		auto &partition = *partitions[actual_partition_idx];

		if (partition.Count() > 0) {
			TupleDataChunkIterator iterator(partition, TupleDataPinProperties::DESTROY_AFTER_DONE, true);
			auto &chunk_state = iterator.GetChunkState();
			do {
				auto count = iterator.GetCount();
				new_partitioned_data.Append(append_state, chunk_state, count);
			} while (iterator.Next());

			RepartitionFinalizeStates(*this, new_partitioned_data, append_state, actual_partition_idx);
		}
		partitions[actual_partition_idx] = nullptr;
	}

	new_partitioned_data.FlushAppendState(append_state);
}

vector<unique_ptr<TupleDataCollection>> &PartitionedTupleData::GetPartitions() {
	return partitions;
}

idx_t PartitionedTupleData::Count() const {
	idx_t total_count = 0;
	for (auto &partition : partitions) {
		total_count += partition->Count();
	}
	return total_count;
}

idx_t PartitionedTupleData::SizeInBytes() const {
	idx_t total_size = 0;
	for (auto &partition : partitions) {
		total_size += partition->SizeInBytes();
	}
	return total_size;
}

void PartitionedTupleData::CreateAllocator() {
	allocators->allocators.emplace_back(make_shared<TupleDataAllocator>(buffer_manager, layout));
}

} // namespace duckdb
