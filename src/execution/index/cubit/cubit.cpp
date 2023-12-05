
#include "duckdb/execution/index/cubit/cubit.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "table.h"
#include "table_lk.h"
#include <cstdint>

namespace duckdb {

struct CUBITIndexScanState : public IndexScanState {
	Value values[2];
	ExpressionType expressions[2];
	vector<row_t> result_ids;
};

CUBIT::CUBIT(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
             const vector<unique_ptr<Expression>> &unbound_expressions, const IndexConstraintType constraint_type,
             AttachedDatabase &db)
    : Index(db, IndexType::CUBIT, table_io_manager, column_ids, unbound_expressions, constraint_type) {
	for (idx_t i = 0; i < types.size(); i++) {
        switch (types[i]) {
		// case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		// case PhysicalType::INT128:
		// case PhysicalType::UINT8:
		// case PhysicalType::UINT16:
		// case PhysicalType::UINT32:
		// case PhysicalType::UINT64:
		// case PhysicalType::FLOAT:
		// case PhysicalType::DOUBLE:
		// case PhysicalType::VARCHAR:
			break;
		default:
			throw InvalidTypeException(logical_types[i], "Invalid type for index key.");
        }
    }

	Table_config *config = new Table_config{};
	config->n_workers = 1; // g_thread_cnt;
	config->DATA_PATH = "";
	// if (Mode && strcmp(Mode, "cache") == 0) {
	// 	string temp = "bm_";
	// 	temp.append(to_string(curr_SF));
	// 	temp.append("_shipdate");
	// 	config->INDEX_PATH = temp;
	// } else
	config->INDEX_PATH = "";
	config->n_rows = 15000; // n_rows; 
	config->g_cardinality = 20; // [92, 98]
	enable_fence_pointer = config->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config->approach = "nbub-lk";
	config->nThreads_for_getval = 4;
	config->show_memory = true;
	config->on_disk = false;
	config->showEB = false;
    config->decode = false;

	// DB doesn't use the following parameters;
	// they are used by nicolas.
	config->n_queries = 0;
	config->n_udis = 0;
	config->verbose = false;
	config->time_out = 100;
	config->autoCommit = false;
	config->n_merge_threshold = 16;
	config->db_control = false;

	config->segmented_btv = false;
	config->encoded_word_len = 31;
	config->rows_per_seg = 100000;
	config->enable_parallel_cnt = false;


	this->bitmap = new nbub_lk::NbubLK(config);
	db_timestamp = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	db_number_of_rows = 15000;
}

unique_ptr<IndexScanState> CUBIT::InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
                                                         const ExpressionType expression_type) {
    auto result = make_uniq<CUBITIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return std::move(result);
}

unique_ptr<IndexScanState> CUBIT::InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
                                                              const ExpressionType low_expression_type,
                                                              const Value &high_value,
                                                              const ExpressionType high_expression_type) {
	auto result = make_uniq<CUBITIndexScanState>();
	result->values[0] = low_value;
    result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
    result->expressions[1] = high_expression_type;
    return std::move(result);
}

bool CUBIT::Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
                 vector<row_t> &result_ids) {
	auto scan_state = state.Cast<CUBITIndexScanState>();
	vector<row_t> row_ids;
	bool success;

	D_ASSERT(scan_state.values[0].type().InternalType() == types[0]);

	if (scan_state.values[0].IsNull()) {
		// single predicate
		lock_guard<mutex> l(lock);
		switch (scan_state.expressions[0]) {
		case ExpressionType::COMPARE_EQUAL:
            success = SearchEqual(scan_state, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			success = SearchGreater(scan_state, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			success = SearchGreater(scan_state, false, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			success = SearchLess(scan_state, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			success = SearchLess(scan_state, false, max_count, row_ids);
			break;
		default:
			throw InternalException("Index scan type not implemented");
        }
	} else {
		// two predicates
		lock_guard<mutex> l(lock);

	    D_ASSERT(scan_state.values[1].type().InternalType() == types[0]);
		bool left_equal = scan_state.expressions[0] == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		bool right_equal = scan_state.expressions[1] == ExpressionType::COMPARE_LESSTHANOREQUALTO;
		success = SearchCloseRange(scan_state, left_equal, right_equal, max_count, row_ids);
    }

	if (!success) {
		return false;
    }
	if (row_ids.empty()) {
		return true;
    }
	sort(row_ids.begin(), row_ids.end());
	result_ids.reserve(row_ids.size());

	for (idx_t i = 0; i < row_ids.size(); i++) {
		result_ids.push_back(row_ids[i]);
    }
    return true;
}

PreservedError CUBIT::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	ExecuteExpressions(appended_data, expression_result);

	return Insert(lock, expression_result, row_identifiers);
}


PreservedError CUBIT::Insert(IndexLock &lock, DataChunk &data, Vector &row_identifiers) {
	D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == data.data[0].GetType());

	row_identifiers.Flatten(data.size());
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	D_ASSERT(data.data.size() == 1);
	auto input = data.data[0];
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(data.size(), idata);

	auto *cubit_bitmap = dynamic_cast<nbub::Nbub *>(bitmap);
	switch (input.GetType().InternalType()) {
	case PhysicalType::INT8: {
		auto input_data_i8 = UnifiedVectorFormat::GetData<int8_t>(idata);
		for (idx_t i = 0; i < data.size(); i++) {
			auto idx = idata.sel->get_index(i);
			if (idata.validity.RowIsValid(idx)) {
				cubit_bitmap->__init_append(0, row_ids[i], input_data_i8[i]);
			}
		}
		break;
	}
	case PhysicalType::INT16: {
		auto input_data_i16 = UnifiedVectorFormat::GetData<int16_t>(idata);
		for (idx_t i = 0; i < data.size(); i++) {
			auto idx = idata.sel->get_index(i);
			if (idata.validity.RowIsValid(idx)) {
				cubit_bitmap->__init_append(0, row_ids[i], input_data_i16[i]);
			}
		}
		break;
	}
	case PhysicalType::INT32: {
		auto input_data_i32 = UnifiedVectorFormat::GetData<int16_t>(idata);
		for (idx_t i = 0; i < data.size(); i++) {
			auto idx = idata.sel->get_index(i);
			if (idata.validity.RowIsValid(idx)) {
				cubit_bitmap->__init_append(0, row_ids[i], input_data_i32[i]);
			}
		}
		break;
	}
	case PhysicalType::INT64: {
		auto input_data_i64 = UnifiedVectorFormat::GetData<int16_t>(idata);
		for (idx_t i = 0; i < data.size(); i++) {
			auto idx = idata.sel->get_index(i);
			if (idata.validity.RowIsValid(idx)) {
				// input_data[i]
				// bitmap->bitmaps[input_data[i]]
				cubit_bitmap->__init_append(0, row_ids[i], input_data_i64[i]);
			}
		}
		break;
	}
	default:
		throw InternalException("Invalid expression type for cubit index");
	}

	return PreservedError();
}


bool CUBIT::MergeIndexes(IndexLock &state, Index &other_index) {
	auto &other_cubit = other_index.Cast<CUBIT>();

	auto this_nbub = dynamic_cast<nbub::Nbub *>(bitmap);
	auto other_nbub = dynamic_cast<nbub::Nbub *>(other_cubit.bitmap);

	D_ASSERT(this_nbub->num_bitmaps == other_nbub->num_bitmaps);

	for (int i = 0; i < this_nbub->num_bitmaps; i++) {
		auto bitmap = this_nbub->bitmaps[i];
		auto other_bitmap = other_nbub->bitmaps[i];
		*bitmap->btv |= *other_bitmap->btv;
	}

	return true;
}

bool CUBIT::SearchEqual(CUBITIndexScanState &state, idx_t max_count, vector<row_t> &result_ids) {
    // where 'expression' == 'value'
	auto value = state.values[0];
    auto type = value.type().InternalType();

	int v;
	switch (type) {
	case PhysicalType::INT8:
		v = value.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		v = value.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		v = value.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		v = value.GetValueUnsafe<int64_t>();
		break;
	default:
    throw InternalException("Invalid attribute type for our cubit index");
    }

	auto *cubit_bitmap = dynamic_cast<nbub::Nbub *>(bitmap);
	ibis::bitvector *bit_vector = cubit_bitmap->bitmaps[v]->btv;

	// TODO: use BMI instructions to accelerate reading (chienguo)
    uint64_t cnt = 0;
	for (ibis::bitvector::indexSet index_set = bit_vector->firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
        const ibis::bitvector::word_t *idx = index_set.indices();
		if (index_set.isRange()) {
			for (ibis::bitvector::word_t row_id = *idx; row_id < idx[1]; ++row_id) {
				result_ids.push_back(row_id);
                cnt++;
                if (cnt >= max_count) {
                    break;
                }
            }
		} else {
			for (unsigned j = 0; j < index_set.nIndices(); ++j) {
				result_ids.push_back(idx[j]);
				cnt++;
				if (cnt >= max_count) {
					break;
                }
            }
		}
	}
	return true;
}

bool CUBIT::SearchGreater(CUBITIndexScanState &state, bool equal, idx_t max_count, vector<row_t> &result_ids) {
	auto value = state.values[0];
    auto type = value.type().InternalType();

	int v;
	switch (type) {
	case PhysicalType::INT8:
		v = value.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		v = value.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		v = value.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		v = value.GetValueUnsafe<int64_t>();
		break;
	default:
    throw InternalException("Invalid attribute type for our cubit index");
    }

	auto *cubit_bitmap = dynamic_cast<nbub::Nbub *>(bitmap);
    vector<ibis::bitvector *> btvs;
	if (equal) {
        btvs.push_back(cubit_bitmap->bitmaps[v]->btv);
	}
	for (int i = v + 1; i < cubit_bitmap->num_bitmaps; i++) {
		if (cubit_bitmap->bitmaps[i] != NULL) {
			btvs.push_back(cubit_bitmap->bitmaps[i]->btv);
        }
    }

	ibis::bitvector greater_btv;
	greater_btv.copy(*btvs[0]);
	for (size_t i = 1; i < btvs.size(); i++) {
		greater_btv |= *btvs[i];
    }

    uint64_t cnt = 0;
	for (ibis::bitvector::indexSet index_set = greater_btv.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
        const ibis::bitvector::word_t *idx = index_set.indices();
		if (index_set.isRange()) {
			for (ibis::bitvector::word_t row_id = *idx; row_id < idx[1]; ++row_id) {
				result_ids.push_back(row_id);
                cnt++;
                if (cnt >= max_count) {
                    break;
                }
            }
		} else {
			for (unsigned j = 0; j < index_set.nIndices(); ++j) {
				result_ids.push_back(idx[j]);
				cnt++;
				if (cnt >= max_count) {
					break;
                }
            }
		}
	}
	return true;
}


bool CUBIT::SearchLess(CUBITIndexScanState &state, bool equal, idx_t max_count, vector<row_t> &result_ids) {
	auto value = state.values[0];
    auto type = value.type().InternalType();

	int v;
	switch (type) {
	case PhysicalType::INT8:
		v = value.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		v = value.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		v = value.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		v = value.GetValueUnsafe<int64_t>();
		break;
	default:
    throw InternalException("Invalid attribute type for our cubit index");
    }

	auto *cubit_bitmap = dynamic_cast<nbub::Nbub *>(bitmap);
    vector<ibis::bitvector *> btvs;
	if (equal) {
        btvs.push_back(cubit_bitmap->bitmaps[v]->btv);
	}
	for (int i = v - 1; i >= 0; i--) {
		if (cubit_bitmap->bitmaps[i] != NULL) {
			btvs.push_back(cubit_bitmap->bitmaps[i]->btv);
        }
    }

	ibis::bitvector less_btv;
	less_btv.copy(*btvs[0]);
	for (size_t i = 1; i < btvs.size(); i++) {
		less_btv |= *btvs[i];
    }

    uint64_t cnt = 0;
	for (ibis::bitvector::indexSet index_set = less_btv.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
        const ibis::bitvector::word_t *idx = index_set.indices();
		if (index_set.isRange()) {
			for (ibis::bitvector::word_t row_id = *idx; row_id < idx[1]; ++row_id) {
				result_ids.push_back(row_id);
                cnt++;
                if (cnt >= max_count) {
                    break;
                }
            }
		} else {
			for (unsigned j = 0; j < index_set.nIndices(); ++j) {
				result_ids.push_back(idx[j]);
				cnt++;
				if (cnt >= max_count) {
					break;
                }
            }
		}
	}
	return true;
}

bool CUBIT::SearchCloseRange(CUBITIndexScanState &state, bool left_equal, bool right_equal, idx_t max_count,
                             vector<row_t> &result_ids) {
	auto left_value = state.values[0];
	auto right_value = state.values[1];

    auto left_type = left_value.type().InternalType();
    auto right_type = right_value.type().InternalType();

	int left_v;
	switch (left_type) {
	case PhysicalType::INT8:
		left_v = left_value.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		left_v = left_value.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		left_v = left_value.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		left_v = left_value.GetValueUnsafe<int64_t>();
		break;
	default:
    throw InternalException("Invalid attribute type for our cubit index");
    }

	int right_v;
	switch (right_type) {
	case PhysicalType::INT8:
		right_v = right_value.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		right_v = right_value.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		right_v = right_value.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		right_v = right_value.GetValueUnsafe<int64_t>();
		break;
	default:
    throw InternalException("Invalid attribute type for our cubit index");
	}

    vector<ibis::bitvector *> btvs;
	auto *cubit_bitmap = dynamic_cast<nbub::Nbub *>(bitmap);
	if (left_equal) {
		btvs.push_back(cubit_bitmap->bitmaps[left_v]->btv);
	}
	for (int i = left_v + 1; i < right_v; i++) {
		if (cubit_bitmap->bitmaps[i] != NULL) {
			btvs.push_back(cubit_bitmap->bitmaps[i]->btv);
        }
    }
	if (right_equal) {
		btvs.push_back(cubit_bitmap->bitmaps[right_v]->btv);
	}

	ibis::bitvector range_btv;
	range_btv.copy(*btvs[0]);
	for (int i = 1; i < btvs.size(); i++) {
		range_btv |= *btvs[i];
    }

    uint64_t cnt = 0;
	for (ibis::bitvector::indexSet index_set = range_btv.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
        const ibis::bitvector::word_t *idx = index_set.indices();
		if (index_set.isRange()) {
			for (ibis::bitvector::word_t row_id = *idx; row_id < idx[1]; ++row_id) {
				result_ids.push_back(row_id);
                cnt++;
                if (cnt >= max_count) {
                    break;
                }
            }
		} else {
			for (unsigned j = 0; j < index_set.nIndices(); ++j) {
				result_ids.push_back(idx[j]);
				cnt++;
				if (cnt >= max_count) {
					break;
                }
            }
		}
	}
	return true;
}

} // namespace duckdb
