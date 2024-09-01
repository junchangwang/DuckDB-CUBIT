#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/tpch_constants.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ios>
#include <numeric>
#include <signal.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <iomanip>
#include <immintrin.h>
#include <arpa/inet.h>

namespace duckdb {

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p,
                                     vector<column_t> column_ids_p, vector<idx_t> projection_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality, ExtraOperatorInfo extra_info)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)),
      column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)),
      table_filters(std::move(table_filters_p)), extra_info(extra_info) {
		cursor = new idx_t(0);
		row_ids = new vector<row_t>;
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.function.init_global) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;

	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalTableScan &op) {
		if (op.function.init_local) {
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
	}

	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

struct q1_data {
	int64_t sum_qty;
	int64_t sum_base_price;
	int64_t sum_disc_price;
	int64_t sum_charge;
	int64_t count_order;
	int64_t sum_discount;
	friend std::ostream& operator<<( std::ostream &output, const q1_data& D);
};
std::ostream& operator<<( std::ostream &output, const q1_data& D)
{
	output << std::fixed << std::setprecision(2) << (double)D.sum_qty / 100 << "  " << \
				 std::fixed << std::setprecision(2) << (double)D.sum_base_price / 100  << " " << \
				 std::fixed << std::setprecision(4) << (double)D.sum_disc_price / 10000 << "  " << \
				 std::fixed << std::setprecision(6) << (double)D.sum_charge / 1000000 << "  " <<\
				 (double)D.sum_qty / D.count_order / 100<< "  " << (double)D.sum_base_price / D.count_order / 100 << \
				 "  " << (double)D.sum_discount / D.count_order / 100 << "  " << D.count_order;

	return output;
}

inline uint32_t reverseBits(uint32_t x) {
	x = (((x & 0xaaaaaaaa) >> 1) | ((x & 0x55555555) << 1));
	x = (((x & 0xcccccccc) >> 2) | ((x & 0x33333333) << 2));
	x = (((x & 0xf0f0f0f0) >> 4) | ((x & 0x0f0f0f0f) << 4));
	x = (((x & 0xff00ff00) >> 8) | ((x & 0x00ff00ff) << 8));
	return((x >> 16) | (x << 16));
}

unsigned char reverse_table[] = 
{
	0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0,
	0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8,
	0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
	0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC,
	0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2,
	0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
	0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6,
	0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
	0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
	0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9,
	0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
	0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
	0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3,
	0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
	0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
	0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF
};

uint8_t popcnt8[256]{
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
	};

inline void vbmi2_decoder_cvtepu16(int64_t *base_ptr, uint32_t &base,
									uint64_t idx, uint32_t bits) {

	__m512i indexes = _mm512_maskz_compress_epi16(bits, _mm512_set_epi32(
		0x001e001d, 0x001c001b, 0x001a0019, 0x00180017,
		0x00160015, 0x00140013, 0x00120011, 0x0010000f,
		0x000e000d, 0x000c000b, 0x000a0009, 0x00080007,
		0x00060005, 0x00040003, 0x00020001, 0x00000000
	));
	__m512i t0 = _mm512_cvtepu16_epi64(_mm512_castsi512_si128(indexes));
	__m512i t1 = _mm512_cvtepu16_epi64(_mm512_extracti32x4_epi32(indexes, 1));
	__m512i t2 = _mm512_cvtepu16_epi64(_mm512_extracti32x4_epi32(indexes, 2));
	__m512i t3 = _mm512_cvtepu16_epi64(_mm512_extracti32x4_epi32(indexes, 3));
	__m512i start_index = _mm512_set1_epi64(idx);

	_mm512_storeu_si512(base_ptr + base, _mm512_add_epi64(t0, start_index));
	_mm512_storeu_si512(base_ptr + base + 8, _mm512_add_epi64(t1, start_index));
	_mm512_storeu_si512(base_ptr + base + 16, _mm512_add_epi64(t2, start_index));
	_mm512_storeu_si512(base_ptr + base + 24, _mm512_add_epi64(t3, start_index));

	base += _popcnt32(bits);
}

inline void avx512_q1_compute(int64_t *quantity_ptr, int64_t *price_ptr, int64_t *discount_ptr, int64_t *tax_ptr, \
						uint16_t base, uint8_t bits, q1_data &sum) {

	if(!bits)
		return;


	__m512i indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(quantity_ptr + base));
	sum.sum_qty += _mm512_reduce_add_epi64(indexes0);

	__m512i discount = _mm512_loadu_epi64(discount_ptr + base);
	__m512i index_discount = _mm512_maskz_compress_epi64(bits, discount);
	sum.sum_discount += _mm512_reduce_add_epi64(index_discount);
	
	indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(price_ptr + base));
	sum.sum_base_price += _mm512_reduce_add_epi64(indexes0);

	__m512i indexes1 = _mm512_maskz_compress_epi64(bits, _mm512_sub_epi64(\
							_mm512_set_epi64(100, 100, 100, 100, 100, 100, 100, 100),\
								discount));
	indexes0 = _mm512_mullo_epi64(indexes0, indexes1);
	sum.sum_disc_price += _mm512_reduce_add_epi64(indexes0);

	indexes1 = _mm512_maskz_compress_epi64(bits, _mm512_add_epi64(\
					_mm512_set_epi64(100, 100, 100, 100, 100, 100, 100, 100),\
						_mm512_loadu_epi64(tax_ptr + base)));
	indexes0 = _mm512_mullo_epi64(indexes0, indexes1);
	sum.sum_charge += _mm512_reduce_add_epi64(indexes0);

}

inline void flip_bitvector(ibis::bitvector *btv) {

	ibis::bitvector::word_t *it = btv->m_vec.begin();
	while(it + 15 < btv->m_vec.end()) {
		_mm512_storeu_epi32(it, _mm512_andnot_epi32(_mm512_loadu_epi32(it), \
													_mm512_set1_epi32(0x7fffffff)));
		it += 16;
	}

	for(; it < btv->m_vec.end(); it++)
	 *it ^= ibis::bitvector::ALLONES;

	if (btv->active.nbits > 0) { // also need to toggle active_word
		btv->active.val ^= ((1 << btv->active.nbits) - 1);
	}

}


// dst/src must have 31/32 elements at least
inline void reduce_zero(uint32_t *dst, uint32_t *src) {

	__m512i mask = _mm512_set_epi8(
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3
	);

	_mm512_storeu_epi32(dst,\
	_mm512_shuffle_epi8( \
	_mm512_or_si512( \
		_mm512_sllv_epi32(_mm512_loadu_epi32(src), _mm512_set_epi32(16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)),\
		_mm512_srlv_epi32(_mm512_loadu_epi32(src + 1), _mm512_set_epi32(15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30))), mask));

	_mm512_storeu_epi32(dst + 16,\
	_mm512_shuffle_epi8( \
	_mm512_or_si512( \
		_mm512_sllv_epi32(_mm512_loadu_epi32(src + 16), _mm512_set_epi32(0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17)),\
		_mm512_srlv_epi32(_mm512_loadu_epi32(src + 17), _mm512_set_epi32(0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14))), mask));
}

void GetRowids(ibis::bitvector &btv_res, vector<row_t> *row_ids) {
	row_ids->resize(btv_res.count() + 64);
	auto element_ptr = &(*row_ids)[0];

	uint32_t ids_count = 0;
	uint64_t ids_idx = 0;
	// traverse m_vec
	auto it = btv_res.m_vec.begin();
	while(it != btv_res.m_vec.end()) {
		vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, reverseBits(*it));
		ids_idx += 31;
		it++;
	}

	// active word
	vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, \
							reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

	row_ids->resize(btv_res.count());

	std::cout << "fetch rows : " << row_ids->size() << std::endl;
}

bool q1_using_bitmap = 1;
bool q1_using_idlist = 0;
void PhysicalTableScan::TPCH_Q1(ExecutionContext &context) const {
	auto s0 = std::chrono::high_resolution_clock::now();

	auto &lineitem_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "lineitem", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	
	// 1998-09-02
	int right_days = 10471;

	std::map<std::pair<char,char>, q1_data> q1_ans;
	if(!q1_using_bitmap) {
		{
			long long time1 = 0;
			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, *this);
			vector<storage_t> storage_column_ids;
			storage_column_ids.push_back(uint64_t(8));
			storage_column_ids.push_back(uint64_t(9));
			storage_column_ids.push_back(uint64_t(4));
			storage_column_ids.push_back(uint64_t(5));
			storage_column_ids.push_back(uint64_t(6));
			storage_column_ids.push_back(uint64_t(7));
			storage_column_ids.push_back(uint64_t(10));
			lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[8]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[9]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

			while(true) {
				DataChunk result;
				result.Initialize(context.client, types);
				lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
				if(result.size() == 0)
					break;



				auto &returnflag = result.data[0];
				auto &linestatus = result.data[1];
				auto &quantity = result.data[2];
				auto &extendedprice = result.data[3];
				auto &discount = result.data[4];
				auto &tax = result.data[5];
				auto &days = result.data[6];

				auto quantity_data = FlatVector::GetData<int64_t>(quantity);
				auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);
				auto tax_data = FlatVector::GetData<int64_t>(tax);
				auto days_data = FlatVector::GetData<int32_t>(days);

				if(result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
					auto &returnflag_sel_vector = DictionaryVector::SelVector(result.data[0]);
					auto &returnflag_child = DictionaryVector::Child(result.data[0]);
					auto &linestatus_sel_vector = DictionaryVector::SelVector(result.data[1]);
					auto &linestatus_child = DictionaryVector::Child(result.data[1]);
					

					auto st1 = std::chrono::high_resolution_clock::now();
					
					for(int i = 0; i < result.size(); i++) {

						if(days_data[i] > right_days)
							continue;

						auto &it = q1_ans[{reinterpret_cast<string_t *>(returnflag_child.GetData())[returnflag_sel_vector.get_index(i)].GetData()[0],\
								reinterpret_cast<string_t *>(linestatus_child.GetData())[linestatus_sel_vector.get_index(i)].GetData()[0]}];

						it.sum_qty += quantity_data[i];
						it.sum_discount += discount_data[i];
						it.sum_base_price += extendedprice_data[i];
						it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
						it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
						it.count_order += 1;
					}
					
					auto et1 = std::chrono::high_resolution_clock::now();
					time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
				}
				else {
					auto st1 = std::chrono::high_resolution_clock::now();
					
					for(int i = 0; i < result.size(); i++) {

						if(days_data[i] > right_days)
							continue;

						auto &it = q1_ans[{reinterpret_cast<string_t *>(returnflag.GetData())[i].GetData()[0], \
							reinterpret_cast<string_t *>(linestatus.GetData())[i].GetData()[0]}];

						it.sum_qty += quantity_data[i];
						it.sum_discount += discount_data[i];
						it.sum_base_price += extendedprice_data[i];
						it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
						it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
						it.count_order += 1;
					}
					
					auto et1 = std::chrono::high_resolution_clock::now();
					time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
				}
			}
			std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
		}

	}
	else {
		if(q1_using_idlist)
		{
			long long time1 = 0;
			long long timeb = 0;
			long long timeids = 0;
			auto cubit_linestatus = dynamic_cast<cubit::Cubit *>(context.client.bitmap_linestatus);
			auto cubit_returnflag = dynamic_cast<cubit::Cubit *>(context.client.bitmap_returnflag);
			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, *this);
			vector<storage_t> storage_column_ids;
			storage_column_ids.push_back(uint64_t(4));
			storage_column_ids.push_back(uint64_t(5));
			storage_column_ids.push_back(uint64_t(6));
			storage_column_ids.push_back(uint64_t(7));
			storage_column_ids.push_back(uint64_t(10));
			lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

			std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
			std::map<int, char> returnflag_map;
			std::map<int, char> linestatus_map;
			returnflag_map[0] = 'N';
			returnflag_map[1] = 'R';
			returnflag_map[2] = 'A';
			linestatus_map[0] = 'O';
			linestatus_map[1] = 'F';

			auto stb = std::chrono::high_resolution_clock::now();
			for(int i = 0; i < cubit_linestatus->config->g_cardinality; i++) {
				for(int r = 0; r < cubit_returnflag->config->g_cardinality; r++) {
					ibis::bitvector btv_res;
					btv_res.copy(*cubit_returnflag->bitmaps[r]->btv);
					btv_res &= *cubit_linestatus->bitmaps[i]->btv;

					if(!btv_res.count())
						continue;

					auto &ids = ids_map[{returnflag_map[r], linestatus_map[i]}].first;

					// ids.reserve(btv_res.count());
					// auto stids = std::chrono::high_resolution_clock::now();
					// for (ibis::bitvector::indexSet index_set = btv_res.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
					// 	const ibis::bitvector::word_t *indices = index_set.indices();
					// 	if (index_set.isRange()) {
					// 		for (ibis::bitvector::word_t j = *indices; j < indices[1]; ++j) {
					// 			ids.push_back((uint64_t)j);
					// 		}
					// 	} else {
					// 		for (unsigned j = 0; j < index_set.nIndices(); ++j) {
					// 			ids.push_back((uint64_t)indices[j]);
					// 		}
					// 	}
					// }
					// auto etids = std::chrono::high_resolution_clock::now();

					auto stids = std::chrono::high_resolution_clock::now();

					ids.resize(btv_res.count() + 64);
					// ids[btv_res.count()] = 999999999;
					auto element_ptr = &ids[0];

					uint32_t ids_count = 0;
					uint64_t ids_idx = 0;
					// traverse m_vec
					auto it = btv_res.m_vec.begin();
					while(it != btv_res.m_vec.end()) {
						vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, reverseBits(*it));
						ids_idx += 31;
						it++;
					}

					// active word
					vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, \
											reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

					ids.resize(btv_res.count());


					auto etids = std::chrono::high_resolution_clock::now();


					timeids += std::chrono::duration_cast<std::chrono::nanoseconds>(etids - stids).count();

				}
			}
			auto etb = std::chrono::high_resolution_clock::now();
			timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();


			int64_t cursor = 0;
			int64_t offset = 0;
			while(true) {
				DataChunk result;
				result.Initialize(context.client, types);
				// lineitem_scan_state.table_state.Scan(lineitem_transaction, result);
				lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
				if(result.size() == 0)
					break;
				
				offset = cursor;
				cursor += result.size();



				auto &quantity = result.data[0];
				auto &extendedprice = result.data[1];
				auto &discount = result.data[2];
				auto &tax = result.data[3];
				auto &days = result.data[4];

				auto quantity_data = FlatVector::GetData<int64_t>(quantity);
				auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);
				auto tax_data = FlatVector::GetData<int64_t>(tax);
				auto days_data = FlatVector::GetData<int32_t>(days);


				auto st1 = std::chrono::high_resolution_clock::now();

				for(auto &ids_it : ids_map) {
					auto &it = q1_ans[ids_it.first];
					auto &ids = ids_it.second.first;
					auto &c_row = ids_it.second.second;
					while(c_row < ids.size() && ids[c_row] < cursor) {
						uint64_t i = ids[c_row++] - offset;

						if(days_data[i] > right_days)
							continue;

						it.sum_qty += quantity_data[i];
						it.sum_discount += discount_data[i];
						it.sum_base_price += extendedprice_data[i];
						it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
						it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
						it.count_order += 1;
					}
				}
				
				auto et1 = std::chrono::high_resolution_clock::now();
				time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
			
			}
			std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
			std::cout << "ids time : "<< timeids/1000000 << "ms" << std::endl;
			std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
		}
		else
		{
			long long time1 = 0;
			long long timeb = 0;
			long long timeids = 0;
			auto cubit_linestatus = dynamic_cast<cubit::Cubit *>(context.client.bitmap_linestatus);
			auto cubit_returnflag = dynamic_cast<cubit::Cubit *>(context.client.bitmap_returnflag);
			auto cubit_shipdate = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipdate);

			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, *this);
			vector<storage_t> storage_column_ids;
			storage_column_ids.push_back(uint64_t(4));
			storage_column_ids.push_back(uint64_t(5));
			storage_column_ids.push_back(uint64_t(6));
			storage_column_ids.push_back(uint64_t(7));
			storage_column_ids.push_back(uint64_t(10));
			lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

			std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
			std::map<std::pair<char, char>, std::pair<vector<uint32_t>*, uint8_t *> > cubit_res_map;
			std::map<int, char> returnflag_map;
			std::map<int, char> linestatus_map;
			returnflag_map[0] = 'N';
			returnflag_map[1] = 'R';
			returnflag_map[2] = 'A';
			linestatus_map[0] = 'O';
			linestatus_map[1] = 'F';

			auto stb = std::chrono::high_resolution_clock::now();

			ibis::bitvector shipdate_res;
			shipdate_res.copy(*cubit_shipdate->bitmaps[cubit_shipdate->config->g_cardinality - 1]->btv);
			shipdate_res.decompress();
			for(int i = cubit_shipdate->config->g_cardinality - 2; i > right_days; i--)
				shipdate_res |= *cubit_shipdate->bitmaps[i]->btv;

			flip_bitvector(&shipdate_res);

			for(int i = 0; i < cubit_linestatus->config->g_cardinality; i++) {
				for(int r = 0; r < cubit_returnflag->config->g_cardinality; r++) {

					ibis::bitvector ttt_res;
					ttt_res.copy(*cubit_linestatus->bitmaps[i]->btv);
					ttt_res &= *cubit_returnflag->bitmaps[r]->btv;
					ttt_res &= shipdate_res;

					uint64_t count = ttt_res.count();

					if(!count) {
						continue;
					}

					vector<uint32_t> *btv_res = new vector<uint32_t>(ttt_res.size() / 32 + (ttt_res.size() % 32?1:0) + 4);

					uint32_t *dst_data = (uint32_t *)&(*btv_res)[0];
					uint32_t *src_data = ttt_res.m_vec.begin();

					// load 31 dst once
					while(src_data + 31 < ttt_res.m_vec.end()) {
						reduce_zero(dst_data, src_data);
						dst_data += 31;
						src_data += 32;
					}

					// 1 dst once
					int need_bits = 31;
					while(src_data + 1 < ttt_res.m_vec.end()) {
						dst_data[0] = ((src_data[0] << (32 - need_bits)) | (src_data[1] >> (need_bits - 1)));
						dst_data[0] = htonl(dst_data[0]);
						need_bits--;
						src_data++;
						dst_data++;
					}

					// active word
					dst_data[0] = src_data[0] << (32 - need_bits);
					uint32_t last_word = ttt_res.active.val << (32 - ttt_res.active.nbits);
					if(ttt_res.active.nbits + need_bits > 32) {
						dst_data[0] |= (last_word >> need_bits);
						dst_data[0] = htonl(dst_data[0]);
						last_word <<= (32 - need_bits);
						dst_data[1] = last_word;
						dst_data[1] = htonl(dst_data[1]);
					}
					else {
						dst_data[0] |= (last_word >> need_bits);
						dst_data[0] = htonl(dst_data[0]);
					}
					

					cubit_res_map[{returnflag_map[r], linestatus_map[i]}].first = btv_res;
					cubit_res_map[{returnflag_map[r], linestatus_map[i]}].second = (uint8_t *)&((*btv_res)[0]);
					q1_ans[{returnflag_map[r], linestatus_map[i]}].count_order = count;

				}
			}
			auto etb = std::chrono::high_resolution_clock::now();
			timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();


			int64_t cursor = 0;
			int64_t offset = 0;
			while(true) {
				DataChunk result;
				result.Initialize(context.client, types);
				lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
				if(result.size() == 0)
					break;
				
				offset = cursor;
				cursor += result.size();



				auto &quantity = result.data[0];
				auto &extendedprice = result.data[1];
				auto &discount = result.data[2];
				auto &tax = result.data[3];
				auto &days = result.data[4];

				auto quantity_data = FlatVector::GetData<int64_t>(quantity);
				auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);
				auto tax_data = FlatVector::GetData<int64_t>(tax);
				auto days_data = FlatVector::GetData<int32_t>(days);


				auto st1 = std::chrono::high_resolution_clock::now();

				for(auto &ids_it : cubit_res_map) {
					auto &btv_it = ids_it.second.second;
					auto &ans_it = q1_ans[ids_it.first];
					uint16_t base = 0;
					while(base + 7 < result.size() ) {
						avx512_q1_compute(quantity_data, extendedprice_data, discount_data, tax_data,\
											base, reverse_table[*btv_it], q1_ans[ids_it.first]);
						btv_it++;
						base += 8;
					}
					if(base < result.size()) {
						uint8_t bits = *btv_it;
						while(base < result.size()) {
							if( bits & 0x80 ) {
								ans_it.sum_qty += quantity_data[base];
								ans_it.sum_discount += discount_data[base];
								ans_it.sum_base_price += extendedprice_data[base];
								ans_it.sum_disc_price += extendedprice_data[base]*(100 - discount_data[base]);
								ans_it.sum_charge += extendedprice_data[base]*(100 - discount_data[base]) *(100 + tax_data[base]);
							}
							base++;
							bits <<= 1;
						}
					}
				}
				
				auto et1 = std::chrono::high_resolution_clock::now();
				time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
			
			}
			std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
			std::cout << "ids time : "<< timeids/1000000 << "ms" << std::endl;
			std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
		}
	}

	auto s1 = std::chrono::high_resolution_clock::now();

	for(auto &it : q1_ans) {
		std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
	}

	std::cout << "q1 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

	return;

}

struct q3_topk {
	bool operator ()(std::pair<int64_t, int64_t> &a, std::pair<int64_t, int64_t> &b) {
		return a.second > b.second;
	}
};

bool q3_using_bitmap = 0;
void PhysicalTableScan::TPCH_Q3(ExecutionContext &context) const {
	auto s0 = std::chrono::high_resolution_clock::now();
	auto &customer_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "customer", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &lineitem_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "lineitem", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &orders_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "orders", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	
	long long time_fetch = 0;
	long long time_ids = 0;
	long long time_bitmap = 0;

	char *msg = "BUILDING";
	int32_t filter_days = 9204;

	auto s1 = std::chrono::high_resolution_clock::now();

	std::bitset<1500001> custkey_b;
	{
		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
		TableScanState customer_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(6));
		customer_table.GetStorage().InitializeScan(customer_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(customer_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(customer_table.GetColumns().GetColumnTypes()[6]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
			if(result.size() == 0)
				break;

			auto &customer_key = result.data[0];
			auto &mktsegment = result.data[1];
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);

			if(result.data[1].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &mktsegment_sel_vector = DictionaryVector::SelVector(result.data[1]);
				auto &mktsegment_child = DictionaryVector::Child(result.data[1]);

				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(mktsegment_child.GetData())[mktsegment_sel_vector.get_index(i)].GetData(), msg))
						custkey_b[customer_key_data[i]] = 1;
				}
			}
			else {
				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(mktsegment.GetData())[i].GetData(), msg))
						custkey_b[customer_key_data[i]] = 1;
				}
			}
		}
	}

	auto s2 = std::chrono::high_resolution_clock::now();

	std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orderkey_map;
	// vector<std::pair<int32_t, int32_t>*> orderkey_map_vec(60000001);
	uint64_t tmp = 0;

	ibis::bitvector btv_res;
	auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_orderkey);
	btv_res.adjustSize(0, cubit_orderkey->config->n_rows);
	btv_res.decompress();

	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(1));
		storage_column_ids.push_back(uint64_t(4));
		storage_column_ids.push_back(uint64_t(7));
		orders_table.GetStorage().InitializeScan(orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[7]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto &order_key = result.data[0];
			auto &customer_key = result.data[1];
			auto &orderdate = result.data[2];
			auto &shippriority = result.data[3];
			auto order_key_data = FlatVector::GetData<int64_t>(order_key);
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);
			auto orderdate_data = FlatVector::GetData<int32_t>(orderdate);
			auto shippriority_data = FlatVector::GetData<int32_t>(shippriority);

			for(int i = 0; i < result.size(); i++) {
				// if(orderdate_data[i] < filter_days && custkey_set.count(customer_key_data[i]))
				if(orderdate_data[i] < filter_days && custkey_b[customer_key_data[i]]) {
				// if(orderdate_data[i] < filter_days)
					// orderkey_map[order_key_data[i]] = {orderdate_data[i], shippriority_data[i]};
					orderkey_map.emplace(order_key_data[i], std::make_pair(orderdate_data[i], shippriority_data[0])); 
					// orderkey_map_vec[order_key_data[i]] = new std::pair(orderdate_data[i], shippriority_data[0]);
					// orderkey_vec.emplace_back(order_key_data[i]);
					btv_res |= *cubit_orderkey->bitmaps[order_key_data[i]]->btv;
				}
			}
		}
	}

	auto s3 = std::chrono::high_resolution_clock::now();

	std::unordered_map<int64_t, int64_t> l_orderkey_map;

	{
		auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_orderkey);
		auto cubit_shipdate = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipdate);
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(5));
		storage_column_ids.push_back(uint64_t(6));
		lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);


		vector<row_t> *ids = new vector<row_t>;
		size_t cursor = 0;

		auto st_bitmap = std::chrono::high_resolution_clock::now();

		ibis::bitvector btv_shipdate;
		btv_shipdate.copy(*cubit_shipdate->bitmaps[filter_days + 1]->btv);
		btv_shipdate.decompress();

		for(uint32_t i = filter_days + 2; i < cubit_shipdate->config->g_cardinality; i++) {
			btv_shipdate |= *cubit_shipdate->bitmaps[i]->btv;
		}

		btv_res &= btv_shipdate;

		auto et_bitmap = std::chrono::high_resolution_clock::now();
		time_bitmap += std::chrono::duration_cast<std::chrono::nanoseconds>(et_bitmap - st_bitmap).count();

		auto st_ids = std::chrono::high_resolution_clock::now();

		for (ibis::bitvector::indexSet index_set = btv_res.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
			const ibis::bitvector::word_t *indices = index_set.indices();
			if (index_set.isRange()) {
				for (ibis::bitvector::word_t j = *indices; j < indices[1]; ++j) {
					ids->push_back((uint64_t)j);
				}
			} else {
				for (unsigned j = 0; j < index_set.nIndices(); ++j) {
					ids->push_back((uint64_t)indices[j]);
				}
			}
		}

		auto et_ids = std::chrono::high_resolution_clock::now();
		time_ids += std::chrono::duration_cast<std::chrono::nanoseconds>(et_ids - st_ids).count();

		while(true) {
			auto st_fetch = std::chrono::high_resolution_clock::now();
			
			DataChunk result;
			result.Initialize(context.client, types);

			if(cursor < ids->size()) {
				ColumnFetchState column_fetch_state;
				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*ids)[cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(cursor + fetch_count > ids->size()) {
					fetch_count = ids->size() - cursor;
				}
				lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);

				cursor += fetch_count;
			}
			else {
				delete ids;
				break;
			}
			auto et_fetch = std::chrono::high_resolution_clock::now();
			time_fetch += std::chrono::duration_cast<std::chrono::nanoseconds>(et_fetch - st_fetch).count();

			auto &order_key = result.data[0];
			auto &extendedprice = result.data[1];
			auto &discount = result.data[2];
			auto order_key_data = FlatVector::GetData<int64_t>(order_key);
			auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
			auto discount_data = FlatVector::GetData<int64_t>(discount);

			for(int i = 0; i < result.size(); i++) {
				l_orderkey_map[order_key_data[i]] += extendedprice_data[i] * (100 - discount_data[i]);
			}
		}
	}

	std::priority_queue<std::pair<int64_t, int64_t>,vector<std::pair<int64_t, int64_t>>,q3_topk> minHeap;
	int heapc = 10;

	for(auto &it : l_orderkey_map) {
		if(minHeap.size() < heapc)
			minHeap.push(it);
		else {
			if(it.second <= minHeap.top().second)
				continue;
			minHeap.pop();
			minHeap.push(it);
		}
	}

	while(!minHeap.empty()) {
		std::cout << minHeap.top().first << " : " << minHeap.top().second << " : " << \
		 orderkey_map[minHeap.top().first].first << " : " << orderkey_map[minHeap.top().first].second << std::endl;

		// std::cout << minHeap.top().first << " : " << minHeap.top().second << " : " << std::endl;


		// std::cout << minHeap.top().first << " : " << minHeap.top().second << " : " << \
		//  orderkey_map_vec[minHeap.top().first]->first << " : " << orderkey_map_vec[minHeap.top().first]->second << std::endl;
		minHeap.pop();
	}

	auto s4 = std::chrono::high_resolution_clock::now();

	// std::cout << "customer num : " << custkey_b.count() << std::endl;
	// std::cout << "orders num : " << orderkey_map.size() << std::endl;
	// std::cout << "orders num : " << orderkey_set.size() << std::endl;
	// std::cout << "lineitem num : " << l_orderkey_map.size() << std::endl;
	// std::cout << tmp << std::endl;

	std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s2).count() << "ms" << std::endl;
	std::cout << "bitmap time : " << time_bitmap / 1000000 << "ms" << std::endl;
	std::cout << "ids time : " << time_ids / 1000000 << "ms" << std::endl;
	std::cout << "fetch time : " << time_fetch / 1000000 << "ms" << std::endl;
	std::cout << "q3 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s4 - s0).count() << "ms" << std::endl;


}

void PhysicalTableScan::TPCH_Q4(ExecutionContext &context) const {
	
	auto s0 = std::chrono::high_resolution_clock::now();

	auto &orders_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "orders", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &lineitem_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "lineitem", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();

	int32_t left_days = 8582;
	int32_t right_days = 8673;

	std::unordered_map<int32_t, std::string> priority_map;
	std::unordered_map<std::string, int32_t> sti_map;
	priority_map[5] = "5-LOW";
	priority_map[1] = "1-URGENT";
	priority_map[4] = "4-NOT SPECIFIED";
	priority_map[2] = "2-HIGH";
	priority_map[3] = "3-MEDIUM";
	sti_map["5-LOW"] = 5;
	sti_map["1-URGENT"] = 1;
	sti_map["4-NOT SPECIFIED"] = 4;
	sti_map["2-HIGH"] = 2;
	sti_map["3-MEDIUM"] = 3;


	std::unordered_map<int64_t, int32_t> orderkey_map;
	orderkey_map.reserve(580000);
	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(4));
		storage_column_ids.push_back(uint64_t(5));
		orders_table.GetStorage().InitializeScan(orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[5]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto order_key_data = FlatVector::GetData<int64_t>(result.data[0]);
			auto date_data = FlatVector::GetData<int32_t>(result.data[1]);

			if(result.data[2].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &sel_vec = DictionaryVector::SelVector(result.data[2]);
				auto &child_vec = DictionaryVector::Child(result.data[2]);
				for(int i = 0; i < result.size(); i++) {
					if(date_data[i] >= left_days && date_data[i] <= right_days)
						orderkey_map[order_key_data[i]] = sti_map[reinterpret_cast<string_t *>(child_vec.GetData())[sel_vec.get_index(i)].GetString()];
				}
			}
			else {
				for(int i = 0; i < result.size(); i++) {
					if(date_data[i] >= left_days && date_data[i] <= right_days)
						orderkey_map[order_key_data[i]] = sti_map[reinterpret_cast<string_t *>(result.data[2].GetData())[i].GetString()];
				}
			}
		}
	}

	auto s1 = std::chrono::high_resolution_clock::now();

	long long time_ids = 0;
	long long time_fetch = 0;
	long long time_bitmap = 0;
	std::unordered_set<int64_t> orderkey_set;
	orderkey_set.reserve(570000);
	{
		auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_orderkey);
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(11));
		storage_column_ids.push_back(uint64_t(12));
		lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[11]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[12]);

		auto s_bitmap = std::chrono::high_resolution_clock::now();
		ibis::bitvector btv_res;
		auto it1 = orderkey_map.begin();
		btv_res.copy(*cubit_orderkey->bitmaps[it1->first]->btv);
		btv_res.decompress();
		it1++;
		while(it1 != orderkey_map.end()) {
			btv_res |= *cubit_orderkey->bitmaps[it1->first]->btv;
			it1++;
		}

		auto s_ids = std::chrono::high_resolution_clock::now();

		time_bitmap = std::chrono::duration_cast<std::chrono::milliseconds>(s_ids - s_bitmap).count();

		vector<row_t> *row_ids = new vector<row_t>;
		size_t cursor = 0;

		row_ids->resize(btv_res.count() + 64);
		// ids[btv_res.count()] = 999999999;
		auto element_ptr = &(*row_ids)[0];

		uint32_t ids_count = 0;
		uint64_t ids_idx = 0;
		// traverse m_vec
		auto it = btv_res.m_vec.begin();
		while(it != btv_res.m_vec.end()) {
			vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, reverseBits(*it));
			ids_idx += 31;
			it++;
		}

		// active word
		vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, \
								reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

		row_ids->resize(btv_res.count());

		auto e_ids = std::chrono::high_resolution_clock::now();
		time_ids = std::chrono::duration_cast<std::chrono::milliseconds>(e_ids - s_ids).count();

		while(true) {
			auto s_fetch = std::chrono::high_resolution_clock::now();
			DataChunk result;
			result.Initialize(context.client, types);

			if(cursor < row_ids->size()) {
				ColumnFetchState column_fetch_state;
				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*row_ids)[cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(cursor + fetch_count > row_ids->size()) {
					fetch_count = row_ids->size() - cursor;
				}
				lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);

				cursor += fetch_count;
			}
			else {
				delete row_ids;
				break;
			}
			auto e_fetch = std::chrono::high_resolution_clock::now();
			time_fetch += std::chrono::duration_cast<std::chrono::nanoseconds>(e_fetch - s_fetch).count();

			auto order_key_data = FlatVector::GetData<int64_t>(result.data[0]);
			auto commitdate_data = FlatVector::GetData<int32_t>(result.data[1]);
			auto receiptdate_data = FlatVector::GetData<int32_t>(result.data[2]);

			auto s_c = std::chrono::high_resolution_clock::now();

			for(int i = 0; i < result.size(); i++) {
				if(commitdate_data[i] < receiptdate_data[i]) {
					orderkey_set.insert(order_key_data[i]);
				}
			}
		}
	}
	
	auto s2 = std::chrono::high_resolution_clock::now();

	std::map<int32_t, int32_t> q4_ans;
	{
		for(auto orderkey : orderkey_set) {
			q4_ans[orderkey_map[orderkey]]++;
		}
	}
	
	auto s3 = std::chrono::high_resolution_clock::now();

	for(auto &it : q4_ans) {
		std::cout << priority_map[it.first] << " : " << it.second << std::endl;
	} 

	std::cout << "fetch time : " << time_fetch / 1000000 << "ms" << std::endl;
	std::cout << "ids time : " << time_ids / 1000000 << "ms" << std::endl;
	std::cout << "bitmap time : " << time_bitmap << "ms" << std::endl;
	std::cout << "q4 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s0).count() << "ms" << std::endl;
}

bool q5_using_bitmap = 1;
void PhysicalTableScan::TPCH_Q5(ExecutionContext &context) const {
	auto &nation_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "nation", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &customer_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "customer", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &lineitem_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "lineitem", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &supplier_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "supplier", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &region_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "region", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &orders_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "orders", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();

	auto s1 = std::chrono::high_resolution_clock::now();
	
	std::unordered_set<int32_t> region_set;
	{
		auto &region_transaction = DuckTransaction::Get(context.client, region_table.catalog);
		TableScanState region_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(1));
		region_table.GetStorage().InitializeScan(region_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(LogicalType(LogicalTypeId::INTEGER));
		types.push_back(LogicalType(LogicalTypeId::VARCHAR));
		while(true) {
			// TODO: read name actually
			DataChunk result;
			result.Initialize(context.client, types);
			region_table.GetStorage().Scan(region_transaction, result, region_scan_state);
			if(result.size() == 0)
				break;
			region_set.insert(2);
		}
	}

	auto s2 = std::chrono::high_resolution_clock::now();
	
	std::unordered_set<int32_t> nation_set;
	{
		auto &nation_transaction = DuckTransaction::Get(context.client, nation_table.catalog);
		TableScanState nation_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(2));
		nation_table.GetStorage().InitializeScan(nation_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(LogicalType(LogicalTypeId::INTEGER));
		types.push_back(LogicalType(LogicalTypeId::INTEGER));
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			nation_table.GetStorage().Scan(nation_transaction, result, nation_scan_state);
			if(result.size() == 0)
				break;

			auto &nation_key = result.data[0];
			auto &region_key = result.data[1];
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);
			auto region_key_data = FlatVector::GetData<int32_t>(region_key);

			for(int i = 0; i < result.size(); i++) {
				if(region_set.count(region_key_data[i]))
					nation_set.insert(nation_key_data[i]);
			}
		}
	}

	auto s3 = std::chrono::high_resolution_clock::now();

	std::unordered_map<int64_t, int32_t> customer_nation_map;
	{
		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
		TableScanState customer_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(3));
		customer_table.GetStorage().InitializeScan(customer_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(customer_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(customer_table.GetColumns().GetColumnTypes()[3]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
			if(result.size() == 0)
				break;

			auto &customer_key = result.data[0];
			auto &nation_key = result.data[1];
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

			for(int i = 0; i < result.size(); i++) {
				if(nation_set.count(nation_key_data[i]))
					customer_nation_map[customer_key_data[i]] = nation_key_data[i];
			}
		}
	}

	auto s4 = std::chrono::high_resolution_clock::now();

	std::unordered_map<int64_t, int32_t> order_nation_map;
	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(1));
		storage_column_ids.push_back(uint64_t(4));
		orders_table.GetStorage().InitializeScan(orders_transaction, orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto &orders_key = result.data[0];
			auto &customer_key = result.data[1];
			auto &days = result.data[2];
			auto orders_key_data = FlatVector::GetData<int64_t>(orders_key);
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);
			auto days_data = FlatVector::GetData<int32_t>(days);

			for(int i = 0; i < result.size(); i++) {
				if(days_data[i] < 8766 || days_data[i] >= 9131)
					continue;
				if(customer_nation_map.count(customer_key_data[i]))
					order_nation_map[orders_key_data[i]] = customer_nation_map[customer_key_data[i]];
			}
		}
	}


	auto s5 = std::chrono::high_resolution_clock::now();

	std::unordered_map<int64_t, int32_t> supp_nation_map;
	{
		auto &supplier_transaction = DuckTransaction::Get(context.client, supplier_table.catalog);
		TableScanState supplier_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(3));
		supplier_table.GetStorage().InitializeScan(supplier_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(supplier_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(supplier_table.GetColumns().GetColumnTypes()[3]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			supplier_table.GetStorage().Scan(supplier_transaction, result, supplier_scan_state);
			if(result.size() == 0)
				break;

			auto &supp_key = result.data[0];
			auto &nation_key = result.data[1];
			auto supp_key_data = FlatVector::GetData<int64_t>(supp_key);
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

			for(int i = 0; i < result.size(); i++) {
				if(nation_set.count(nation_key_data[i])) {
					supp_nation_map[supp_key_data[i]] = nation_key_data[i];
				}
			}
		}
	}

	auto s6 = std::chrono::high_resolution_clock::now();

	std::unordered_map<int32_t, int64_t> q5_ans;
	if(!q5_using_bitmap) {
		{
			long long time1 = 0;
			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, *this);
			vector<storage_t> storage_column_ids;
			storage_column_ids.push_back(uint64_t(0));
			storage_column_ids.push_back(uint64_t(2));
			storage_column_ids.push_back(uint64_t(5));
			storage_column_ids.push_back(uint64_t(6));
			lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[2]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);

			while(true) {
				DataChunk result;
				result.Initialize(context.client, types);
				// lineitem_scan_state.table_state.Scan(lineitem_transaction, result);
				lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
				if(result.size() == 0)
					break;

				auto &order_key = result.data[0];
				auto &supp_key = result.data[1];
				auto &extenedprice = result.data[2];
				auto &discount = result.data[3];
				auto order_key_data = FlatVector::GetData<int64_t>(order_key);
				auto supp_key_data = FlatVector::GetData<int64_t>(supp_key);
				auto extenedprice_data = FlatVector::GetData<int64_t>(extenedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);

				auto st1 = std::chrono::high_resolution_clock::now();
				for(int i = 0; i < result.size(); i++) {
					if(order_nation_map.count(order_key_data[i]) && supp_nation_map.count(supp_key_data[i]) && order_nation_map[order_key_data[i]] == supp_nation_map[supp_key_data[i]]) {
						q5_ans[supp_nation_map[supp_key_data[i]]] += extenedprice_data[i] * (100 - discount_data[i]);
					}
				}
				auto et1 = std::chrono::high_resolution_clock::now();
				time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
			}
			std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
		}
	}
	else {
		{
			long long time_compute = 0;
			long long time_fetch = 0;
			long long time_ids = 0;
			auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_orderkey);
			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, *this);
			vector<storage_t> storage_column_ids;
			storage_column_ids.push_back(uint64_t(0));
			storage_column_ids.push_back(uint64_t(2));
			storage_column_ids.push_back(uint64_t(5));
			storage_column_ids.push_back(uint64_t(6));
			lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[2]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);

			vector<row_t> *ids = new vector<row_t>;
			size_t cursor = 0;

			ibis::bitvector btv_res;

			auto it = order_nation_map.begin();
			
			// int bitmap_i = 0;

			btv_res.copy(*cubit_orderkey->bitmaps[it->first]->btv);
			btv_res.decompress();
			it++;

			while(it != order_nation_map.end()) {
				btv_res |= *cubit_orderkey->bitmaps[it->first]->btv;
				it++;
			}

			auto st_ids = std::chrono::high_resolution_clock::now();

			for (ibis::bitvector::indexSet index_set = btv_res.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
				const ibis::bitvector::word_t *indices = index_set.indices();
				if (index_set.isRange()) {
					for (ibis::bitvector::word_t j = *indices; j < indices[1]; ++j) {
						ids->push_back((uint64_t)j);
					}
				} else {
					for (unsigned j = 0; j < index_set.nIndices(); ++j) {
						ids->push_back((uint64_t)indices[j]);
					}
				}
			}

			auto et_ids = std::chrono::high_resolution_clock::now();
			time_ids += std::chrono::duration_cast<std::chrono::nanoseconds>(et_ids - st_ids).count();

			while(true) {
				auto st_fetch = std::chrono::high_resolution_clock::now();
				
				DataChunk result;
				result.Initialize(context.client, types);

				if(cursor < ids->size()) {
					ColumnFetchState column_fetch_state;
					data_ptr_t row_ids_data = nullptr;
					row_ids_data = (data_ptr_t)&((*ids)[cursor]);
					Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
					idx_t fetch_count = 2048;
					if(cursor + fetch_count > ids->size()) {
						fetch_count = ids->size() - cursor;
					}
					lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
															column_fetch_state);

					cursor += fetch_count;
				}
				else {
					delete ids;
					break;
				}
				auto et_fetch = std::chrono::high_resolution_clock::now();
				time_fetch += std::chrono::duration_cast<std::chrono::nanoseconds>(et_fetch - st_fetch).count();

				auto &order_key = result.data[0];
				auto &supp_key = result.data[1];
				auto &extenedprice = result.data[2];
				auto &discount = result.data[3];
				auto order_key_data = FlatVector::GetData<int64_t>(order_key);
				auto supp_key_data = FlatVector::GetData<int64_t>(supp_key);
				auto extenedprice_data = FlatVector::GetData<int64_t>(extenedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);

				auto st_compute = std::chrono::high_resolution_clock::now();
				for(int i = 0; i < result.size(); i++) {
					if(order_nation_map[order_key_data[i]] == supp_nation_map[supp_key_data[i]])
						q5_ans[supp_nation_map[supp_key_data[i]]] += extenedprice_data[i] * (100 - discount_data[i]);
				}
				auto et_compute = std::chrono::high_resolution_clock::now();
				time_compute += std::chrono::duration_cast<std::chrono::nanoseconds>(et_compute - st_compute).count();
			}

			std::cout << "ids time : "<< time_ids/1000000 << "ms" << std::endl;
			std::cout << "fetch time : "<< time_fetch/1000000 << "ms" << std::endl;
			std::cout << "compute time : "<< time_compute/1000000 << "ms" << std::endl;

		}
	}

	std::vector<std::pair<uint64_t, uint32_t> > output;
	for(auto &it : q5_ans) {
		output.push_back({it.second, it.first});
	}
	std::sort(output.begin(), output.end(), [](std::pair<uint64_t, uint32_t> &a, std::pair<uint64_t, uint32_t> &b) {return a.first > b.first;});
	for(auto &p : output) {
		std::cout << p.second << " : " << std::fixed << std::setprecision(4) << ((double)p.first) / 10000 << std::endl;
	}


	auto s7 = std::chrono::high_resolution_clock::now();

	// std::cout << "--------------------------------" << std::endl;
	// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count() << "ms" << std::endl;
	// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s2).count() << "ms" << std::endl;
	// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(s4 - s3).count() << "ms" << std::endl;
	// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(s5 - s4).count() << "ms" << std::endl;
	// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(s6 - s5).count() << "ms" << std::endl;
	// std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(s7 - s6).count() << "ms" << std::endl;
	// std::cout << "--------------------------------" << std::endl;
	return;

}

void PhysicalTableScan::TPCH_Q6_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids) const {
	auto cubit_shipdate = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipdate);
	auto cubit_discount = dynamic_cast<cubit::Cubit *>(context.client.bitmap_discount);
	auto cubit_quantity = dynamic_cast<cubit::Cubit *>(context.client.bitmap_quantity);

	// '1994-01-01'
	int lower_days = 8766;

	// '1995-01-01' - 1
	int upper_days = 9130;

	int lower_discount = 5;
	int upper_discount = 7;

	int upper_quantity = 23;

	auto s0 = std::chrono::high_resolution_clock::now();

	ibis::bitvector btv_shipdate;
	btv_shipdate.copy(*cubit_shipdate->bitmaps[lower_days]->btv);
	btv_shipdate.decompress();

	for(int i = lower_days + 1; i <= upper_days; i++) {
		btv_shipdate |= *cubit_shipdate->bitmaps[i]->btv;
	}

	auto s1 = std::chrono::high_resolution_clock::now();

	ibis::bitvector btv_discount;
	btv_discount.copy(*cubit_discount->bitmaps[lower_discount]->btv);
	btv_discount.decompress();

	for(int i = lower_discount + 1; i <= upper_discount; i++) {
		btv_discount |= *cubit_discount->bitmaps[i]->btv;
	}

	auto s2 = std::chrono::high_resolution_clock::now();

	ibis::bitvector btv_quantity;
	btv_quantity.copy(*cubit_quantity->bitmaps[1]->btv);
	btv_quantity.decompress();

	for(int i = 2; i <= upper_quantity; i++) {
		btv_quantity |= *cubit_quantity->bitmaps[i]->btv;
	}

	auto s3 = std::chrono::high_resolution_clock::now();

	auto &btv_res = btv_shipdate;
	btv_res &= btv_discount;
	btv_res &= btv_quantity;

	auto s4 = std::chrono::high_resolution_clock::now();
	
	GetRowids(btv_res, row_ids);

	auto s5 = std::chrono::high_resolution_clock::now();

	std::cout << "shipdate's bitmap time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;
	std::cout << "discount's bitmap time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count() << "ms" << std::endl;
	std::cout << "quantity's bitmap time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s2).count() << "ms" << std::endl;
	std::cout << "bitmaps time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s4 - s0).count() << "ms" << std::endl;
	std::cout << "get row id : " << std::chrono::duration_cast<std::chrono::milliseconds>(s5 - s4).count() << "ms" << std::endl;

}

struct pair_hash {
	template <class T1, class T2>
	std::size_t operator() (const std::pair<T1, T2>& p) const {
		auto h1 = std::hash<T1>{}(p.first);
		auto h2 = std::hash<T2>{}(p.second);
		return h1 ^ (h2 << 1);
	}
};

struct q10_topk {
	bool operator ()(std::pair<int64_t, int64_t> &a, std::pair<int64_t, int64_t> &b) {
		return a.second > b.second;
	}
};

void PhysicalTableScan::TPCH_Q10(ExecutionContext &context) const {

	auto s0 = std::chrono::high_resolution_clock::now();

	auto &nation_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "nation", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &customer_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "customer", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &lineitem_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "lineitem", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &orders_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "orders", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();

	const char returnflag = 'R';

	std::unordered_map<int32_t, std::string> nation_map;
	{
		auto &nation_transaction = DuckTransaction::Get(context.client, nation_table.catalog);
		TableScanState nation_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(1));
		nation_table.GetStorage().InitializeScan(nation_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(nation_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(nation_table.GetColumns().GetColumnTypes()[1]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			nation_table.GetStorage().Scan(nation_transaction, result, nation_scan_state);
			if(result.size() == 0)
				break;

			auto &nation_key = result.data[0];
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

			for(int i = 0; i < result.size(); i++) {
				nation_map[nation_key_data[i]] = "tmp";
			}
		}
	}

	std::unordered_map<int64_t, int32_t> customer_nation_map;
	// customer_nation_map.reserve(1500000);
	{
		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
		TableScanState customer_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(3));
		// storage_column_ids.push_back(uint64_t(1));
		// storage_column_ids.push_back(uint64_t(2));
		// storage_column_ids.push_back(uint64_t(4));
		// storage_column_ids.push_back(uint64_t(5));
		// storage_column_ids.push_back(uint64_t(7));
		customer_table.GetStorage().InitializeScan(customer_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(customer_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(customer_table.GetColumns().GetColumnTypes()[3]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[1]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[2]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[4]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[5]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[7]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
			if(result.size() == 0)
				break;

			auto &customer_key = result.data[0];
			auto &nation_key = result.data[1];
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

			for(int i = 0; i < result.size(); i++) {
				if(nation_map.count(nation_key_data[i]))
					customer_nation_map[customer_key_data[i]] = nation_key_data[i];
			}
		}
	}

	auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_orderkey);
	ibis::bitvector btv_res;
	btv_res.adjustSize(0, cubit_orderkey->config->n_rows);
	btv_res.decompress();
	std::unordered_map<int64_t, std::pair<int64_t, int32_t>> order_map;
	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(1));
		storage_column_ids.push_back(uint64_t(4));
		orders_table.GetStorage().InitializeScan(orders_transaction, orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto &orders_key = result.data[0];
			auto &customer_key = result.data[1];
			auto &days = result.data[2];
			auto orders_key_data = FlatVector::GetData<int64_t>(orders_key);
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);
			auto days_data = FlatVector::GetData<int32_t>(days);

			for(int i = 0; i < result.size(); i++) {
				if(days_data[i] < 8766 && days_data[i] >= 8674) {
					if(customer_nation_map.count(customer_key_data[i])) {
						btv_res |= *cubit_orderkey->bitmaps[orders_key_data[i]]->btv;
						order_map[orders_key_data[i]] = {customer_key_data[i], customer_nation_map[customer_key_data[i]]};
					}
				}
			}
		}
	}

	std::unordered_map<std::pair<int64_t, int32_t>, int64_t, pair_hash> group_map;
	{
		long long time1 = 0;
		auto cubit_returnflag = dynamic_cast<cubit::Cubit *>(context.client.bitmap_returnflag);
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(5));
		storage_column_ids.push_back(uint64_t(6));
		// storage_column_ids.push_back(uint64_t(8));
		lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
		// types.push_back(lineitem_table.GetColumns().GetColumnTypes()[8]);

		// 1 is 'R'
		btv_res &= *cubit_returnflag->bitmaps[1]->btv;


		vector<row_t> *row_ids = new vector<row_t>;
		size_t cursor = 0;

		row_ids->resize(btv_res.count() + 64);
		// ids[btv_res.count()] = 999999999;
		auto element_ptr = &(*row_ids)[0];

		uint32_t ids_count = 0;
		uint64_t ids_idx = 0;
		// traverse m_vec
		auto it = btv_res.m_vec.begin();
		while(it != btv_res.m_vec.end()) {
			vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, reverseBits(*it));
			ids_idx += 31;
			it++;
		}

		// active word
		vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, \
								reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

		row_ids->resize(btv_res.count());

		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);

			if(cursor < row_ids->size()) {
				ColumnFetchState column_fetch_state;
				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*row_ids)[cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(cursor + fetch_count > row_ids->size()) {
					fetch_count = row_ids->size() - cursor;
				}
				lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);

				cursor += fetch_count;
			}
			else {
				delete row_ids;
				break;
			}

			auto &order_key = result.data[0];
			auto &extenedprice = result.data[1];
			auto &discount = result.data[2];
			auto order_key_data = FlatVector::GetData<int64_t>(order_key);
			auto extenedprice_data = FlatVector::GetData<int64_t>(extenedprice);
			auto discount_data = FlatVector::GetData<int64_t>(discount);

			for(int i = 0; i < result.size(); i++) {
					group_map[order_map[order_key_data[i]]] += extenedprice_data[i] * (100 - discount_data[i]);
			}
		}

	}

	std::priority_queue<std::pair<int64_t, int64_t>,vector<std::pair<int64_t, int64_t>>,q10_topk> minHeap;
	int heapc = 20;

	for(auto &it : group_map) {
		if(minHeap.size() < heapc)
			minHeap.push({it.first.first, it.second});
		else {
			if(it.second <= minHeap.top().second)
				continue;
			minHeap.pop();
			minHeap.push({it.first.first, it.second});
		}
	}

	while(!minHeap.empty()) {
		std::cout << minHeap.top().first << " : " << minHeap.top().second << std::endl;
		minHeap.pop();
	}

	auto e0 = std::chrono::high_resolution_clock::now();

	std::cout << "q10 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(e0 - s0).count() << "ms" << std::endl;

}

void PhysicalTableScan::TPCH_Q12_Orders_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids) const {
	auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_o_orderkey);

	ibis::bitvector btv_res;
	btv_res.adjustSize(0, cubit_orderkey->config->n_rows);
	btv_res.decompress();

	for(auto orderkey : context.client.q12_orderkey) {
		btv_res |= *cubit_orderkey->bitmaps[orderkey]->btv;
	}

	GetRowids(btv_res, row_ids);
}

void PhysicalTableScan::TPCH_Q14_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids) const {
	auto cubit_shipdate = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipdate);

	int lower_days = 9374;
	int upper_days = 9404;

	ibis::bitvector btv_res;
	btv_res.copy(*cubit_shipdate->bitmaps[lower_days]->btv);
	btv_res.decompress();

	for(int i = lower_days + 1; i < upper_days; i++) {
		btv_res |= *cubit_shipdate->bitmaps[i]->btv;
	}

	row_ids->resize(btv_res.count() + 64);
	auto element_ptr = &(*row_ids)[0];

	uint32_t ids_count = 0;
	uint64_t ids_idx = 0;
	// traverse m_vec
	auto it = btv_res.m_vec.begin();
	while(it != btv_res.m_vec.end()) {
		vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, reverseBits(*it));
		ids_idx += 31;
		it++;
	}

	// active word
	vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, \
							reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

	row_ids->resize(btv_res.count());

	std::cout << row_ids->size() << std::endl;
}

void PhysicalTableScan::TPCH_Q15_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids) const {
	auto cubit_shipdate = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipdate);

	int lower_days = 9496;
	int upper_days = 9587;

	ibis::bitvector btv_res;
	btv_res.copy(*cubit_shipdate->bitmaps[lower_days]->btv);
	btv_res.decompress();

	for(int i = lower_days + 1; i < upper_days; i++) {
		btv_res |= *cubit_shipdate->bitmaps[i]->btv;
	}

	row_ids->resize(btv_res.count() + 64);
	auto element_ptr = &(*row_ids)[0];

	uint32_t ids_count = 0;
	uint64_t ids_idx = 0;
	// traverse m_vec
	auto it = btv_res.m_vec.begin();
	while(it != btv_res.m_vec.end()) {
		vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, reverseBits(*it));
		ids_idx += 31;
		it++;
	}

	// active word
	vbmi2_decoder_cvtepu16(element_ptr, ids_count, ids_idx, \
							reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

	row_ids->resize(btv_res.count());

	std::cout << row_ids->size() << std::endl;
}

void PhysicalTableScan::TPCH_Q17(ExecutionContext &context) const {
	auto s0 = std::chrono::high_resolution_clock::now();
	auto &part_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "part", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();
	auto &lineitem_table = Catalog::GetEntry(context.client, CatalogType::TABLE_ENTRY, "", "", "lineitem", OnEntryNotFound::RETURN_NULL)->Cast<TableCatalogEntry>();

	const char *brand = "Brand#23";
	const char *container = "MED BOX";

	ibis::bitvector btv_res;
	auto cubit_partkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_partkey);
	btv_res.adjustSize(0, cubit_partkey->config->n_rows);
	btv_res.decompress();
	{
		auto &part_transaction = DuckTransaction::Get(context.client, part_table.catalog);
		TableScanState part_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(0));
		storage_column_ids.push_back(uint64_t(3));
		storage_column_ids.push_back(uint64_t(6));
		part_table.GetStorage().InitializeScan(part_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(part_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(part_table.GetColumns().GetColumnTypes()[3]);
		types.push_back(part_table.GetColumns().GetColumnTypes()[6]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			part_table.GetStorage().Scan(part_transaction, result, part_scan_state);
			if(result.size() == 0)
				break;

			auto &part_key = result.data[0];
			auto &part_brand = result.data[1];
			auto &part_container = result.data[2];
			auto part_key_data = FlatVector::GetData<int64_t>(part_key);

			if(part_brand.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &brand_sel_vector = DictionaryVector::SelVector(part_brand);
				auto &brand_child = DictionaryVector::Child(part_brand);
				auto &container_sel_vector = DictionaryVector::SelVector(part_container);
				auto &container_child = DictionaryVector::Child(part_container);

				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(brand_child.GetData())[brand_sel_vector.get_index(i)].GetData(), brand) && \
						!strcmp(reinterpret_cast<string_t *>(container_child.GetData())[container_sel_vector.get_index(i)].GetData(), container))
						btv_res |= *cubit_partkey->bitmaps[part_key_data[i]]->btv;
				}
			}
			else {
				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(part_brand.GetData())[i].GetData(), brand) && \
						!strcmp(reinterpret_cast<string_t *>(part_container.GetData())[i].GetData(), container))
						btv_res |= *cubit_partkey->bitmaps[part_key_data[i]]->btv;
				}
			}
		}
	}

	vector<row_t> *row_ids = new vector<row_t>;

	GetRowids(btv_res, row_ids);

	std::unordered_map<int64_t, std::pair<int64_t, int32_t> > partkey_quantity_map;
	std::unordered_map<int64_t, double> avg_quantity_map;
	{
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(1));
		storage_column_ids.push_back(uint64_t(4));
		lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);

		size_t cursor = 0;

		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);

			if(cursor < row_ids->size()) {
				ColumnFetchState column_fetch_state;
				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*row_ids)[cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(cursor + fetch_count > row_ids->size()) {
					fetch_count = row_ids->size() - cursor;
				}
				lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);

				cursor += fetch_count;
			}
			else {
				break;
			}

			auto &part_key = result.data[0];
			auto &quantity = result.data[1];
			auto part_key_data = FlatVector::GetData<int64_t>(part_key);
			auto quantity_data = FlatVector::GetData<int64_t>(quantity);

			for(int i = 0; i < result.size(); i++) {
				auto &it = partkey_quantity_map[part_key_data[i]];
				it.first += quantity_data[i];
				it.second++;
			}
		}

		for(auto it = partkey_quantity_map.begin(); it != partkey_quantity_map.end(); it++) {
			avg_quantity_map[it->first] = 0.2 * ((double)it->second.first / it->second.second);
		}

	}

	int64_t sum_extendedprice = 0;
	{
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, *this);
		vector<storage_t> storage_column_ids;
		storage_column_ids.push_back(uint64_t(1));
		storage_column_ids.push_back(uint64_t(4));
		storage_column_ids.push_back(uint64_t(5));
		lineitem_table.GetStorage().InitializeScan(lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);

		size_t cursor = 0;

		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);

			if(cursor < row_ids->size()) {
				ColumnFetchState column_fetch_state;
				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*row_ids)[cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(cursor + fetch_count > row_ids->size()) {
					fetch_count = row_ids->size() - cursor;
				}
				lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);

				cursor += fetch_count;
			}
			else {
				break;
			}

			auto &part_key = result.data[0];
			auto &quantity = result.data[1];
			auto &extendedprice = result.data[2];
			auto part_key_data = FlatVector::GetData<int64_t>(part_key);
			auto quantity_data = FlatVector::GetData<int64_t>(quantity);
			auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);

			for(int i = 0; i < result.size(); i++) {
				if((double)quantity_data[i] < avg_quantity_map[part_key_data[i]])
					sum_extendedprice += extendedprice_data[i];
			}
		}
	}

	delete row_ids;
	auto e0 = std::chrono::high_resolution_clock::now();

	std::cout << "sum : " << sum_extendedprice << std::endl;
	sum_extendedprice /= 100;
	double q17_ans = sum_extendedprice / 7.0;
	std::cout << "avg : " << std::setprecision(16) << q17_ans << std::endl;
	std::cout << "q17 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(e0 - s0).count() << "ms" << std::endl;
	

}

void PhysicalTableScan::TPCH_Q18_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids) const {
	auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_orderkey);

	ibis::bitvector btv_res;
	btv_res.adjustSize(0, cubit_orderkey->config->n_rows);
	btv_res.decompress();

	for(auto orderkey : context.client.q18_orderkey) {
		btv_res |= *cubit_orderkey->bitmaps[orderkey]->btv;
	}

	GetRowids(btv_res, row_ids);
}

void PhysicalTableScan::TPCH_Q18_Orders_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids) const {
	auto cubit_orderkey = dynamic_cast<cubit::Cubit *>(context.client.bitmap_o_orderkey);

	ibis::bitvector btv_res;
	btv_res.adjustSize(0, cubit_orderkey->config->n_rows);
	btv_res.decompress();

	for(auto orderkey : context.client.q18_orderkey) {
		btv_res |= *cubit_orderkey->bitmaps[orderkey]->btv;
	}

	GetRowids(btv_res, row_ids);
}

void PhysicalTableScan::TPCH_Q19_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids) const {
	auto cubit_shipmode = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipmode);
	auto cubit_shipinstruct = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipinstruct);

	std::unordered_map<std::string, int> modeti;
	modeti["REG AIR"] = 0;
	modeti["AIR"] = 1;
	modeti["RAIL"] = 2;
	modeti["SHIP"] = 3;
	modeti["TRUCK"] = 4;
	modeti["MAIL"] = 5;
	modeti["FOB"] = 6;

	std::unordered_map<std::string, int> instructti;
	instructti["DELIVER IN PERSON"] = 0;
	instructti["COLLECT COD"] = 1;
	instructti["NONE"] = 2;
	instructti["TAKE BACK RETURN"] = 3;

	ibis::bitvector btv_res;
	btv_res.copy(*cubit_shipmode->bitmaps[modeti["AIR"]]->btv);
	btv_res.decompress();

	btv_res &= *cubit_shipinstruct->bitmaps[instructti["DELIVER IN PERSON"]]->btv;

	GetRowids(btv_res, row_ids);
}

SourceResultType PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	// TPCH_Q5(context);

	D_ASSERT(!column_ids.empty());
	auto &gstate = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &state = input.local_state.Cast<TableScanLocalSourceState>();

	if(context.client.GetCurrentQuery() == (char*)TPCH_QUERIES_q06) {
		if(*cursor == 0) {
			TPCH_Q6_Lineitem_GetRowIds(context, row_ids);
		}
		
		if(*cursor < row_ids->size()) {

			vector<storage_t> storage_column_ids;

			storage_column_ids.push_back(uint64_t(6));	// For l_discount
			storage_column_ids.push_back(uint64_t(5));	// For l_extendedprice

			TableScanState local_storage_state;
			local_storage_state.Initialize(storage_column_ids, table_filters.get());
			ColumnFetchState column_fetch_state;

			auto &table_bind_data = bind_data.get()->Cast<TableScanBindData>();
			auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			idx_t fetch_count = 2048;
			if(*cursor + fetch_count > row_ids->size()) {
				fetch_count = row_ids->size() - *cursor;
			}

			table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state);
			*cursor += fetch_count;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		else {
			delete row_ids;
			delete cursor;
			return SourceResultType::FINISHED;
		}
	}

	if( bind_data.get()->Cast<TableScanBindData>().table.name == "orders" && context.client.GetCurrentQuery() == (char*)TPCH_QUERIES_q12) {
		if(*cursor == 0) {
			TPCH_Q12_Orders_GetRowIds(context, row_ids);
			context.client.q12_orderkey.clear();
		}
		
		if(*cursor < row_ids->size()) {

			vector<storage_t> storage_column_ids;

			storage_column_ids.push_back(uint64_t(0));	// For o_orderkey
			storage_column_ids.push_back(uint64_t(5));	// For o_orderpriority

			TableScanState local_storage_state;
			local_storage_state.Initialize(storage_column_ids, table_filters.get());
			ColumnFetchState column_fetch_state;

			auto &table_bind_data = bind_data.get()->Cast<TableScanBindData>();
			auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			idx_t fetch_count = 2048;
			if(*cursor + fetch_count > row_ids->size()) {
				fetch_count = row_ids->size() - *cursor;
			}

			table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state);
			*cursor += fetch_count;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		else {
			delete row_ids;
			delete cursor;
			return SourceResultType::FINISHED;
		}
	}

	if( bind_data.get()->Cast<TableScanBindData>().table.name == "lineitem" && context.client.GetCurrentQuery() == (char*)TPCH_QUERIES_q14) {
		if(*cursor == 0) {
			TPCH_Q14_Lineitem_GetRowIds(context, row_ids);
		}
		
		if(*cursor < row_ids->size()) {

			vector<storage_t> storage_column_ids;

			storage_column_ids.push_back(uint64_t(1));	// For l_partkey
			storage_column_ids.push_back(uint64_t(5));	// For l_extendedprice
			storage_column_ids.push_back(uint64_t(6));	// For l_discount

			TableScanState local_storage_state;
			local_storage_state.Initialize(storage_column_ids, table_filters.get());
			ColumnFetchState column_fetch_state;

			auto &table_bind_data = bind_data.get()->Cast<TableScanBindData>();
			auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			idx_t fetch_count = 2048;
			if(*cursor + fetch_count > row_ids->size()) {
				fetch_count = row_ids->size() - *cursor;
			}

			table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state);
			*cursor += fetch_count;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		else {
			delete row_ids;
			delete cursor;
			return SourceResultType::FINISHED;
		}
	}

	if( bind_data.get()->Cast<TableScanBindData>().table.name == "lineitem" && context.client.GetCurrentQuery() == (char*)TPCH_QUERIES_q15) {
		if(*cursor == 0) {
			TPCH_Q15_Lineitem_GetRowIds(context, row_ids);
		}
		
		if(*cursor < row_ids->size()) {

			vector<storage_t> storage_column_ids;

			storage_column_ids.push_back(uint64_t(2));	// For l_suppkey
			storage_column_ids.push_back(uint64_t(5));	// For l_extendedprice
			storage_column_ids.push_back(uint64_t(6));	// For l_discount

			TableScanState local_storage_state;
			local_storage_state.Initialize(storage_column_ids, table_filters.get());
			ColumnFetchState column_fetch_state;

			auto &table_bind_data = bind_data.get()->Cast<TableScanBindData>();
			auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			idx_t fetch_count = 2048;
			if(*cursor + fetch_count > row_ids->size()) {
				fetch_count = row_ids->size() - *cursor;
			}

			table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state);
			*cursor += fetch_count;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		else {
			delete row_ids;
			delete cursor;
			return SourceResultType::FINISHED;
		}
	}

	if(context.client.GetCurrentQuery() == (char*)TPCH_QUERIES_q18 && context.client.q18_orderkey.size() != 0) {
		if( bind_data.get()->Cast<TableScanBindData>().table.name == "lineitem" ) {
			if(*cursor == 0) {
				TPCH_Q18_Lineitem_GetRowIds(context, row_ids);
			}
			
			if(*cursor < row_ids->size()) {

				vector<storage_t> storage_column_ids;

				storage_column_ids.push_back(uint64_t(0));	// For l_orderkey
				storage_column_ids.push_back(uint64_t(4));	// For l_quantity

				TableScanState local_storage_state;
				local_storage_state.Initialize(storage_column_ids, table_filters.get());
				ColumnFetchState column_fetch_state;

				auto &table_bind_data = bind_data.get()->Cast<TableScanBindData>();
				auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(*cursor + fetch_count > row_ids->size()) {
					fetch_count = row_ids->size() - *cursor;
				}

				table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);
				*cursor += fetch_count;
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
			else {
				delete row_ids;
				delete cursor;
				context.client.q18_orderkey.resize(0);
				return SourceResultType::FINISHED;
			}
		}

		if( bind_data.get()->Cast<TableScanBindData>().table.name == "orders" ) {
			if(*cursor == 0) {
				TPCH_Q18_Orders_GetRowIds(context, row_ids);
			}
			
			if(*cursor < row_ids->size()) {

				vector<storage_t> storage_column_ids;

				storage_column_ids.push_back(uint64_t(0));	// For o_orderkey
				storage_column_ids.push_back(uint64_t(1));	// For o_custkey
				storage_column_ids.push_back(uint64_t(4));	// For o_orderdate
				storage_column_ids.push_back(uint64_t(3));	// For o_totalprice


				TableScanState local_storage_state;
				local_storage_state.Initialize(storage_column_ids, table_filters.get());
				ColumnFetchState column_fetch_state;

				auto &table_bind_data = bind_data.get()->Cast<TableScanBindData>();
				auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(*cursor + fetch_count > row_ids->size()) {
					fetch_count = row_ids->size() - *cursor;
				}

				table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);
				*cursor += fetch_count;
				return SourceResultType::HAVE_MORE_OUTPUT;
			}
			else {
				delete row_ids;
				delete cursor;
				return SourceResultType::FINISHED;
			}
		}
	}

	if( bind_data.get()->Cast<TableScanBindData>().table.name == "lineitem" && context.client.GetCurrentQuery() == (char*)TPCH_QUERIES_q19) {
		if(*cursor == 0) {
			TPCH_Q19_Lineitem_GetRowIds(context, row_ids);
		}
		
		if(*cursor < row_ids->size()) {

			vector<storage_t> storage_column_ids;

			storage_column_ids.push_back(uint64_t(1));	// For l_partkey
			storage_column_ids.push_back(uint64_t(4));	// For l_quantity
			storage_column_ids.push_back(uint64_t(14));	// For l_shipmode
			storage_column_ids.push_back(uint64_t(5));	// For l_extendedprice
			storage_column_ids.push_back(uint64_t(6));	// For l_discount

			TableScanState local_storage_state;
			local_storage_state.Initialize(storage_column_ids, table_filters.get());
			ColumnFetchState column_fetch_state;

			auto &table_bind_data = bind_data.get()->Cast<TableScanBindData>();
			auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			idx_t fetch_count = 2048;
			if(*cursor + fetch_count > row_ids->size()) {
				fetch_count = row_ids->size() - *cursor;
			}

			table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state);
			*cursor += fetch_count;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		else {
			delete row_ids;
			delete cursor;
			return SourceResultType::FINISHED;
		}
	}


	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	function.function(context.client, data, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
	/*
	We run 10 trials for CUBIT-powered DuckDB on TPC-H Q6.
	The first two trials are to warm up. We report the mean value of the following trials. 
	We use a similar strategy in reporting the performance of DuckDB's original Scan.
	for (int times = 0; times < 10; times++) {

		auto start = std::chrono::high_resolution_clock::now();

		D_ASSERT(!column_ids.empty());
		auto &gstate = input.global_state.Cast<TableScanGlobalSourceState>();
		auto &state = input.local_state.Cast<TableScanLocalSourceState>();

		auto cubit_shipdate = dynamic_cast<cubit::Cubit *>(context.client.bitmap_shipdate);
		auto cubit_discount = dynamic_cast<cubit::Cubit *>(context.client.bitmap_discount);
		auto cubit_quantity = dynamic_cast<cubit::Cubit *>(context.client.bitmap_quantity);

		int lower_year;
		int upper_year;

		int lower_discount;
		int upper_discount;

		int upper_quantity = 24;

		// Interpret the input Q6 predicate.
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;

			switch (filter->filter_type) {
			case TableFilterType::CONJUNCTION_AND: {
				auto &conj_filter = filter->Cast<ConjunctionAndFilter>();
				if (column_index < names.size()) {
					const std::string &column_name = names[column_ids[column_index]];

					if (column_name == "l_shipdate") {
						for (auto &child_filter : conj_filter.child_filters) {
							switch (child_filter->filter_type) {
							case TableFilterType::CONSTANT_COMPARISON: {
								auto &constant_comp_filter = child_filter->Cast<ConstantFilter>();
								if (constant_comp_filter.constant.type().id() == LogicalTypeId::DATE) {
									D_ASSERT(constant_comp_filter.constant.type().InternalType() ==
									         PhysicalType::INT32);
									D_ASSERT(column_name == "l_shipdate");
									auto days = constant_comp_filter.constant.GetValueUnsafe<int32_t>();
									date_t date(days);
									auto year = Date::ExtractYear(date);
									switch (constant_comp_filter.comparison_type) {
									case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
										lower_year = year - 1992;
										break;
									case ExpressionType::COMPARE_LESSTHAN:
										upper_year = year - 1992;
										break;
									default:
										throw InternalException("Invalid comparison in TPCH-Q6");
									}
								}
								break;
							}
							default:
								break;
							}
						}
					} else if (column_name == "l_discount") {
						auto null_par = CastParameters();
						for (auto &child_filter : conj_filter.child_filters) {
							switch (child_filter->filter_type) {
							case TableFilterType::CONSTANT_COMPARISON: {
								auto &constant_comp_filter = child_filter->Cast<ConstantFilter>();
								if (constant_comp_filter.constant.type().id() == LogicalTypeId::DECIMAL) {
									double discount;
									switch (constant_comp_filter.constant.type().InternalType()) {
									case PhysicalType::INT16: {
										int16_t value16 = constant_comp_filter.constant.GetValueUnsafe<int16_t>();
										TryCastFromDecimal::Operation(value16, discount, null_par, 15, 2);
										break;
									}
									case PhysicalType::INT32: {
										int32_t value32 = constant_comp_filter.constant.GetValueUnsafe<int32_t>();
										TryCastFromDecimal::Operation(value32, discount, null_par, 15, 2);
										break;
									}
									case PhysicalType::INT64: {
										int64_t value64 = constant_comp_filter.constant.GetValueUnsafe<int64_t>();
										TryCastFromDecimal::Operation(value64, discount, null_par, 15, 2);
										break;
									}
									default:
										throw InternalException("Unsupported physical type for decimal");
									}
									switch (constant_comp_filter.comparison_type) {
									case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
										lower_discount = static_cast<int>(discount * 100);
										break;
									case ExpressionType::COMPARE_LESSTHANOREQUALTO:
										upper_discount = static_cast<int>(discount * 100);
										break;
									default:
										throw InternalException("Invalid comparison in TPCH-Q6");
									}
								}
								break;
							}
							default:
								break;
							}
						}
					}
				}
				break;
			}
			default:
				throw InternalException("unspported filter type");
			}
		}

		uint64_t n_seg = cubit_shipdate->bitmaps[0]->seg_btv->seg_table.size();
		uint64_t n_threads = 1;
		uint64_t n_seg_per_thread = n_seg / n_threads;
		uint64_t n_left = n_seg % n_threads;

		std::thread *threads = new std::thread[n_threads];
		std::vector<vector<row_t>> thread_row_ids(n_threads);
		std::vector<uint64_t> begin(n_threads + 1, 0);
		std::vector<double> local_revenues(n_threads, 0.0);
		for (uint64_t i = 1; i <= n_left; i++) {
			begin[i] = begin[i - 1] + n_seg_per_thread + 1;
		}
		for (uint64_t i = n_left + 1; i <= n_threads; i++) {
			begin[i] = begin[i - 1] + n_seg_per_thread;
		}

		// Assign the workload of each query to n_threads background threads,
		// each of which performs the workload of the parallel executor for TPC-H Q6.
		// We control the parallelism by hand, rather than relying on the parallel executor 
		// for better (hardware characteristics) measurement and performance tuning.
		auto &table_bind_data = bind_data->Cast<TableScanBindData>();
		auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);
		auto logical_types = GetTypes();
		for (uint64_t i = 0; i < n_threads; i++) {
			threads[i] = std::thread(&IndexRead, &thread_row_ids[i], begin[i], begin[i + 1], cubit_shipdate,
			                         cubit_discount, cubit_quantity, lower_year, upper_year, lower_discount,
			                         upper_discount, upper_quantity, &transaction, &context.client, &local_revenues[i],
			                         bind_data.get(), table_filters.get(), &chunk, &logical_types);
		}
		for (uint64_t i = 0; i < n_threads; i++) {
			threads[i].join();
		}
		double revenues = std::accumulate(local_revenues.begin(), local_revenues.end(), 0.0);

		auto end = std::chrono::high_resolution_clock::now();
		int64_t time_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
		std::cout << "revenues: " << std::fixed << revenues << std::endl;
		std::cout << "time consumption: " << time_elapsed_us << "us" << std::endl;
		std::cout << "------------------------" << std::endl;
	}

	std::exit(0);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
	*/
}


#define WAIT_FOR_PERF_U (1000 * 50)

static int gen_perf_process(char *tag) {
	int perf_pid = fork();
	if (perf_pid > 0) {
		// parent
		return perf_pid;
	} else if (perf_pid == 0) {
		// child
		perf_pid = getppid();
		char perf_pid_opt[24];
		memset(perf_pid_opt, 0, 24);
		snprintf(perf_pid_opt, 24, "%d", perf_pid);
		char output_filename[36];
		memset(output_filename, 0, 36);
		snprintf(output_filename, 36, "perf.output.%s.%d", tag, perf_pid);
		std::string events =
		    "cache-references,cache-misses,cycles,instructions,branches,branch-misses,page-faults,cpu-migrations";
		events += ",L1-dcache-loads,L1-dcache-load-misses,L1-icache-load-misses";
		events += ",LLC-loads,LLC-load-misses";
		events += ",dTLB-loads,dTLB-load-misses";
		char const *perfargs[12] = {"perf",          "stat",     "-e", events.c_str(), "-p", perf_pid_opt, "-o",
		                            output_filename, "--append", NULL};
		execvp("perf", (char **)perfargs);
		std::cout << "=== Failed to invoke perf:  " << strerror(errno) << std::endl;
	} else {
		std::cout << "=== Fork did not work in perf ===" << std::endl;
	}

	assert(0);
	return -1;
}

static int kill_perf_process(int perf_pid) {
	int stat_val;
	pid_t child_pid;

	do {
		kill(perf_pid, SIGINT);
		usleep(WAIT_FOR_PERF_U);
		child_pid = wait(&stat_val);
	} while (perf_pid != child_pid);

	return 0;
}

void or_op(ibis::bitvector *res, ibis::bitvector *rhs)
{
	// assume res.size() == rhs.size()
	// assume both uncompress
	size_t mvec_size = res->m_vec.size();
	size_t i = 0;
	// __m512i a,b,c;
	ibis::array_t<ibis::bitvector::word_t>::iterator i0 = res->m_vec.begin();
	ibis::array_t<ibis::bitvector::word_t>::const_iterator i1 = rhs->m_vec.begin();
	ibis::array_t<ibis::bitvector::word_t>::iterator iend = res->m_vec.begin() + (res->m_vec.size() / 16) * 16;
	__m512i *srcp = (__m512i *)i0;
	__m512i *dstp = (__m512i *)i1;
	__m512i *t = (__m512i *)iend;
	while(srcp < t) {
		const __m512i a = _mm512_loadu_si512(srcp);
		const __m512i b = _mm512_loadu_si512(dstp++);
		const __m512i c = _mm512_or_epi32(a, b);
		_mm512_storeu_si512(srcp++, c);
	}
}

// TPC-H Q6	
// SELECT
//     sum(l_extendedprice * l_discount) AS revenue
// FROM
//     lineitem
// WHERE
//     l_shipdate >= CAST('1994-01-01' AS date)
//     AND l_shipdate < CAST('1995-01-01' AS date)
//     AND l_discount BETWEEN 0.05
//     AND 0.07
//     AND l_quantity < 24;

void PhysicalTableScan::IndexRead(vector<row_t> *row_ids, uint64_t begin, uint64_t end, cubit::Cubit *bitmap_shipdate,
                                  cubit::Cubit *bitmap_discount, cubit::Cubit *bitmap_quantity, int lower_year,
                                  int upper_year, int lower_discount, int upper_discount, int upper_qantity,
                                  DuckTransaction *transaction, ClientContext *context, double *local_revenue,
                                  FunctionData *bind_data, TableFilterSet *table_filters, DataChunk *chunk,
                                  vector<LogicalType> *types) {

	int64_t elapsed1 = 0;
	int64_t elapsed2 = 0;
	int64_t elapsed3 = 0;
	int64_t elapsed4 = 0;
	int64_t elapsed5 = 0;

	for (uint64_t seg_idx = begin; seg_idx < end; seg_idx++)
	 {
		// Generate the resulting bitvector (segment) from CUBIT instances.
		auto s1 = std::chrono::high_resolution_clock::now();
		ibis::bitvector seg_shipdate;
		seg_shipdate.copy(*bitmap_quantity->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.btv);
		seg_shipdate.decompress();
		for (int i = lower_year; i < upper_year; i++) {
			auto &seg = bitmap_shipdate->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			// seg_shipdate |= *seg.btv;
			or_op(&seg_shipdate, seg.btv);
		}

		ibis::bitvector seg_discount;
		seg_discount.copy(*bitmap_quantity->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.btv);
		seg_discount.decompress();
		for (int i = lower_discount; i <= upper_discount; i++) {
			auto &seg = bitmap_discount->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			// seg_discount |= *seg.btv;
			or_op(&seg_discount, seg.btv);
		}
		seg_shipdate &= seg_discount;

		ibis::bitvector seg_result;
		// TODO: get rid of this copy, do `AND` directly on this seg
		seg_result.copy(*bitmap_quantity->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.btv);
		seg_result.decompress();
		// The original tpch-q6 in duckdb use `AND l_quantity < 24;` so for fair comparation, we use the following
		// seg_result |= *bitmap_quantity->bitmaps[1]->seg_btv->seg_table.find(seg_idx)->second.btv;

		for(int i = 1; i < 24; i++) {
			auto &seg = bitmap_quantity->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			// seg_result |= *seg.btv;
			or_op(&seg_result, seg.btv);
		}

		seg_result &= seg_shipdate;
		auto e1 = std::chrono::high_resolution_clock::now();
		elapsed1 += std::chrono::duration_cast<std::chrono::microseconds>(e1 - s1).count();

		// Transform the resulting bitvector (segment) from CUBIT instances to ID list.
		auto s2 = std::chrono::high_resolution_clock::now();
		for (ibis::bitvector::indexSet index_set = seg_result.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
			const ibis::bitvector::word_t *indices = index_set.indices();
			if (index_set.isRange()) {
				for (ibis::bitvector::word_t j = *indices; j < indices[1]; ++j) {
					row_ids->push_back((uint64_t)j +
					                   bitmap_shipdate->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.start_row);
				}
			} else {
				for (unsigned j = 0; j < index_set.nIndices(); ++j) {
					row_ids->push_back((uint64_t)indices[j] +
					                   bitmap_shipdate->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.start_row);
				}
			}
		}
		auto e2 = std::chrono::high_resolution_clock::now();
		elapsed2 += std::chrono::duration_cast<std::chrono::microseconds>(e2 - s2).count();
	}

	auto s3 = std::chrono::high_resolution_clock::now();
	auto &table_bind_data = bind_data->Cast<TableScanBindData>();
	auto result_ids = *row_ids;
	vector<storage_t> storage_column_ids;

	// Define the two columns (by specifying their ids) to be probed.
	// Note that for Q6, we only probe two columns, rather than 4 in Scan.
	// storage_column_ids.push_back(uint64_t(4));	// For l_quantity
	storage_column_ids.push_back(uint64_t(5));	// For l_extendedprice
	storage_column_ids.push_back(uint64_t(6));	// For l_discount

	TableScanState local_storage_state;
	local_storage_state.Initialize(storage_column_ids, table_filters);
	ColumnFetchState column_fetch_state;

	// Prepare chunks to receive data from the underlying data.
	std::vector<DataChunk *> local_chunks;
	int64_t elapsed_init = 0;
	std::cout << row_ids->size() << std::endl;
	for (size_t i = 0; i < row_ids->size(); i += 2048) {
		DataChunk *local_chunk = new DataChunk();
		auto s6 = std::chrono::high_resolution_clock::now();
		local_chunk->Initialize(Allocator::Get(*context), *types);
		auto e6 = std::chrono::high_resolution_clock::now();
		elapsed_init += std::chrono::duration_cast<std::chrono::microseconds>(e6 - s6).count();
		local_chunks.push_back(local_chunk);
	}
	// std::cout << "thread[" << begin << "] "
	//           << "init local chunks: " << elapsed_init << "us" << std::endl;
	auto e3 = std::chrono::high_resolution_clock::now();
	elapsed3 = std::chrono::duration_cast<std::chrono::microseconds>(e3 - s3).count();

	// To measure hardware characteristics.
	// int perf_pid;
	// perf_pid = gen_perf_process((char *)"fetch_chunk");
	// usleep(WAIT_FOR_PERF_U);

	// Probe l_extendedprice and l_discount.
	// For each iteration, we fetch a single chunk of data (commonly 2048 items).
	int128_t local_local_revenue = 0;
	for (size_t i = 0; i < local_chunks.size(); i++) {
		DataChunk *chunk = local_chunks[i];
		uint64_t fetch_count = 0;
		data_ptr_t row_ids_data = nullptr;
		if (local_chunks.size() == 1) {
			fetch_count = row_ids->size();
			row_ids_data = (data_ptr_t)&result_ids[0];
		} else if (i < local_chunks.size() - 1) {
			fetch_count = 2048;
			row_ids_data = (data_ptr_t)&result_ids[2048 * i];
		} else {
			fetch_count = row_ids->size() - (2048 * i);
			row_ids_data = (data_ptr_t)&result_ids[2048 * i];
		}
		Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);

		auto s4 = std::chrono::high_resolution_clock::now();
		table_bind_data.table.GetStorage().Fetch(*transaction, *chunk, storage_column_ids, row_ids_vec, fetch_count,
		                                         column_fetch_state);
		auto e4 = std::chrono::high_resolution_clock::now();
		elapsed4 += std::chrono::duration_cast<std::chrono::microseconds>(e4 - s4).count();

		// Calculate the revenue of Q6 for this chunk.
		auto s5 = std::chrono::high_resolution_clock::now();
		auto &vec1 = chunk->data[0];
		auto &vec2 = chunk->data[1];
		auto vec1_data = FlatVector::GetData<int64_t>(vec1);
		auto vec2_data = FlatVector::GetData<int64_t>(vec2);
		for (size_t i = 0; i < chunk->size(); i++) {
			int64_t lentry = vec1_data[i];
			int64_t rentry = vec2_data[i];
			local_local_revenue += lentry * rentry;
		}

		auto e5 = std::chrono::high_resolution_clock::now();
		elapsed5 += std::chrono::duration_cast<std::chrono::microseconds>(e5 - s5).count();
	}
	*local_revenue += (double)(local_local_revenue / 10000.0);

	// kill_perf_process(perf_pid);
	// usleep(WAIT_FOR_PERF_U);
	std::cout << "thread[" << begin << "] "
	          << "seg result: " << elapsed1 << "us" << std::endl;
	std::cout << "thread[" << begin << "] "
	          << "row ids: " << elapsed2 << "us" << std::endl;
	std::cout << "thread[" << begin << "] "
	          << "prepare for fetch: " << elapsed3 << "us" << std::endl;
	std::cout << "thread[" << begin << "] "
	          << "fetch: " << elapsed4 << "us" << std::endl;
	std::cout << "thread[" << begin << "] "
	          << "computation: " << elapsed5 << "us" << std::endl;
}

void PhysicalTableScan::IndexRead_nofetch(vector<row_t> *row_ids, uint64_t begin, uint64_t end, cubit::Cubit *bitmap_shipdate,
                                  cubit::Cubit *bitmap_discount, cubit::Cubit *bitmap_quantity, int lower_year,
                                  int upper_year, int lower_discount, int upper_discount, int upper_qantity) {

	int64_t elapsed1 = 0;
	int64_t elapsed2 = 0;
	int64_t elapsed_quantity = 0;

	for (uint64_t seg_idx = begin; seg_idx < end; seg_idx++)
	{
		// Generate the resulting bitvector (segment) from CUBIT instances.
		auto s1 = std::chrono::high_resolution_clock::now();
		ibis::bitvector seg_shipdate;
		seg_shipdate.copy(*bitmap_quantity->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.btv);
		seg_shipdate.decompress();
		for (int i = lower_year; i < upper_year; i++) {
			auto &seg = bitmap_shipdate->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			seg_shipdate |= *seg.btv;
			// or_op2(&seg_shipdate, seg.btv);
		}

		ibis::bitvector seg_discount;
		seg_discount.copy(*bitmap_quantity->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.btv);
		seg_discount.decompress();
		for (int i = lower_discount; i <= upper_discount; i++) {
			auto &seg = bitmap_discount->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			seg_discount |= *seg.btv;
			// or_op2(&seg_discount, seg.btv);
		}
		seg_shipdate &= seg_discount;

		auto s_quantity = std::chrono::high_resolution_clock::now();

		ibis::bitvector seg_result;
		// TODO: get rid of this copy, do `AND` directly on this seg
		seg_result.copy(*bitmap_quantity->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.btv);
		seg_result.decompress();
		// The original tpch-q6 in duckdb use `AND l_quantity < 24;` so for fair comparation, we use the following
		// seg_result |= *bitmap_quantity->bitmaps[1]->seg_btv->seg_table.find(seg_idx)->second.btv;

		for(int i = 1; i < 24; i++) {
			auto &seg = bitmap_quantity->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			seg_result |= *seg.btv;
			// or_op2(&seg_result, seg.btv);
		}

		auto e_quantity = std::chrono::high_resolution_clock::now();

		seg_result &= seg_shipdate;
		auto e1 = std::chrono::high_resolution_clock::now();
		elapsed1 += std::chrono::duration_cast<std::chrono::nanoseconds>(e1 - s1).count();
		elapsed_quantity += std::chrono::duration_cast<std::chrono::nanoseconds>(e_quantity - s_quantity).count();

		// Transform the resulting bitvector (segment) from CUBIT instances to ID list.
		auto s2 = std::chrono::high_resolution_clock::now();
		for (ibis::bitvector::indexSet index_set = seg_result.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
			const ibis::bitvector::word_t *indices = index_set.indices();
			if (index_set.isRange()) {
				for (ibis::bitvector::word_t j = *indices; j < indices[1]; ++j) {
					auto it = (uint64_t)j +
					                   bitmap_shipdate->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.start_row;
					if(it < 59986052)
						row_ids->push_back(it);
				}
			} else {
				for (unsigned j = 0; j < index_set.nIndices(); ++j) {
					auto it = (uint64_t)indices[j] +
					                   bitmap_shipdate->bitmaps[0]->seg_btv->seg_table.find(seg_idx)->second.start_row;
					if(it < 59986052)
						row_ids->push_back(it);
				}
			}
		}
		auto e2 = std::chrono::high_resolution_clock::now();
		elapsed2 += std::chrono::duration_cast<std::chrono::nanoseconds>(e2 - s2).count();
	}
	std::cout << "time for quantity : " << elapsed_quantity << "ns" << std::endl;
	std::cout << "time for id list : " << elapsed2 << "ns" << std::endl;
	std::cout << "time for op : " << elapsed1 << "ns" << std::endl;
}

double PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	if (function.table_scan_progress) {
		return function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
	}
	// if table_scan_progress is not implemented we don't support this function yet in the progress bar
	return -1;
}

idx_t PhysicalTableScan::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                       LocalSourceState &lstate) const {
	D_ASSERT(SupportsBatchIndex());
	D_ASSERT(function.get_batch_index);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(),
	                                gstate.global_state.get());
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name + " " + function.extra_info);
}

string PhysicalTableScan::ParamsToString() const {
	string result;
	if (function.to_string) {
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown) {
		if (function.filter_prune) {
			for (idx_t i = 0; i < projection_ids.size(); i++) {
				const auto &column_id = column_ids[projection_ids[i]];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		} else {
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column_id = column_ids[i];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		}
	}
	if (function.filter_pushdown && table_filters) {
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	if (!extra_info.file_filters.empty()) {
		result += "\n[INFOSEPARATOR]\n";
		result += "File Filters: " + extra_info.file_filters;
	}
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_cardinality);
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function.function != other.function.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

} // namespace duckdb
