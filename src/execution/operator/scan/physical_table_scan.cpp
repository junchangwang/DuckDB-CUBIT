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

SourceResultType PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	D_ASSERT(!column_ids.empty());
	auto &gstate = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &state = input.local_state.Cast<TableScanLocalSourceState>();

	if(context.client.GetCurrentQuery() != "SELECT\n    sum(l_extendedprice * l_discount) AS revenue\nFROM\n    lineitem\nWHERE\n    l_shipdate >= CAST('1994-01-01' AS date)\n    AND l_shipdate < CAST('1995-01-01' AS date)\n    AND l_discount BETWEEN 0.05\n    AND 0.07\n    AND l_quantity < 24;\n") {
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	function.function(context.client, data, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
	}

	// We run 10 trials for CUBIT-powered DuckDB on TPC-H Q6.
	// The first two trials are to warm up. We report the mean value of the following trials. 
	// We use a similar strategy in reporting the performance of DuckDB's original Scan.											
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
		uint64_t n_threads = 4;
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
		seg_shipdate.copy(*bitmap_shipdate->bitmaps[lower_year]->seg_btv->seg_table.find(seg_idx)->second.btv);
		for (int i = lower_year + 1; i < upper_year; i++) {
			auto &seg = bitmap_shipdate->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			seg_shipdate |= *seg.btv;
		}

		ibis::bitvector seg_discount;
		seg_discount.copy(*bitmap_discount->bitmaps[lower_discount]->seg_btv->seg_table.find(seg_idx)->second.btv);
		for (int i = lower_discount + 1; i <= upper_discount; i++) {
			auto &seg = bitmap_discount->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			seg_discount |= *seg.btv;
		}

		ibis::bitvector seg_result;
		// TODO: get rid of this copy, do `AND` directly on this seg
		seg_result.copy(*bitmap_quantity->bitmaps[1]->seg_btv->seg_table.find(seg_idx)->second.btv);
		// The original tpch-q6 in duckdb use `AND l_quantity < 24;` so for fair comparation, we use the following
		// seg_result |= *bitmap_quantity->bitmaps[1]->seg_btv->seg_table.find(seg_idx)->second.btv;
		for(int i = 2; i < 24; i++) {
			auto &seg = bitmap_quantity->bitmaps[i]->seg_btv->seg_table.find(seg_idx)->second;
			seg_result |= *seg.btv;
		}

		seg_result &= seg_shipdate;
		seg_result &= seg_discount;
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
