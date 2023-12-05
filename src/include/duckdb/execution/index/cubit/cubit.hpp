#pragma once
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/index.hpp"
#include "fastbit/bitvector.h"
#include "nicolas/base_table.h"
#include "nicolas/util.h"
#include "table_lk.h"

namespace duckdb {

struct CUBITIndexScanState;

class CUBIT : public Index {
public:
	CUBIT(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	    const vector<unique_ptr<Expression>> &unbound_expressions, const IndexConstraintType constraint_type,
	    AttachedDatabase &db);

	//! Root of the tree
	// from CUBIT
	BaseTable* bitmap;

public:
	//! Initialize a single predicate scan on the index with the given expression and column IDs
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
	                                                         const ExpressionType expression_type) override;
	//! Initialize a two predicate scan on the index with the given expression and column IDs
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
	                                                       const ExpressionType low_expression_type,
	                                                       const Value &high_value,
	                                                       const ExpressionType high_expression_type) override;
	//! Performs a lookup on the index, fetching up to max_count result IDs. Returns true if all row IDs were fetched,
	//! and false otherwise
	bool Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
	          vector<row_t> &result_ids) override;

	//! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	PreservedError Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	// //! Verify that data can be appended to the index without a constraint violation
	// void VerifyAppend(DataChunk &chunk) override;
	// //! Verify that data can be appended to the index without a constraint violation using the conflict manager
	// void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	// //! Deletes all data from the index. The lock obtained from InitializeLock must be held
	// void CommitDrop(IndexLock &index_lock) override;
	// //! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	// void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert a chunk of entries into the index
	PreservedError Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	bool MergeIndexes(IndexLock &state, Index &other_index) override;

	//! Traverses an ART and vacuums the qualifying nodes. The lock obtained from InitializeLock must be held
	// void Vacuum(IndexLock &state) override;

private:
	bool SearchEqual(CUBITIndexScanState &state, idx_t max_count, vector<row_t> &result_ids);
	bool SearchGreater(CUBITIndexScanState &state, bool equal, idx_t max_count, vector<row_t> &result_ids);
	bool SearchLess(CUBITIndexScanState &state, bool equal, idx_t max_count, vector<row_t> &result_ids);
	bool SearchCloseRange(CUBITIndexScanState &state, bool left_equal, bool right_equal, idx_t max_count, vector<row_t> &result_ids);
};

} // namespace duckdb
