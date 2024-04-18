//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/aes_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/storage/storage_extension.hpp"

// does this matter?
namespace duckdb {

class ClientContext;
class DatabaseInstance;

class AESGCMState {

public:
	DUCKDB_API AESGCMState();
	DUCKDB_API virtual ~AESGCMState();

public:
	DUCKDB_API virtual bool IsOpenSSL();
	DUCKDB_API virtual void InitializeEncryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len,
	                                             const std::string *key);
	DUCKDB_API virtual void InitializeDecryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len,
	                                             const std::string *key);
	DUCKDB_API virtual size_t Process(duckdb::const_data_ptr_t in, duckdb::idx_t in_len, duckdb::data_ptr_t out,
	                                  duckdb::idx_t out_len);
	DUCKDB_API virtual size_t Finalize(duckdb::data_ptr_t out, duckdb::idx_t out_len, duckdb::data_ptr_t tag,
	                                   duckdb::idx_t tag_len);
	DUCKDB_API virtual void GenerateRandomData(duckdb::data_ptr_t data, duckdb::idx_t len);

public:
	static constexpr size_t BLOCK_SIZE = 16;
};

class AESStateFactory {
public:
	virtual shared_ptr<AESGCMState> CreateAesState() const {
		return shared_ptr<AESGCMState>(new AESGCMState());
	}

	virtual ~AESStateFactory() {
	}
};

} // namespace duckdb