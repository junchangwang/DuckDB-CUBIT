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

class AESGCMState {

public:
	DUCKDB_API AESGCMState();
	DUCKDB_API virtual ~AESGCMState();

public:
	DUCKDB_API virtual bool IsOpenSSL();
	DUCKDB_API virtual void InitializeEncryption(const_data_ptr_t iv, idx_t iv_len,
	                                             const std::string *key);
	DUCKDB_API virtual void InitializeDecryption(const_data_ptr_t iv, idx_t iv_len,
	                                             const std::string *key);
	DUCKDB_API virtual size_t Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out,
	                                  idx_t out_len);
	DUCKDB_API virtual size_t Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag,
	                                   idx_t tag_len);
	DUCKDB_API virtual void GenerateRandomData(data_ptr_t data, idx_t len);

public:
	static constexpr size_t BLOCK_SIZE = 16;
};

class EncryptionUtil {
public:
	virtual shared_ptr<AESGCMState> CreateAesState() const {
		return shared_ptr<AESGCMState>(new AESGCMState());
	}

	virtual ~EncryptionUtil() {
	}
};

} // namespace duckdb
