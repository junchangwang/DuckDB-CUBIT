#pragma once

#include <stddef.h>
#include <string>
#include "duckdb/common/aes_state.hpp"

namespace duckdb {

typedef unsigned char hash_bytes[32];
typedef unsigned char hash_str[64];

void sha256(const char *in, size_t in_len, hash_bytes &out);

void hmac256(const std::string &message, const char *secret, size_t secret_len, hash_bytes &out);

void hmac256(std::string message, hash_bytes secret, hash_bytes &out);

void hex256(hash_bytes &in, hash_str &out);

class AESGCMStateSSL : public duckdb::AESGCMState {

public:
	DUCKDB_API explicit AESGCMStateSSL();
	DUCKDB_API ~AESGCMStateSSL() override;

public:
	DUCKDB_API bool IsOpenSSL() override;
	DUCKDB_API void InitializeEncryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len,
	                                     const std::string *key) override;
	DUCKDB_API void InitializeDecryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len,
	                                     const std::string *key) override;
	DUCKDB_API size_t Process(duckdb::const_data_ptr_t in, duckdb::idx_t in_len, duckdb::data_ptr_t out,
	                          duckdb::idx_t out_len) override;
	DUCKDB_API size_t Finalize(duckdb::data_ptr_t out, duckdb::idx_t out_len, duckdb::data_ptr_t tag,
	                           duckdb::idx_t tag_len) override;
	DUCKDB_API void GenerateRandomData(duckdb::data_ptr_t data, duckdb::idx_t len) override;

public:
	static constexpr size_t BLOCK_SIZE = 16;
	static constexpr bool SSL = true;

private:
	void *gcm_context;
	// 0 = encrypt, 1 = decrypt
	bool mode;
};

class AESGCMStateSSLFactory : public duckdb::AESStateFactory {
public:
	duckdb::shared_ptr<duckdb::AESGCMState> CreateAesState() const override {
		return duckdb::shared_ptr<AESGCMStateSSL>(new AESGCMStateSSL());
	}

	~AESGCMStateSSLFactory() override {
	} //
};

} // namespace duckdb
