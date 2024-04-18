#include "duckdb/common/aes_state.hpp"

namespace duckdb {

AESGCMState::AESGCMState() {
	// abstract class, no implementation needed
}

AESGCMState::~AESGCMState() {
}

bool AESGCMState::IsOpenSSL() {
	throw NotImplementedException("AESGCMState Abstract Class is called");
}

void AESGCMState::InitializeEncryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len, const std::string *key) {
	throw NotImplementedException("AESGCMState Abstract Class is called");
}

void AESGCMState::InitializeDecryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len, const std::string *key) {
	throw NotImplementedException("AESGCMState Abstract Class is called");
}

size_t AESGCMState::Process(duckdb::const_data_ptr_t in, duckdb::idx_t in_len, duckdb::data_ptr_t out,
                            duckdb::idx_t out_len) {
	throw NotImplementedException("AESGCMState Abstract Class is called");
}

size_t AESGCMState::Finalize(duckdb::data_ptr_t out, duckdb::idx_t out_len, duckdb::data_ptr_t tag,
                             duckdb::idx_t tag_len) {
	throw NotImplementedException("AESGCMState Abstract Class is called");
}

void AESGCMState::GenerateRandomData(duckdb::data_ptr_t data, duckdb::idx_t len) {
	throw NotImplementedException("AESGCMState Abstract Class is called");
}

} // namespace duckdb
