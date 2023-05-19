#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);

	Connection con(db);

	auto result = con.Query("SELECT sha512('123')");
	result->Print();
}
