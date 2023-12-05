#include "duckdb.hpp"

using namespace duckdb;

int main() {
	// real_row_num = sf*10*15000;
	// so, only real_sf >= 0.1 is right
 	double real_sf = 0.0;
	std::cout << "input sf :" << std::endl;
	std::cin >> real_sf ;
	ClientContext::sf = (int)(real_sf*10);
	DuckDB db(nullptr);

	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (3)");
	auto result = con.Query("SELECT * FROM integers");
	result->Print();
}
