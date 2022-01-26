#include <string>
#include <vector>
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/limits.hpp"
#include "../../third_party/numformat/zformat.hxx"
#include "../../third_party/numformat/localedata.h"

using namespace std;

namespace duckdb {

string_t NumForFun::NumberFormatScalarFunction(Vector &result, double num_value, string_t format) {
	try {
		string in_str = format.GetString();
		duckdb_numformat::LocaleData locale_data;
		duckdb_numformat::ImpSvNumberInputScan input_scan(&locale_data);
		unsigned short nCheckPos;
		string out_str;
		duckdb_numformat::Color *pColor = NULL;

		duckdb_numformat::SvNumberformat num_format(in_str, &locale_data, &input_scan, nCheckPos);

		if (!num_format.GetOutputString(num_value, out_str, &pColor)) {
			auto result_string = StringVector::EmptyString(result, out_str.size());
			auto result_data = result_string.GetDataWriteable();
			memcpy(result_data, out_str.c_str(), out_str.size());
			result_string.Finalize();
			return result_string;
		} else {
			auto result_string = StringVector::EmptyString(result, 0);
			result_string.Finalize();
			return result_string;
		}
	} catch (...) {
		throw InternalException("Unexpected result for number format");
	}

	return string_t();
}

static void NumberFormatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &number_vector = args.data[0];
	auto &format_vector = args.data[1];
	BinaryExecutor::Execute<double, string_t, string_t>(
	    number_vector, format_vector, result, args.size(),
	    [&](double value, string_t format) { return NumForFun::NumberFormatScalarFunction(result, value, format); });
}

void NumForFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet substr("text");
	substr.AddFunction(
	    ScalarFunction({LogicalType::DOUBLE, LogicalType::VARCHAR}, LogicalType::VARCHAR, NumberFormatFunction));
	set.AddFunction(substr);
	substr.name = "excel_text";
	set.AddFunction(substr);
}

} // namespace duckdb
