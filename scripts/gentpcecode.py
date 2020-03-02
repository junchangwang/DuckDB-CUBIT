import os

GENERATED_HEADER = 'include/tpce_generated.hpp'
GENERATED_SOURCE = 'tpce_generated.cpp'
TPCE_DIR = os.path.join('third_party', 'tpce-tool')

GENERATED_HEADER = os.path.join(TPCE_DIR, GENERATED_HEADER)
GENERATED_SOURCE = os.path.join(TPCE_DIR, GENERATED_SOURCE)

current_table = None

tables = {}

print(GENERATED_HEADER)
print(GENERATED_SOURCE)

header = open(GENERATED_HEADER, 'w+')
source = open(GENERATED_SOURCE, 'w+')

for fp in [header, source]:
	fp.write("""
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
// THIS FILE IS GENERATED BY gentpcecode.py, DO NOT EDIT MANUALLY //
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

""")

header.write("""
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "main/BaseLoader.h"
#include "main/BaseLoaderFactory.h"
#include "main/NullLoader.h"
#include "main/TableRows.h"

namespace TPCE {
	class DuckDBLoaderFactory : public CBaseLoaderFactory {
		duckdb::ClientContext *context;
		std::string schema;
		std::string suffix;

	  public:
		DuckDBLoaderFactory(duckdb::ClientContext *context, std::string schema,
		                    std::string suffix)
		    : context(context), schema(schema), suffix(suffix) {
		}

		// Functions to create loader classes for individual tables.
		virtual CBaseLoader<ACCOUNT_PERMISSION_ROW> *
		CreateAccountPermissionLoader();
		virtual CBaseLoader<ADDRESS_ROW> *CreateAddressLoader();
		virtual CBaseLoader<BROKER_ROW> *CreateBrokerLoader();
		virtual CBaseLoader<CASH_TRANSACTION_ROW> *
		CreateCashTransactionLoader();
		virtual CBaseLoader<CHARGE_ROW> *CreateChargeLoader();
		virtual CBaseLoader<COMMISSION_RATE_ROW> *CreateCommissionRateLoader();
		virtual CBaseLoader<COMPANY_COMPETITOR_ROW> *
		CreateCompanyCompetitorLoader();
		virtual CBaseLoader<COMPANY_ROW> *CreateCompanyLoader();
		virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW> *
		CreateCustomerAccountLoader();
		virtual CBaseLoader<CUSTOMER_ROW> *CreateCustomerLoader();
		virtual CBaseLoader<CUSTOMER_TAXRATE_ROW> *
		CreateCustomerTaxrateLoader();
		virtual CBaseLoader<DAILY_MARKET_ROW> *CreateDailyMarketLoader();
		virtual CBaseLoader<EXCHANGE_ROW> *CreateExchangeLoader();
		virtual CBaseLoader<FINANCIAL_ROW> *CreateFinancialLoader();
		virtual CBaseLoader<HOLDING_ROW> *CreateHoldingLoader();
		virtual CBaseLoader<HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader();
		virtual CBaseLoader<HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader();
		virtual CBaseLoader<INDUSTRY_ROW> *CreateIndustryLoader();
		virtual CBaseLoader<LAST_TRADE_ROW> *CreateLastTradeLoader();
		virtual CBaseLoader<NEWS_ITEM_ROW> *CreateNewsItemLoader();
		virtual CBaseLoader<NEWS_XREF_ROW> *CreateNewsXRefLoader();
		virtual CBaseLoader<SECTOR_ROW> *CreateSectorLoader();
		virtual CBaseLoader<SECURITY_ROW> *CreateSecurityLoader();
		virtual CBaseLoader<SETTLEMENT_ROW> *CreateSettlementLoader();
		virtual CBaseLoader<STATUS_TYPE_ROW> *CreateStatusTypeLoader();
		virtual CBaseLoader<TAX_RATE_ROW> *CreateTaxRateLoader();
		virtual CBaseLoader<TRADE_HISTORY_ROW> *CreateTradeHistoryLoader();
		virtual CBaseLoader<TRADE_ROW> *CreateTradeLoader();
		virtual CBaseLoader<TRADE_REQUEST_ROW> *CreateTradeRequestLoader();
		virtual CBaseLoader<TRADE_TYPE_ROW> *CreateTradeTypeLoader();
		virtual CBaseLoader<WATCH_ITEM_ROW> *CreateWatchItemLoader();
		virtual CBaseLoader<WATCH_LIST_ROW> *CreateWatchListLoader();
		virtual CBaseLoader<ZIP_CODE_ROW> *CreateZipCodeLoader();
	};

""")

source.write("""
#include "tpce_generated.hpp"

using namespace duckdb;
using namespace std;

namespace TPCE {
struct tpce_append_information {
	TableCatalogEntry *table;
	DataChunk chunk;
	ClientContext *context;
};

static void append_value(DataChunk & chunk, size_t index,
                         size_t & column, int32_t value) {
	((int32_t *)chunk.data[column++].GetData())[index] = value;
}

static void append_bigint(DataChunk & chunk, size_t index,
                          size_t & column, int64_t value) {
	((int64_t *)chunk.data[column++].GetData())[index] = value;
}

static void append_string(DataChunk & chunk, size_t index,
                          size_t & column, const char *value) {
	chunk.data[column++].SetValue(index, Value(value));
}

static void append_double(DataChunk & chunk, size_t index,
                          size_t & column, double value) {
	((double *)chunk.data[column++].GetData())[index] = value;
}

static void append_bool(DataChunk & chunk, size_t index,
                        size_t & column, bool value) {
	((bool *)chunk.data[column++].GetData())[index] = value;
}

static void append_timestamp(DataChunk & chunk, size_t index,
                             size_t & column, CDateTime time) {
	((timestamp_t *)chunk.data[column++].GetData())[index] =
	    0; // Timestamp::FromString(time.ToStr(1));
}

void append_char(DataChunk & chunk, size_t index, size_t & column,
                 char value) {
	char val[2];
	val[0] = value;
	val[1] = '\\0';
	append_string(chunk, index, column, val);
}

static void append_to_append_info(tpce_append_information & info) {
	auto &chunk = info.chunk;
	auto &table = info.table;
	if (chunk.column_count() == 0) {
		// initalize the chunk
		auto types = table->GetTypes();
		chunk.Initialize(types);
	} else if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		// flush the chunk
		table->storage->Append(*table, *info.context, chunk);
		// have to reset the chunk
		chunk.Reset();
	}
	chunk.SetCardinality(chunk.size() + 1);
}

template <typename T> class DuckDBBaseLoader : public CBaseLoader<T> {
  protected:
	tpce_append_information info;

  public:
	DuckDBBaseLoader(TableCatalogEntry *table, ClientContext *context) {
		info.table = table;
		info.context = context;
	}

	void FinishLoad() {
		// append the remainder
		info.table->storage->Append(*info.table, *info.context, info.chunk);
		info.chunk.Reset();
	}
};

""")

with open(os.path.join(TPCE_DIR, 'include/main/TableRows.h'), 'r') as f:
	for line in f:
		line = line.strip()
		if line.startswith('typedef struct '):
			line = line.replace('typedef struct ', '')
			current_table = line.split(' ')[0].replace('_ROW', ' ').replace('_', ' ').lower().strip()
			tables[current_table] = []
		elif line.startswith('}'):
			current_table = None
		elif current_table != None:
#row
#get type
			splits = line.strip().split(' ')
			if len(splits) < 2:
				continue
			line = splits[0]
			name = splits[1].split(';')[0].split('[')[0].lower()
			is_single_char = False
			if 'TIdent' in line or 'INT64' in line or 'TTrade' in line:
				tpe = "TypeId::BIGINT"
				sqltpe = "BIGINT"
			elif 'double' in line or 'float' in line:
				tpe = "TypeId::DECIMAL"
				sqltpe = "DECIMAL"
			elif 'int' in line:
				tpe = "TypeId::INTEGER"
				sqltpe = "INTEGER"
			elif 'CDateTime' in line:
				tpe = "TypeId::TIMESTAMP"
				sqltpe = "TIMESTAMP"
			elif 'bool' in line:
				tpe = 'TypeId::BOOLEAN'
				sqltpe = "BOOLEAN"
			elif 'char' in line:
				if '[' not in splits[1]:
					is_single_char = True
				tpe = "TypeId::VARCHAR"
				sqltpe = "VARCHAR"
			else:
				continue
			tables[current_table].append([name, tpe, is_single_char, sqltpe])

def get_tablename(name):
	name = name.title().replace(' ', '')
	if name == 'NewsXref':
		return 'NewsXRef'
	return name

for table in tables.keys():
	source.write("""
class DuckDB${TABLENAME}Load : public DuckDBBaseLoader<${ROW_TYPE}> {
public:
	DuckDB${TABLENAME}Load(TableCatalogEntry *table, ClientContext *context) :
		DuckDBBaseLoader(table, context) {

	}

	void WriteNextRecord(const ${ROW_TYPE} &next_record) {
		auto &chunk = info.chunk;
		append_to_append_info(info);
		size_t index = chunk.size() - 1;
		size_t column = 0;""".replace("${TABLENAME}", get_tablename(table)).replace("${ROW_TYPE}", table.upper().replace(' ', '_') + '_ROW'));
	source.write("\n\n")
	collist = tables[table]
	for i in range(len(collist)):
		entry = collist[i]
		name = entry[0].upper()
		tpe = entry[1]
		if tpe == "TypeId::BIGINT":
			funcname = "bigint"
		elif tpe == "TypeId::DECIMAL":
			funcname = "double"
		elif tpe == "TypeId::INTEGER":
			funcname = "value"
		elif tpe == "TypeId::TIMESTAMP":
			funcname = "timestamp"
		elif tpe == 'TypeId::BOOLEAN':
			funcname = "bool"
		elif tpe == "TypeId::VARCHAR":
			if entry[2]:
				funcname = "char"
			else:
				funcname = "string"
		else:
			print("Unknown type " + tpe)
			exit(1)
		source.write("\t\tappend_%s(chunk, index, column, next_record.%s);" % (funcname, name))
		if i != len(collist) - 1:
			source.write("\n")
	source.write("""
	}

};""")


for table in tables.keys():
	source.write("""
CBaseLoader<${ROW_TYPE}> *
DuckDBLoaderFactory::Create${TABLENAME}Loader() {
	auto table = Catalog::GetCatalog(*context).GetEntry<TableCatalogEntry>(*context,
	                                          schema, "${TABLEINDB}" + suffix);
	return new DuckDB${TABLENAME}Load(table, context);
}
""".replace("${TABLENAME}", get_tablename(table)).replace("${ROW_TYPE}", table.upper().replace(' ', '_') + '_ROW').replace("${TABLEINDB}", table.replace(' ', '_')))

source.write("\n")

# static string RegionSchema(string schema, string suffix) {
# 	return "CREATE TABLE " + schema + ".region" + suffix + " ("
# 		"r_regionkey INT NOT NULL,"
# 		"r_name VARCHAR(25) NOT NULL,"
# 		"r_comment VARCHAR(152) NOT NULL);";
# }


for table in tables.keys():
	tname = table.replace(' ', '_')
	str = 'static string ' + table.title().replace(' ', '') + 'Schema(string schema, string suffix) {\n'
	str += '\treturn "CREATE TABLE " + schema + ".%s" + suffix + " ("\n' % (tname,)
	columns = tables[table]
	for i in range(len(columns)):
		column = columns[i]
		str += '\t    "' + column[0] + " " + column[3]
		if i == len(columns) - 1:
			str += ')";'
		else:
			str += ',"'
		str += "\n"
	str += "}\n\n"
	source.write(str)


func = 'void CreateTPCESchema(duckdb::DuckDB &db, duckdb::Connection &con, std::string &schema, std::string &suffix)'
header.write(func + ';\n\n')
source.write(func + ' {\n')


# con.Query(RegionSchema(schema, suffix));

for table in tables.keys():
	tname = table.replace(' ', '_')
	source.write('\tcon.Query(%sSchema(schema, suffix));\n' %
		(table.title().replace(' ', '')))


source.write('}\n\n')



for fp in [header, source]:
	fp.write("} /* namespace TPCE */\n")
	fp.close()
