//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {
class SchemaCatalogEntry;

struct CreateViewInfo : public CreateInfo {
	CreateViewInfo();
	CreateViewInfo(SchemaCatalogEntry &schema, string view_name);
	CreateViewInfo(string catalog_p, string schema_p, string view_name);

	//! Table name to insert to
	string view_name;
	//! Aliases of the view
	vector<string> aliases;
	//! Return types
	vector<LogicalType> types;
	//! The SelectStatement of the view
	unique_ptr<SelectStatement> query;

public:
	unique_ptr<CreateInfo> Copy() const override;

	static unique_ptr<CreateViewInfo> Deserialize(Deserializer &deserializer);

	//! Gets a bound CreateViewInfo object from a SELECT statement and a view name, schema name, etc
	DUCKDB_API static unique_ptr<CreateViewInfo> FromSelect(ClientContext &context, unique_ptr<CreateViewInfo> info);
	//! Gets a bound CreateViewInfo object from a CREATE VIEW statement
	DUCKDB_API static unique_ptr<CreateViewInfo> FromCreateView(ClientContext &context, const string &sql);

	DUCKDB_API void FormatSerialize(FormatSerializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> FormatDeserialize(FormatDeserializer &deserializer);

protected:
	void SerializeInternal(Serializer &serializer) const override;
};

} // namespace duckdb
