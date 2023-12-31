//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/constraints/list.hpp"

namespace duckdb {

void Constraint::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<ConstraintType>(100, "type", type);
}

unique_ptr<Constraint> Constraint::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<ConstraintType>(100, "type");
	unique_ptr<Constraint> result;
	switch (type) {
	case ConstraintType::CHECK:
		result = CheckConstraint::Deserialize(deserializer);
		break;
	case ConstraintType::FOREIGN_KEY:
		result = ForeignKeyConstraint::Deserialize(deserializer);
		break;
	case ConstraintType::NOT_NULL:
		result = NotNullConstraint::Deserialize(deserializer);
		break;
	case ConstraintType::UNIQUE:
		result = UniqueConstraint::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of Constraint!");
	}
	return result;
}

void CheckConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression", expression);
}

unique_ptr<Constraint> CheckConstraint::Deserialize(Deserializer &deserializer) {
	auto expression = deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression");
	auto result = duckdb::unique_ptr<CheckConstraint>(new CheckConstraint(std::move(expression)));
	return std::move(result);
}

void ForeignKeyConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<string>>(200, "pk_columns", pk_columns);
	serializer.WritePropertyWithDefault<vector<string>>(201, "fk_columns", fk_columns);
	serializer.WriteProperty<ForeignKeyType>(202, "fk_type", info.type);
	serializer.WritePropertyWithDefault<string>(203, "schema", info.schema);
	serializer.WritePropertyWithDefault<string>(204, "table", info.table);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(205, "pk_keys", info.pk_keys);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(206, "fk_keys", info.fk_keys);
}

unique_ptr<Constraint> ForeignKeyConstraint::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ForeignKeyConstraint>(new ForeignKeyConstraint());
	deserializer.ReadPropertyWithDefault<vector<string>>(200, "pk_columns", result->pk_columns);
	deserializer.ReadPropertyWithDefault<vector<string>>(201, "fk_columns", result->fk_columns);
	deserializer.ReadProperty<ForeignKeyType>(202, "fk_type", result->info.type);
	deserializer.ReadPropertyWithDefault<string>(203, "schema", result->info.schema);
	deserializer.ReadPropertyWithDefault<string>(204, "table", result->info.table);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(205, "pk_keys", result->info.pk_keys);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(206, "fk_keys", result->info.fk_keys);
	return std::move(result);
}

void NotNullConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WriteProperty<LogicalIndex>(200, "index", index);
}

unique_ptr<Constraint> NotNullConstraint::Deserialize(Deserializer &deserializer) {
	auto index = deserializer.ReadProperty<LogicalIndex>(200, "index");
	auto result = duckdb::unique_ptr<NotNullConstraint>(new NotNullConstraint(index));
	return std::move(result);
}

void UniqueConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WritePropertyWithDefault<bool>(200, "is_primary_key", is_primary_key);
	serializer.WriteProperty<LogicalIndex>(201, "index", index);
	serializer.WritePropertyWithDefault<vector<string>>(202, "columns", columns);
}

unique_ptr<Constraint> UniqueConstraint::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<UniqueConstraint>(new UniqueConstraint());
	deserializer.ReadPropertyWithDefault<bool>(200, "is_primary_key", result->is_primary_key);
	deserializer.ReadProperty<LogicalIndex>(201, "index", result->index);
	deserializer.ReadPropertyWithDefault<vector<string>>(202, "columns", result->columns);
	return std::move(result);
}

} // namespace duckdb
