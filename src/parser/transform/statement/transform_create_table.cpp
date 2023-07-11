#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

string Transformer::TransformCollation(optional_ptr<duckdb_libpgquery::PGCollateClause> collate) {
	if (!collate) {
		return string();
	}
	string collation;
	for (auto c = collate->collname->head; c != nullptr; c = lnext(c)) {
		auto pgvalue = PGPointerCast<duckdb_libpgquery::PGValue>(c->data.ptr_value);
		if (pgvalue->type != duckdb_libpgquery::T_PGString) {
			throw ParserException("Expected a string as collation type!");
		}
		auto collation_argument = string(pgvalue->val.str);
		if (collation.empty()) {
			collation = collation_argument;
		} else {
			collation += "." + collation_argument;
		}
	}
	return collation;
}

OnCreateConflict Transformer::TransformOnConflict(duckdb_libpgquery::PGOnCreateConflict conflict) {
	switch (conflict) {
	case duckdb_libpgquery::PG_ERROR_ON_CONFLICT:
		return OnCreateConflict::ERROR_ON_CONFLICT;
	case duckdb_libpgquery::PG_IGNORE_ON_CONFLICT:
		return OnCreateConflict::IGNORE_ON_CONFLICT;
	case duckdb_libpgquery::PG_REPLACE_ON_CONFLICT:
		return OnCreateConflict::REPLACE_ON_CONFLICT;
	default:
		throw InternalException("Unrecognized OnConflict type");
	}
}

unique_ptr<ParsedExpression> Transformer::TransformCollateExpr(duckdb_libpgquery::PGCollateClause &collate) {
	auto child = TransformExpression(collate.arg);
	auto collation = TransformCollation(&collate);
	return make_uniq<CollateExpression>(collation, std::move(child));
}

ColumnDefinition Transformer::TransformColumnDefinition(duckdb_libpgquery::PGColumnDef &cdef) {
	string colname;
	if (cdef.colname) {
		colname = cdef.colname;
	}
	bool is_generated = cdef.category == duckdb_libpgquery::COL_GENERATED;
	LogicalType target_type = (is_generated && !cdef.typeName) ? LogicalType::ANY : TransformTypeName(*cdef.typeName);

	auto name = PGPointerCast<duckdb_libpgquery::PGValue>((*cdef.typeName).names->tail->data.ptr_value)->val.str;
	LogicalTypeId base_type = TransformStringToLogicalTypeId(name);
	bool is_serial = SerialColumnType::IsColumnSerial(base_type, name);

	TableColumnType col_category_type = TableColumnType::STANDARD;
	if (is_generated) {
		col_category_type = TableColumnType::GENERATED;
	} else if (is_serial) {
		col_category_type = TableColumnType::SERIAL;
	}

	if (cdef.collClause) {
		if (cdef.category == duckdb_libpgquery::COL_GENERATED) {
			throw ParserException("Collations are not supported on generated columns");
		}
		if (is_serial) {
			throw ParserException("Collations are not supported on serial columns");
		}
		if (target_type.id() != LogicalTypeId::VARCHAR) {
			throw ParserException("Only VARCHAR columns can have collations!");
		}
		target_type = LogicalType::VARCHAR_COLLATION(TransformCollation(cdef.collClause));
	}

	return ColumnDefinition(colname, target_type, col_category_type);
}

unique_ptr<CreateStatement> Transformer::TransformCreateTable(duckdb_libpgquery::PGCreateStmt &stmt) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTableInfo>();

	if (stmt.inhRelations) {
		throw NotImplementedException("inherited relations not implemented");
	}
	D_ASSERT(stmt.relation);

	info->catalog = INVALID_CATALOG;
	auto qname = TransformQualifiedName(*stmt.relation);
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->table = qname.name;
	info->on_conflict = TransformOnConflict(stmt.onconflict);
	info->temporary =
	    stmt.relation->relpersistence == duckdb_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;

	if (info->temporary && stmt.oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_PRESERVE_ROWS &&
	    stmt.oncommit != duckdb_libpgquery::PGOnCommitAction::PG_ONCOMMIT_NOOP) {
		throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
	}
	if (!stmt.tableElts) {
		throw ParserException("Table must have at least one column!");
	}

	idx_t column_count = 0;
	for (auto c = stmt.tableElts->head; c != nullptr; c = lnext(c)) {
		auto node = PGPointerCast<duckdb_libpgquery::PGNode>(c->data.ptr_value);
		switch (node->type) {
		case duckdb_libpgquery::T_PGColumnDef: {
			auto cdef = PGPointerCast<duckdb_libpgquery::PGColumnDef>(c->data.ptr_value);
			auto centry = TransformColumnDefinition(*cdef);
			if (centry.SERIAL()) {
				string seq_name = info->table + "_" + cdef->colname + "_seq";
				auto sequence = SerialColumnType::makeSequence(seq_name, centry.Type());
				info->sequences.push_back(std::move(sequence));

				// serial implies not null as well
				LogicalIndex index(info->columns.LogicalColumnCount());
				info->constraints.push_back(make_uniq<NotNullConstraint>(index));

				// make the default value arg for the serial column
				vector<unique_ptr<ParsedExpression>> children;
				children.push_back(make_uniq<ConstantExpression>(Value(seq_name)));
				auto nextval_expr = make_uniq<FunctionExpression>("nextval", std::move(children));
				auto alter_edata =
				    AlterEntryData(qname.catalog, qname.schema, qname.name, OnEntryNotFound::THROW_EXCEPTION);
				auto col_default = make_uniq<SetDefaultInfo>(alter_edata, cdef->colname, std::move(nextval_expr));
				info->col_defaults.push_back(std::move(col_default));
			}
			if (cdef->constraints) {
				for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
					auto constraint = TransformConstraint(constr, centry, info->columns.LogicalColumnCount());
					if (constraint) {
						info->constraints.push_back(std::move(constraint));
					}
				}
			}
			info->columns.AddColumn(std::move(centry));
			column_count++;
			break;
		}
		case duckdb_libpgquery::T_PGConstraint: {
			info->constraints.push_back(TransformConstraint(c));
			break;
		}
		default:
			throw NotImplementedException("ColumnDef type not handled yet");
		}
	}

	if (!column_count) {
		throw ParserException("Table must have at least one column!");
	}

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
