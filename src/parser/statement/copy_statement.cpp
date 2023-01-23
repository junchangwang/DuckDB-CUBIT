#include "duckdb/parser/statement/copy_statement.hpp"

namespace duckdb {

CopyStatement::CopyStatement() : SQLStatement(StatementType::COPY_STATEMENT), info(make_unique<CopyInfo>()) {
}

CopyStatement::CopyStatement(const CopyStatement &other) : SQLStatement(other), info(other.info->Copy()) {
	if (other.select_statement) {
		select_statement = other.select_statement->Copy();
	}
}

string ConvertOptionValueToString(const Value &val) {
	auto type = val.type().id();
	switch (type) {
	case LogicalTypeId::VARCHAR:
		return KeywordHelper::WriteOptionallyQuoted(val.ToString());
	default:
		return val.ToString();
	}
}

string CopyStatement::CopyOptionsToString(const unordered_map<string, vector<Value>> &options) const {
	if (options.empty()) {
		return string();
	}
	string result;

	result += " (";
	for (auto it = options.begin(); it != options.end(); it++) {
		if (it != options.begin()) {
			result += ", ";
		}
		auto &name = it->first;
		auto &values = it->second;

		result += name + " ";
		D_ASSERT(!values.empty());
		if (values.size() == 1) {
			result += ConvertOptionValueToString(values[0]);
		} else {
			result += "( ";
			for (idx_t i = 0; i < values.size(); i++) {
				auto &value = values[i];
				if (i) {
					result += ", ";
				}
				result += KeywordHelper::WriteOptionallyQuoted(value.ToString());
			}
			result += " )";
		}
	}
	result += " )";
	return result;
}

// COPY table-name (c1, c2, ..)
string TablePart(const CopyInfo &info) {
	string result;

	if (!info.catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(info.catalog) + ".";
	}
	if (!info.schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(info.schema) + ".";
	}
	D_ASSERT(!info.table.empty());
	result += KeywordHelper::WriteOptionallyQuoted(info.table);

	// (c1, c2, ..)
	if (!info.select_list.empty()) {
		result += " (";
		for (idx_t i = 0; i < info.select_list.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += KeywordHelper::WriteOptionallyQuoted(info.select_list[i]);
		}
		result += " )";
	}
	return result;
}

string CopyStatement::ToString() const {
	string result;

	result += "COPY ";
	if (info->is_from) {
		D_ASSERT(!select_statement);
		result += TablePart(*info);
		result += " FROM";
		result += StringUtil::Format("'%s'", info->file_path);
		result += CopyOptionsToString(info->options);
	} else {
		if (select_statement) {
			// COPY (select-node) TO ...
			result += "(" + select_statement->ToString() + ")";
		} else {
			result += TablePart(*info);
		}
		result += " TO";
		result += StringUtil::Format("'%s'", info->file_path);
		result += CopyOptionsToString(info->options);
	}
	return result;
}

unique_ptr<SQLStatement> CopyStatement::Copy() const {
	return unique_ptr<CopyStatement>(new CopyStatement(*this));
}

bool CopyStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (const CopyStatement &)*other_p;

	if (!other.info->Equals(info.get())) {
		return false;
	}

	if (!other.select_statement && !select_statement) {
		return true;
	}
	if (!other.select_statement || !select_statement) {
		return false;
	}
	return select_statement->Equals(other.select_statement.get());
}

} // namespace duckdb
