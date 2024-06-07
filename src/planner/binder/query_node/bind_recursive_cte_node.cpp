#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

unique_ptr<BoundQueryNode> Binder::BindNode(RecursiveCTENode &statement) {
	auto result = make_uniq<BoundRecursiveCTENode>();

	// first recursively visit the recursive CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);
	result->ctename = statement.ctename;
	result->union_all = statement.union_all;
	result->setop_index = GenerateTableIndex();

	for (idx_t i = 0; i < statement.key_targets.size(); ++i) {
		result->key_targets.push_back(statement.key_targets[i]);
	}

	result->left_binder = Binder::CreateBinder(context, this);
	result->left = result->left_binder->BindNode(*statement.left);

	// the result types of the CTE are the types of the LHS
	result->types = result->left->types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result->names = result->left->names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result->names.size(); i++) {
		result->names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);

	result->right_binder = Binder::CreateBinder(context, this);

	if (!statement.key_targets.empty()) {
		result->recurring_index = GenerateTableIndex();
		// Retrieves the recursive CTE information in order to add it to the own context for the recurring table.
		optional_ptr<CommonTableExpressionInfo> current_cte_info = FindCTE(statement.ctename);
		AddCTE("recurring", *current_cte_info);
		result->right_binder->bind_context.AddCTEBinding(result->recurring_index, "recurring", result->names,
		                                                 result->types);
	}

	// Add bindings of left side to temporary CTE bindings context
	result->right_binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, result->names,
	                                                 result->types);
	result->right = result->right_binder->BindNode(*statement.right);
	for (auto &c : result->left_binder->correlated_columns) {
		result->right_binder->AddCorrelatedColumn(c);
	}

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	MoveCorrelatedExpressions(*result->right_binder);

	// now both sides have been bound we can resolve types
	if (result->left->types.size() != result->right->types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (!statement.modifiers.empty()) {
		throw NotImplementedException("FIXME: bind modifiers in recursive CTE");
	}

	return std::move(result);
}

} // namespace duckdb
