#include "parser/expression/constant_expression.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace std;

bool Transformer::TransformOrderBy(postgres::List *order, vector<OrderByNode> &result) {
	if (!order) {
		return false;
	}

	for (auto node = order->head; node != nullptr; node = node->next) {
		auto temp = reinterpret_cast<postgres::Node *>(node->data.ptr_value);
		if (temp->type == postgres::T_SortBy) {
			OrderByNode ordernode;
			auto sort = reinterpret_cast<postgres::SortBy *>(temp);
			auto target = sort->node;
			if (sort->sortby_dir == postgres::SORTBY_ASC || sort->sortby_dir == postgres::SORTBY_DEFAULT) {
				ordernode.type = OrderType::ASCENDING;
			} else if (sort->sortby_dir == postgres::SORTBY_DESC) {
				ordernode.type = OrderType::DESCENDING;
			} else {
				throw NotImplementedException("Unimplemented order by type");
			}
			ordernode.expression = TransformExpression(target);
			result.push_back(OrderByNode(ordernode.type, move(ordernode.expression)));
		} else {
			throw NotImplementedException("ORDER BY list member type %d\n", temp->type);
		}
	}
	return true;
}
