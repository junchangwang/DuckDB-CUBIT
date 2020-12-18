#include "duckdb/main/connection_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

ConnectionManager::~ConnectionManager() {
	std::lock_guard<std::mutex> lock(connections_lock);
	for (auto &conn : connections) {
		conn->context->Invalidate();
	}
}

void ConnectionManager::AddConnection(Connection *conn) {
	D_ASSERT(conn);
	std::lock_guard<std::mutex> lock(connections_lock);
	connections.insert(conn);
}

void ConnectionManager::RemoveConnection(Connection *conn) {
	D_ASSERT(conn);
	std::lock_guard<std::mutex> lock(connections_lock);
	connections.erase(conn);
}

} // namespace duckdb
