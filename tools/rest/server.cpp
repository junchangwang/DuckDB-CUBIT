#include <chrono>
#include <cstdio>
#include <thread>
#include <iostream>

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "httplib.hpp"
#include "json.hpp"

#include <unordered_map>

using namespace httplib;
using namespace duckdb;
using namespace nlohmann;

void print_help() {
	fprintf(stderr, "🦆 Usage: duckdb_rest_server\n");
	fprintf(stderr, "         --listen=[address] listening address\n");
	fprintf(stderr, "         --port=[no]        listening port\n");
	fprintf(stderr, "         --database=[file]  use given database file\n");
	fprintf(stderr, "         --read_only        open database in read-only mode\n");
	fprintf(stderr, "         --log=[file]       log queries to file\n");
}

// https://stackoverflow.com/a/12468109/2652376
std::string random_string(size_t length) {
	auto randchar = []() -> char {
		const char charset[] = "0123456789"
		                       "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		                       "abcdefghijklmnopqrstuvwxyz";
		const size_t max_index = (sizeof(charset) - 1);
		return charset[rand() % max_index];
	};
	std::string str(length, 0);
	std::generate_n(str.begin(), length, randchar);
	return str;
}

// todo query cleanup time limit for streaming queries

struct RestClientState {
	unique_ptr<QueryResult> res;
	unique_ptr<Connection> con;
};

enum ReturnContentType { JSON, BSON, CBOR, MESSAGE_PACK, UBJSON };

void serialize_chunk(QueryResult *res, DataChunk *chunk, json &j) {
	assert(res);
	Vector v2;
	for (size_t col_idx = 0; col_idx < chunk->column_count; col_idx++) {
		auto *v = &chunk->data[col_idx];
		switch (res->sql_types[col_idx].id) {
		case SQLTypeId::DATE:
		case SQLTypeId::TIME:
		case SQLTypeId::TIMESTAMP:
			v2.Initialize(TypeId::VARCHAR, false);
			VectorOperations::Cast(*v, v2, res->sql_types[col_idx], SQLType::VARCHAR);
			v = &v2;
			break;
		default:
			break;
		}
		assert(v);
		switch (v->type) {
		case TypeId::BOOLEAN:
		case TypeId::TINYINT:
		case TypeId::SMALLINT:
		case TypeId::INTEGER:
		case TypeId::BIGINT:
			// int types
			v->Cast(TypeId::BIGINT);
			VectorOperations::Exec(*v, [&](index_t i, index_t k) {
				int64_t *data_ptr = (int64_t *)v->data;
				if (!v->nullmask[i]) {
					j["data"][col_idx] += data_ptr[i];

				} else {
					j["data"][col_idx] += nullptr;
				}
			});

			break;
		case TypeId::FLOAT:
		case TypeId::DOUBLE:
			v->Cast(TypeId::DOUBLE);

			VectorOperations::Exec(*v, [&](index_t i, index_t k) {
				double *data_ptr = (double *)v->data;
				if (!v->nullmask[i]) {
					j["data"][col_idx] += data_ptr[i];

				} else {
					j["data"][col_idx] += nullptr;
				}
			});

			break;
		case TypeId::VARCHAR:
			VectorOperations::Exec(*v, [&](index_t i, index_t k) {
				char **data_ptr = (char **)v->data;
				if (!v->nullmask[i]) {
					j["data"][col_idx] += data_ptr[i];

				} else {
					j["data"][col_idx] += nullptr;
				}
			});
			break;
		default:
			throw std::runtime_error("Unsupported Type");
		}
	}
}

void serialize_json(const Request &req, Response &resp, json &j) {

	// TODO should we compress?
	// zlib dependency uuuugh
	// CPPHTTPLIB_ZLIB_SUPPORT

	auto return_type = ReturnContentType::JSON;

	if (req.has_header("Accept")) {
		auto accept = req.get_header_value("Accept");
		if (accept.rfind("application/bson", 0) == 0 || accept.rfind("application/x-bson", 0) == 0) {
			return_type = ReturnContentType::BSON;
		} else if (accept.rfind("application/cbor", 0) == 0) {
			return_type = ReturnContentType::CBOR;
		} else if (accept.rfind("application/msgpack", 0) == 0 || accept.rfind("application/x-msgpack", 0) == 0 ||
		           accept.rfind("application/vnd.msgpack", 0) == 0) {
			return_type = ReturnContentType::MESSAGE_PACK;
		} else if (accept.rfind("application/ubjson", 0) == 0) {
			return_type = ReturnContentType::UBJSON;
		}
	}

	switch (return_type) {
	case ReturnContentType::JSON: {
		if (req.has_param("callback")) {
			auto jsonp_callback = req.get_param_value("callback");
			resp.set_content(jsonp_callback + "(" + j.dump() + ");", "application/javascript");

		} else {
			resp.set_content(j.dump(), "application/json");
		}
		break;
	}
	case ReturnContentType::BSON: {
		auto bson = json::to_bson(j);
		resp.set_content((const char *)bson.data(), bson.size(), "application/bson");
		break;
	}
	case ReturnContentType::CBOR: {
		auto cbor = json::to_cbor(j);
		resp.set_content((const char *)cbor.data(), cbor.size(), "application/cbor");
		break;
	}
	case ReturnContentType::MESSAGE_PACK: {
		auto msgpack = json::to_msgpack(j);
		resp.set_content((const char *)msgpack.data(), msgpack.size(), "application/msgpack");
		break;
	}
	case ReturnContentType::UBJSON: {
		auto ubjson = json::to_ubjson(j);
		resp.set_content((const char *)ubjson.data(), ubjson.size(), "application/ubjson");
		break;
	}
	}
}

void sleep_thread(Connection *conn, bool *is_active, int timeout_duration) {
	// timeout is given in seconds
	// we wait 10ms per iteration, so timeout * 100 gives us the amount of
	// iterations
	assert(conn);
	assert(is_active);

	if (timeout_duration < 0) {
		return;
	}
	for (size_t i = 0; i < (size_t)(timeout_duration * 100) && *is_active; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	if (*is_active) {
		conn->Interrupt();
	}
}

int main(int argc, char **argv) {
	Server svr;
	if (!svr.is_valid()) {
		printf("server has an error...\n");
		return -1;
	}

	std::mutex out_mutex;

	unordered_map<string, RestClientState> client_state_map;

	DBConfig config;
	string dbfile;
	string logfile_name;

	string listen = "localhost";
	int port = 1294;
	std::ofstream logfile;

	// parse config
	for (int arg_index = 1; arg_index < argc; ++arg_index) {
		string arg = argv[arg_index];
		if (arg == "--help") {
			print_help();
			exit(0);
		} else if (arg == "--read_only") {
			config.access_mode = AccessMode::READ_ONLY;
		} else if (StringUtil::StartsWith(arg, "--database=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			dbfile = string(splits[1]);
		} else if (StringUtil::StartsWith(arg, "--log=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			logfile_name = string(splits[1]);
		} else if (StringUtil::StartsWith(arg, "--listen=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			listen = string(splits[1]);
		} else if (StringUtil::StartsWith(arg, "--port=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			port = std::stoi(splits[1]);
			;
		} else {
			fprintf(stderr, "Error: unknown argument %s\n", arg.c_str());
			print_help();
			exit(1);
		}
	}

	if (!logfile_name.empty()) {
		logfile.open(logfile_name, std::ios_base::app);
	}
	DuckDB duckdb(dbfile.empty() ? nullptr : dbfile, &config);

	svr.Get("/query", [&duckdb, &client_state_map, &out_mutex, &logfile](const Request &req, Response &resp) {
		auto q = req.get_param_value("q");

		{
			std::lock_guard<std::mutex> guard(out_mutex);
			logfile << q << " ; -- DFgoEnx9UIRgHFsVYW8K" << std::endl
			        << std::flush; // using a terminator that will **never** occur in queries
		}

		json j;

		RestClientState state;
		state.con = make_unique<Connection>(duckdb);
		state.con->EnableProfiling();
		bool is_active = true;

		std::thread interrupt_thread(sleep_thread, state.con.get(), &is_active, 60);
		auto res = state.con->context->Query(q, true);

		is_active = false;
		interrupt_thread.join();

		state.res = move(res);

		if (state.res->success) {
			j = {{"query", q},
			     {"success", state.res->success},
			     {"column_count", state.res->types.size()},
			     {"statement_type", StatementTypeToString(state.res->statement_type)},
			     {"names", json(state.res->names)},
			     {"name_index_map", json::object()},
			     {"types", json::array()},
			     {"sql_types", json::array()},
			     {"data", json::array()}};

			for (auto &sql_type : state.res->sql_types) {
				j["sql_types"] += SQLTypeToString(sql_type);
			}
			for (auto &type : state.res->types) {
				j["types"] += TypeIdToString(type);
			}

			// make it easier to get col data by name
			size_t col_idx = 0;
			for (auto &name : state.res->names) {
				j["name_index_map"][name] = col_idx;
				col_idx++;
			}

			// only do this if query was successful
			string query_ref = random_string(10);
			j["ref"] = query_ref;
			auto chunk = state.res->Fetch();
			serialize_chunk(state.res.get(), chunk.get(), j);
			client_state_map[query_ref] = move(state);

		} else {
			j = {{"query", q}, {"success", state.res->success}, {"error", state.res->error}};
			resp.status = 400;
		}

		serialize_json(req, resp, j);
	});

	svr.Get("/fetch", [&duckdb, &client_state_map](const Request &req, Response &resp) {
		auto ref = req.get_param_value("ref");
		Connection conn(duckdb);
		json j;

		if (client_state_map.find(ref) != client_state_map.end()) {
			auto &state = client_state_map[ref];
			auto chunk = state.res->Fetch();
			j = {{"success", true}, {"ref", ref}, {"count", chunk->data[0].count}, {"data", json::array()}};

			serialize_chunk(state.res.get(), chunk.get(), j);

			if (chunk->data[0].count == 0) {
				client_state_map.erase(client_state_map.find(ref));
			}

		} else {
			j = {{"success", false}, {"error", "Unable to find ref."}};
			resp.status = 400;
		}

		serialize_json(req, resp, j);
	});

	svr.Get("/close", [&duckdb, &client_state_map](const Request &req, Response &resp) {
		auto ref = req.get_param_value("ref");
		Connection conn(duckdb);
		json j;

		if (client_state_map.find(ref) != client_state_map.end()) {
			client_state_map.erase(client_state_map.find(ref));
			j = {{"success", true}, {"ref", ref}};
		} else {
			j = {{"success", false}, {"error", "Unable to find ref."}};
			resp.status = 400;
		}

		serialize_json(req, resp, j);
	});

	std::cout << "🦆 listening on http://" + listen + ":" + std::to_string(port) + "\n";

	svr.listen(listen.c_str(), port);
	return 0;
}
