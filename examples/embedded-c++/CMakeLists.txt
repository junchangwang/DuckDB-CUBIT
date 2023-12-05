cmake_minimum_required(VERSION 2.8.12)
project(example-c++)

set(CMAKE_CXX_STANDARD 17)

include_directories(../../src/include/duckdb/execution/index/cubit/CUBIT-PDS/src)
include_directories(../../src/include/duckdb/execution/index/cubit/CUBIT-PDS/src/nbub)
include_directories(../../src/include)
link_directories(../../build/release/src)

add_executable(example main.cpp)
target_link_libraries(example duckdb)
