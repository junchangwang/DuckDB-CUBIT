
### Integrating CUBIT into DuckDB.

DuckDB is a column-based OLAP DBMS leveraging multicores. The original readme file can be found at README_DuckDB.md. We have integrated CUBIT into DuckDB to accelerate scan-intensive queries like TPC-H Q1 and Q6.

The CUBIT-powered DuckDB works as follows. Once DuckDB starts, it builds CUBIT instances for the specified attributes. When queries selecting on these attributes arrive, the planner generates query plans that use our new parallel executor. The executor (1) generates a segment of the resulting bitvector by ORing/ANDing the corresponding bitvector segments from each index, (2) transforms the resulting bitvector segment to a row ID list, and (3) probes physical pages in one pass. Thus, the new executor can naturally parallelize work using multiple cores. The rest of DuckDB is left unchanged.


### What is DuckDB-CUBIT?

The goal of this project (DuckDB-CUBIT) is to help you have a taste for using CUBIT to accelerate DuckDB. Currently, DuckDB-CUBIT supports TPC-H Q6 (SF=10) and allows users to perform the evaluation by typing one command.

Once start, DuckDB-CUBIT loads three CUBIT instances (respectively for the attributes l_shipdate, l_discount, and l_quantity) from disk files stored in the directories bm_15000000_*) to expedite the evaluation. Once a Q6 arrives, DuckDB-CUBIT assigns the query workload to a group of background threads. Each thread explicitly executes the parallel executor's workload for TPC-H Q6. Specifically, it (1) generates the resulting bitvector segments by using the CUBIT instances, (2) transforms the bitvector segments to ID lists, and (3) probes the underlying data (i.e., columns l_extendedprice and l_discount) in one pass.

The majority of the code implementing the above function is in PhysicalTableScan::GetData() in src/execution/operator/scan/physical_table_scan.cpp. Note that there are several code snippets for performance measurement. In particular, gen_perf_process() is to help you collect hardware characteristics like LLC and TLB misses. 


### How to compile?

- Compile CUBIT by using the command 
  ```
  cd ./src/include/duckdb/execution/index/cubit/CUBIT-PDS/
  ./build.sh
  ```
- Build the DuckDB-CUBIT project by using the command "make release"

### How to execute?

- Start DuckDB by using the command "./build/release/duckdb db_file"
- Load TPC-H module by using "LOAD tpch;"
- (The first time only) Generate TPC-H workload (SF=10) by using the command "call dbgen(sf=10);"
- Use the CUBIT-powered DuckDB to perform Q6 by simply typing "pragma tpch(6);"

