
About this project (CUBIT-powered DuckDB)
-----------------------------------------

We have integrated CUBIT into DuckDB to accelerate analytical queries like TPC-H Q1 and Q6. Our experimental results show that for selective queries, the resulting system (CUBIT-powered DuckDB) outperforms DuckDB's highly optimized scan by 2x, demonstrating that CUBIT is a promising indexing candidate for this use case.

Note that DuckDB is a column-based OLAP DBMS that fully exploits multicores. Its original readme information can be found in README_DuckDB.md. 


How CUBIT-powered DuckDB works?
-------------------------------

The CUBIT-powered DuckDB works as follows. 
- Once DuckDB starts, it builds CUBIT instances for the specified attributes. 
- When queries selecting these attributes arrive, the DBMS planner generates query plans that use our new parallel executor. 
- The executor (1) generates a segment of the resulting bitvector by ORing/ANDing the corresponding bitvector segments from CUBIT instances, (2) transforms the resulting bitvector segment to a row ID list, and (3) probes physical pages in one pass. 
- The rest of DuckDB is left unchanged.

This repository (DuckDB-CUBIT) aims to help you verify the above-described design and replicate the experiments (TPC-H Q6 with SF=10) in our paper.

When started, DuckDB-CUBIT loads three CUBIT instances (respectively for the attributes l_shipdate, l_discount, and l_quantity) from disk files. (Note that CUBIT instances can also be built on-the-fly. However, that takes hours). Once a Q6 arrives, DuckDB assigns the query workload to a group of background threads. Each thread explicitly executes the parallel executor's workload for TPC-H Q6. Specifically, it (1) generates the resulting bitvector segments by using the CUBIT instances, (2) transforms the bitvector segments to ID lists, and (3) probes the underlying data (i.e., columns l_extendedprice and l_discount) in one pass.

The majority of the code implementing the above function is in PhysicalTableScan::GetData() and IndexRead() in src/execution/operator/scan/physical_table_scan.cpp. Note that several code snippets are provided for performance evaluation. In particular, gen_perf_process() helps you collect hardware characteristics like LLC and TLB misses. 


How to compile?
---------------

- Compile CUBIT by using the command 
  ```
  cd ../
  ./build.sh
  ```
- Build the DuckDB-CUBIT project by using the command "make release"


How to execute?
---------------

- Start DuckDB by using the command "./build/release/duckdb db_file"
- Load TPC-H module by using the command "LOAD tpch;"
- (The first time only) Generate TPC-H workload (SF=10) by using the command "call dbgen(sf=10);"
- Perform a TPC-H Q6 query on CUBIT-powered DuckDB by simply typing "pragma tpch(6);"

