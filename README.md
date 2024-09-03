
About this project (CUBIT-powered DuckDB)
-----------------------------------------

We have integrated CUBIT into DuckDB to accelerate analytical queries, and conducted experiments on 12 out of 22 TPC-H queries, including the scan-intensive (Q6 and Q12), aggregation-heavy (Q1 and Q18), and join-dominant (Q3, Q4, Q5, Q10, Q14, Q15, Q17, and Q19) queries. We omitted other queries that neither involve the fact table LINEITEM (e.g., Q2) nor present obvious choke points for query engines (e.g., Q16). Our experimental results demonstrate that our CUBIT-powered query engine is 1.2x - 2.7x faster than DuckDB's native approaches.

Note that DuckDB is a column-based OLAP DBMS that fully exploits multicores. Its original readme information can be found in README_DuckDB.md. 


How CUBIT-powered DuckDB works?
-------------------------------

We have described in detail how CUBIT accelerate various operators. This repository (DuckDB-CUBIT) aims to help you verify our CUBIT-powered operators and replicate the experiments (SF = 10).

For example, for Q6, when DuckDB-CUBIT starts, it loads three CUBIT instances (for the attributes l_shipdate, l_discount, and l_quantity) from disk files. (Note that CUBIT instances can also be built on-the-fly, but this process is time-consuming). Once Q6 is initiated, it (1) generates the resulting bitvector segments using the CUBIT instances, (2) transforms the bitvector segments to ID lists, and (3) probes the underlying data (i.e., columns l_extendedprice and l_discount) in one pass.

The majority of the code for CUBIT-powered query engine can be found in src/execution/operator/scan/physical_table_scan.cpp. Note that several code snippets are provided for performance evaluation. In particular, gen_perf_process() assists in collecting hardware characteristics like LLC and TLB misses. 


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
- To perform a TPC-H query on CUBIT-powered DuckDB, simply type "pragma tpch(Q);" where Q is the query ID that you want to execute.

