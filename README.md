
About this project (CUBIT-powered DuckDB)
-----------------------------------------

We have integrated CUBIT into DuckDB and conducted experiments on 12 out of 22 TPC-H queries, including the scan-intensive (Q6 and Q12), aggregation-heavy (Q1 and Q18), and join-dominant (Q3, Q4, Q5, Q10, Q14, Q15, Q17, and Q19) queries. We excluded other queries that either do not involve the fact table LINEITEM (e.g., Q2) or do not present obvious choke points for query engines (e.g., Q16). Our experimental results demonstrate that our CUBIT-powered query engine notably accelerates analytical queries, delivering a 1.2x - 2.7x query improvement over DuckDB's native approaches.


How CUBIT-powered DuckDB works?
-------------------------------

We have described in detail in the paper how CUBIT accelerates various operators. This repository (DuckDB-CUBIT) aims to help you replicate the experiments (SF = 10).

Our CUBIT-power query engine leverages pre-built CUBIT instances to accelerate the Scan, Join, and Aggregation operators. For example, for Q6, when DuckDB-CUBIT starts, it loads three CUBIT instances (for the attributes l_shipdate, l_discount, and l_quantity) from disk files. (Note that while CUBIT instances can also be built on-the-fly, this process is time-consuming). Once Q6 is initiated, it (1) generates the resulting bitvector segments using the CUBIT instances, (2) transforms the bitvector segments to ID lists, and (3) probes the underlying data (i.e., columns l_extendedprice and l_discount) in one pass.

The majority of the code the BIT-powered query engine can be found in src/execution/operator/scan/physical_table_scan.cpp. Note that several code snippets are provided for performance evaluation. In particular, gen_perf_process() assists in collecting hardware characteristics like LLC and TLB misses. 


Compile the project
-------------------

- Compile CUBIT by using the command 
  
  `cd ../  &&   ./build.sh`
  
- Build the DuckDB-CUBIT project by using the command 

    `make release`


Retrieve/Build the CUBIT instances
----------------------------------

- You can use CUBIT to pre-build all the CUBIT instances required by the query engine (please see the README.md file of the CUBIT repository). However, building these instances for large datasets (e.g., when SF = 10) can take hours. Therefore, we strongly recommend downloading our pre-built CUBIT instances using the following command.

  `./getbitmaps.sh`

How to execute?
---------------

- Start DuckDB by using the command 

    `./build/release/duckdb db_file`

- Generate TPC-H workload (SF=10) by using the command (the first time only) 

    `call dbgen(sf=10);`

- Set the number of worker threads to 1 by using the command 
  `set threads to 1;`
  
- It is helpful to output DuckDB's statistics by using the command

  `SET enable_profiling = 'query_tree_optimizer';`

- To perform TPC-H queries including Q6, Q12, Q14, Q15, Q18, and Q19, simply type 

    `pragma tpch(Q);`

    where Q is the query ID that you want to execute. For other queries, you can specify the execution of a query at the beginning of the function PhysicalTableScan::GetData() in physical_table_scan.cpp() to replicate the experiments. We are working on integrating these queries into DuckDB's standard workflow.

