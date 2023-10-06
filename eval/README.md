# Evaluation Data
This page presents some raw evaluation data for the RelJoin paper. 

## Testbeds and Benchmark Settings
We deploy SparkSQL running on YARN in a cluster of 6 computer nodes, 
where each node is equipped with 12 CPU cores at 2.6 GHz and 64 GB memory. 
One node is configured as the HDFS name node and the YARN resource manager. 
The other 5 nodes are configured as HDFS data nodes and YARN node managers. 
Each node manager is allocated 16GB memory and 8 CPU cores The SparkSQL jobs 
are submitted in the YARN client mode, 
running in 10 executors with 4 GB memory each. 
The distributed join parallelism is 20 and the Kyro library is used as the serializer 
for the shuffling and broadcasting I/O.

We use the TPC-DS benchmark to evaluate the performance of RelJoin. 
TPC-DS is a representative bench- mark with complex decision workloads 
for general big data processing systems. 
We run 97 test queries integrated by SparkSQL excluding some flaky tests. 
Unless otherwise specified, we test with w = 1.
The size scale of the benchmark can be 1, 10, and 100, with the details listed in the following table.
Datasets are transformed to the parquet format. 
Each query runs three times for all tests and the average results are presented.

|Scale | Total Size | Largest Size | Node | Executor |
| 1 | 1.2 GB | 386 MB | 16 GB | 4 GB/1 core |
| 10 | 12 GB | 3.7 GB | 32 GB | 14 GB/2 cores |
| 100 | 96 GB | 38 GB | 32 GB | 14 GB/2 cores |

We compare the query completion time of RelJoin with that of various 
distributed join method strategies, namely ShuffleSort, ShuffleHash, and AQE. 
The detailed descriptions of the strategies are listed in the table.

| Strategy | Description|
|----------|------------|
|ShuffleSort|Force to select the shuffle sort join if keys are sortable.|
|ShuffleHash|Force to select the shuffle hash join if the dataset is small enough for building the hash.|
|AQE|Select the broadcast hash join if the size statistics of a dataset does not exceed 10MB. Otherwise, select the shuffle hash or shuffle sort join.|
|RelJoin|Network workload weight w = 1|

## Data
### Query completion time
* Results of query completion completion time running ShuffleSort, ShuffleHash, AQE, and RelJoin(w=1)
in the TPC-DS benchmark of different size scales three times: 

|Strategy | scale-1 | scale-10 | scale-100|
|---------|---------|----------|----------|
|ShuffleSort | [queryShuffleSort.txt](./data/queryShuffleSort.txt) |[queryShuffleSortScale10.txt](./data/queryShuffleSortScale10.txt) | [queryShuffleSortScale100.txt](./data/queryShuffleSortScale100.txt)|
|ShuffleHash | [queryShuffleHash.txt](./data/queryShuffleHash.txt) |[queryShuffleHashScale10.txt](./data/queryShuffleHashScale10.txt) | [queryShuffleHashScale100.txt](./data/queryShuffleHashScale100.txt)|
|AQE         | [queryAQE.txt](./data/queryAQE.txt)                 |[queryAQEScale10.txt](./data/queryAQEScale10.txt)  |[queryAQEScale100.txt](./data/queryAQEScale100.txt)  |
|RelJoin     | [queryRelJoin.txt](./data/queryRelJoin.txt)         |[queryRelJoinScale10.txt](./data/queryRelJoinScale10.txt)  |[queryRelJoinScale100.txt](./data/queryRelJoinScale100.txt)  |

* Results of query completion completion time running RelJoin w = 10 three times: 
[queryRelJoinW10.txt](./data/queryRelJoinW10.txt).
* Results of query completion completion time running RelJoin w = 100000 three times: 
[queryRelJoinW10.txt](./data/queryRelJoinW100000.txt).
* Results of query completion completion time running RelJoin w = 0.1 three times: 
[queryRelJoinW10.txt](./data/queryRelJoinW0.1.txt).

### Query plans
* Optimized logical plans of all queries: 
[optimizedLogicalPlans.txt](./data/optimizedLogicalPlans.txt).
* Final physical plans of all queries by RelJoin: 
[RelJoinFinalPhysicalPlan.txt](./data/RelJoinFinalPhysicalPlan.txt).
* Final physical plans of all queries by AQE: 
[AQEFinalPhysicalPlan.txt](./data/AQEFinalPhysicalPlan.txt).



