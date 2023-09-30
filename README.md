# RelJoin

RelJoin implements a cost-based distributed join method optimization rule on top 
of Spark SQL. It selects optimal join methods for logical joins when planning 
the physical plan for an optimized logical plan. It replaces the original
"JoinSelection" rule. 
The RelJoin feature is enabled by setting the configuration 
option "spark.sql.adaptive.cost.join.enabled"
true. 

This project is a fork of the [Spark project](https://github.com/apache/spark).


## Prepare
The project can run locally or in distributed data processing platforms such as YARN. 
If you want to run it on YARN with Hadoop distributed file system(HDFS), 
you need to have YARN properly deployed in a cluster.
Please refer to the
[Hadoop website](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)
to install and setup a YARN and HDFS cluster. 
The default Hadoop version matching this project is v3.3.2.

When HDFS and YARN are ready, 
the YARN home path is exported as $HADOOP_HOME.
Start the HDFS and YARN cluster.  

```bash
$HADOOP_HOME/sbin/start-all.sh
```


## Compile and Deploy

Compile the Spark project with Hadoop and YARN support. $SPARK_HOME is the path of the project directory. 

```bash
cd $SPARK_HOME && build/sbt -Pyarn -Dhadoop.version=3.3.2 package
```

Deploy Spark in the cluster can be as simple as copying the built project into the same path 
in every node in the YARN cluster.

For other details about deploying and configurating Spark on YARN, refer to 
the [Spark deploying page](https://spark.apache.org/docs/latest/running-on-yarn.html).

## Generate TPC-DS datasets.
The TPC-DS dataset generater project is integrated as a submodule in this project. 
Download the TPC-DS dataset generater by updating the submodule.
```bash
git submodule update
```

Make the TPC-DS generater project, providing the type of the operating system. 
For example, if it is built in Linux or Mac OSX, run the following command. 
Note that neccessary compiling tools are needed for comiplation 
depending on the operating system. 
Please refer to: 
https://github.com/gregrahn/tpcds-kit
```bash
cd tpcds-kit/tools
# Linux (e.g. ubuntu)
sudo apt-get install gcc make flex bison byacc git
make OS=LINUX
# Mac OSX
xcode-select --install
make OS=MACOS
```

Create the directory for the datasets, and generate a unit-scaled TPC-DS dataset.

```bash
cd $SPARK_HOME
mkdir ~/benchmark/tpcds/
tpcds-kit/tools/dsdgen -dir ~/benchmark/tpcds/ -DISTRIBUTIONS $SPARK_HOME/tpcds-kit/tools/tpcds.idx -scale 1 -verbose y -terminate n
```

Transform the dataset format to parquet for Spark SQL queries. 
```bash
build/sbt "sql/test:runMain org.apache.spark.sql.GenTPCDSDataFromFile --dsdgenDir [absolute_path_to ~/benchmark/tpcds] --location [absolute_path_to ~/benchmark/tpcdsTable] --scaleFactor n"
```

For example:
```bash
build/sbt "sql/test:runMain org.apache.spark.sql.GenTPCDSDataFromFile --dsdgenDir /home/xxx/benchmark/tpcds --location /home/xxx/benchmark/tpcdsTable --scaleFactor 1"
```


If you run the project in the YARN cluster, upload the dataset to HDFS.
```bash
$HADOOP_HOME/bin/hadoop fs -mkdir /benchmark
$HADOOP_HOME/bin/hadoop fs -put -d ~/benchmark/tpcdsTable/ /benchmark/
```

## Run the benchmark
To execute TPC-DS queries with different join method selection strategies, run command in the following format. 

```bash
$SPARK_HOME/bin/spark-submit run-example [SPARK_OPTIONS] org.apache.spark.examples.sql.TPCDSRun [DATASET_DIR] [COMMAND] [JOIN_STRATEGY] [RUN_TIMES]
```

The COMMAND can be "execute" or "explain", where "explain" will output the logical and physical query plans to the console. 
The JOIN_STRATEGY can be "ShuffleSortJoin", "ShuffleHashJoin", "AQEJoin", "RelJoin", "RelJoinW10", "RelJoinW100".
For example, to run RelJoin 3 times in the YARN cluster in the client mode, run: 

```bash
$SPARK_HOME/bin/spark-submit run-example --master yarn --executor-memory 4G --num-executors 10 org.apache.spark.examples.sql.TPCDSRun hdfs:///benchmark/tpcdsTable execute RelJoin 3
```

If you want to view the optimized logical query plan and the physical plan, 
run with the "explain" option.

```bash
$SPARK_HOME/bin/spark-submit run-example --master yarn --executor-memory 4G --num-executors 10 org.apache.spark.examples.sql.TPCDSRun hdfs:///benchmark/tpcdsTable explain RelJoin 1
```

You can also run the benchmark locally by specifying the local path of the dataset. 

```bash
$SPARK_HOME/bin/spark-submit run-example org.apache.spark.examples.sql.TPCDSRun ~/home/benchmark/tpcdsTable execute RelJoin 3
```

## Evaluation Result Data
The raw data result in the RelJoin paper can be found in [./eval](./eval/README.md).
