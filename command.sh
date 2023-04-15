// cd spark && build/sbt -Pyarn -Phadoop-provided -Dhadoop.version=3.3.2 package
cd spark && build/sbt -Pyarn -Dhadoop.version=3.3.2 package

mkdir ~/benchmark/tpcds/
tpcds-kit/tools/dsdgen -dir ~/benchmark/tpcds/ -scale 1 -verbose y -terminate n

build/sbt "sql/test:runMain org.apache.spark.sql.GenTPCDSDataFromFile --dsdgenDir /home/liang/benchmark/tpcds50 --location /home/liang/benchmark/tpcdsTable50 --scaleFactor 50"

hadoop fs -mkdir /benchmark

hadoop fs -put -d ~/benchmark/tpcdsTable/ /benchmark/

SPARK_TPCDS_DATA=~/Downloads/tpcdsData3/ build/sbt "sql/testOnly *TPCDSQueryPerformanceSuite"

SPARK_TPCDS_DATA=hdfs:///benchmark/tpcdsTable SPARK_MASTER=yarn build/sbt "sql/testOnly *TPCDSQueryPerformanceSuite"

build/sbt "sql/test:runMain org.apache.spark.sql.TPCDSRun hdfs:///benchmark/tpcdsTable yarn true"

spark-submit run-example --master yarn --executor-memory 4G --num-executors 10 org.apache.spark.examples.sql.TPCDSRun hdfs:///benchmark/tpcdsTable false
