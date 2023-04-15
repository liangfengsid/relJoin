/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.tags.ExtendedSQLTest

/**
 * End-to-end tests to check TPCDS query results.
 *
 * To run this test suite:
 * {{{
 *   SPARK_TPCDS_DATA=<path of TPCDS SF=1 data> build/sbt "sql/testOnly *TPCDSQueryTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "~sql/testOnly *TPCDSQueryTestSuite -- -z q79"
 * }}}
 *
 * To re-generate golden files for this suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "sql/testOnly *TPCDSQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "sql/testOnly *TPCDSQueryTestSuite -- -z q79"
 * }}}
 */
@ExtendedSQLTest
class TPCDSQueryPerformanceSuite extends QueryTest with TPCDSBase with SQLQueryTestHelper {

  private val tpcdsDataPath = sys.env.get("SPARK_TPCDS_DATA")
  private val master = sys.env.get("SPARK_MASTER").getOrElse("local[1]")

  protected override def createSparkSession: TestSparkSession = {
    val testSession = new TestSparkSession(new SparkContext(master,
      this.getClass.getSimpleName, sparkConf))
    testSession.sparkContext.conf.setMaster(master)
    testSession.sqlContext.conf.setConfString(SQLConf.SHUFFLE_PARTITIONS.key, "20")
    testSession
  }

  // We use SF=1 table data here, so we cannot use SF=100 stats
  protected override val injectStats: Boolean = true

  if (tpcdsDataPath.nonEmpty && !tpcdsDataPath.get.startsWith("hdfs:")) {
    val nonExistentTables = tableNames.filterNot { tableName =>
      Files.exists(Paths.get(s"${tpcdsDataPath.get}/$tableName"))
    }
    if (nonExistentTables.nonEmpty) {
      fail(s"Non-existent TPCDS table paths found in ${tpcdsDataPath.get}: " +
        nonExistentTables.mkString(", "))
    }
  }

  protected val baseResourcePath = {
    // use the same way as `SQLQueryTestSuite` to get the resource path
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "tpcds-query-results")
      .toFile.getAbsolutePath
  }

  override def createTable(
      spark: SparkSession,
      tableName: String,
      format: String = "parquet",
      options: Seq[String] = Nil): Unit = {
    val sql = s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |LOCATION '${tpcdsDataPath.get}/$tableName'
         |${options.mkString("\n")}
       """.stripMargin
    print(sql + "\n")
    spark.sql(sql)
  }

  private def runQuery(
      query: String,
      conf: Map[String, String]): Unit = {
    withSQLConf(conf.toSeq: _*) {
      try {
        val startTime = System.currentTimeMillis()
        handleExceptions(getNormalizedResult(spark, query))
        val endTime = System.currentTimeMillis()
        val queryString = query.trim
        val time = endTime - startTime
        print(s"plan execution time: ${time} ms\n")
        assertResult(true, s"plan execution time:${time} ms\n$queryString") {
          true
        }

      } catch {
        case e: Throwable =>
          val configs = conf.map {
            case (k, v) => s"$k=$v"
          }
          throw new Exception(s"${e.getMessage}\nError using configs:\n${configs.mkString("\n")}")
      }
    }
  }

  val sortMergeJoinConf = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true")

  val broadcastHashJoinConf = Map(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760")

  val shuffledHashJoinConf = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    "spark.sql.join.forceApplyShuffledHashJoin" -> "true")

  val allJoinConfCombinations = Seq(
    sortMergeJoinConf, broadcastHashJoinConf, shuffledHashJoinConf)

  val joinConfs: Seq[Map[String, String]] = {
    sys.env.get("SPARK_TPCDS_JOIN_CONF").map { s =>
      val p = new java.util.Properties()
      p.load(new java.io.StringReader(s))
      Seq(p.asScala.toMap)
    }.getOrElse(allJoinConfCombinations)
  }

  assert(joinConfs.nonEmpty)
  joinConfs.foreach(conf => require(
    allJoinConfCombinations.contains(conf),
    s"Join configurations [$conf] should be one of $allJoinConfCombinations"))

  if (tpcdsDataPath.nonEmpty) {
    tpcdsQueries.foreach { name =>
      val queryString = resourceToString(s"tpcds/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      joinConfs.foreach { conf =>
        System.gc()  // Workaround for GitHub Actions memory limitation, see also SPARK-37368
        val confName = if (conf == sortMergeJoinConf) s"sortMergeJoinConf"
          else if (conf == broadcastHashJoinConf) s"broadcastHashJoinConf"
          else if (conf == shuffledHashJoinConf) s"shuffledHashJoinConf"
          else s"unknownJoinConf"
        test(s"$name $confName") {
          runQuery(queryString, conf)
        }

      }
    }
  } else {
    ignore("skipped because env `SPARK_TPCDS_DATA` is not set") {}
  }
}
