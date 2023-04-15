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

package org.apache.spark.examples.sql

import java.lang.{Boolean => JBoolean}
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf

/**
 * This class generates TPCDS table data by using tpcds-kit:
 *  - https://github.com/databricks/tpcds-kit
 *
 * To run this:
 * {{{
 *   build/sbt "sql/test:runMain <this class> --dsdgenDir <path> --location <path> --scaleFactor 1"
 * }}}
 */
object TPCDSRun extends TPCDSRunBase with SQLQueryTestHelper {
  var tpcdsDataPath = ""
  var injectStats = false

  def createTable(
        spark: SparkSession,
        tableName: String,
        format: String = "parquet",
        options: Seq[String] = Nil): Unit = {
    val sql = s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |LOCATION '${tpcdsDataPath}/$tableName'
         |${options.mkString("\n")}
       """.stripMargin
    print(sql + "\n")
    spark.sql(sql)
  }

  def createTables(spark: SparkSession): Unit = {
    tableNames.foreach { tableName =>
      createTable(spark, tableName)
      if (injectStats) {
        // To simulate plan generation on actual TPC-DS data, injects data stats here
        spark.sessionState.catalog.alterTableStats(
          TableIdentifier(tableName), Some(TPCDSTableStats.sf100TableStats(tableName)))
      }
    }
  }

  def dropTables(spark: SparkSession): Unit = {
    tableNames.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
  }

  def tryPrintConf(spark: SparkSession, key: String): Unit = {
    try {
      print(s"$key = ${spark.conf.get(key)}\n")
    } catch {
      case _: Throwable =>
        print(s"$key not found in the config}\n")
    }
  }

  private def runQuery(
      spark: SparkSession,
      queryName: String,
      confName: String,
      query: String,
      conf: Map[String, String]): Unit = {
    SparkSession.setActiveSession(spark)
    withSQLConf(conf.toSeq: _*) {
      try {
        val startTime = System.currentTimeMillis()
        handleExceptions(getNormalizedResult(spark, query))
        val endTime = System.currentTimeMillis()
        val queryString = query.trim
        val time = endTime - startTime
        print(s"Query ${queryName} with conf ${confName} plan execution time: ${time} ms\n")
        tryPrintConf(spark, SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)
        tryPrintConf(spark, SQLConf.PREFER_SORTMERGEJOIN.key)
        tryPrintConf(spark, "spark.sql.join.forceApplyShuffledHashJoin")
        tryPrintConf(spark, SQLConf.ADAPTIVE_EXECUTION_ENABLED.key)

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
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")

  val broadcastHashJoinConf = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")

  val shuffledHashJoinConf = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    "spark.sql.join.forceApplyShuffledHashJoin" -> "true",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")

  val aqeJoinConf = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
    SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "false",
    SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
    SQLConf.SKEW_JOIN_ENABLED.key -> "false",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true")

  val aqeCostBasedJoinConf = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
    SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "false",
    SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
    SQLConf.SKEW_JOIN_ENABLED.key -> "false",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
    SQLConf.ADAPTIVE_COST_JOIN_ENABLE.key -> "true")

  val aqeCostBased10JoinConf = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
    SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "false",
    SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
    SQLConf.SKEW_JOIN_ENABLED.key -> "false",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
    SQLConf.ADAPTIVE_COST_JOIN_ENABLE.key -> "true",
    SQLConf.ADAPTIVE_NETWORK_WEIGHT.key -> "10")

  val allJoinConfCombinations = Seq(
    aqeCostBasedJoinConf, aqeCostBasedJoinConf, aqeCostBasedJoinConf)

  def main(args: Array[String]): Unit = {
    val options = (0 to 1).map(i => if (i < args.length) Some(args(i)) else None)

    options.toArray match {
      case Array(dataPath, inject) =>
        tpcdsDataPath = dataPath.getOrElse("")
        injectStats = JBoolean.valueOf(inject.getOrElse("true"))
      case _ =>
        System.err.print("Usage: TPCDSRun dataPath injectStats\n")
        System.exit(1)
    }

    val sparkBuilder = SparkSession
      .builder
      .appName(getClass.getName)
      .config(SQLConf.MAX_TO_STRING_FIELDS.key, Int.MaxValue)
    if (injectStats) {
      sparkBuilder.config(SQLConf.CBO_ENABLED.key, true)
        .config(SQLConf.PLAN_STATS_ENABLED.key, true)
        .config(SQLConf.JOIN_REORDER_ENABLED.key, true)
    }
    val spark = sparkBuilder.getOrCreate();

    val joinConfs: Seq[Map[String, String]] = {
      sys.env.get("SPARK_TPCDS_JOIN_CONF").map { s =>
        val p = new java.util.Properties()
        p.load(new java.io.StringReader(s))
        Seq(p.asScala.toMap)
      }.getOrElse(allJoinConfCombinations)
    }

    joinConfs.foreach(conf => require(
      allJoinConfCombinations.contains(conf),
      s"Join configurations [$conf] should be one of $allJoinConfCombinations"))

    if (tpcdsDataPath.nonEmpty && !tpcdsDataPath.startsWith("hdfs:")) {
      val nonExistentTables = tableNames.filterNot { tableName =>
        Files.exists(Paths.get(s"${tpcdsDataPath}/$tableName"))
      }
      if (nonExistentTables.nonEmpty) {
        throw new Exception(s"Non-existent TPCDS table paths found in ${tpcdsDataPath}: " +
          nonExistentTables.mkString(", "))
      }
    }

    createTables(spark)

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
          runQuery(spark, name, confName, queryString, conf)
        }
      }
    } else {
      print("skipped because env `SPARK_TPCDS_DATA` is not set.\n")
    }

    dropTables(spark)
    spark.stop()
  }
}
