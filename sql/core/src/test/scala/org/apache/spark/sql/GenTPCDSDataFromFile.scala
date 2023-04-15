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

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, rpad}
import org.apache.spark.sql.types.{CharType, StringType, StructField, StructType, VarcharType}

// The classes in this file are basically moved from https://github.com/databricks/spark-sql-perf
class TPCDSTablesFromFile(sqlContext: SQLContext, dsdgenDir: String, scaleFactor: Int)
  extends TPCDSSchema with Logging with Serializable {

  private def tables: Seq[Table] = tableColumns.map { case (tableName, schemaString) =>
    val partitionColumns = tablePartitionColumns.getOrElse(tableName, Nil)
      .map(_.stripPrefix("`").stripSuffix("`"))
    Table(tableName, partitionColumns, StructType.fromDDL(schemaString))
  }.toSeq

  private case class Table(name: String, partitionColumns: Seq[String], schema: StructType) {
    def nonPartitioned: Table = {
      Table(name, Nil, schema)
    }

    private def df(numPartition: Int) = {
      val generatedData = sqlContext.sparkContext.textFile(s"$dsdgenDir/$name.dat", numPartition)
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          val values = l.split("\\|", -1).map { v =>
            if (v.equals("")) {
              // If the string value is an empty string, we turn it to a null
              null
            } else {
              v
            }
          }
          Row.fromSeq(values)
        }
      }

      val stringData =
        sqlContext.createDataFrame(
          rows,
          StructType(schema.fields.map(f => StructField(f.name, StringType))))

      val convertedData = {
        val columns = schema.fields.map { f =>
          val c = f.dataType match {
            // Needs right-padding for char types
            case CharType(n) => rpad(Column(f.name), n, " ")
            // Don't need a cast for varchar types
            case _: VarcharType => col(f.name)
            case _ => col(f.name).cast(f.dataType)
          }
          c.as(f.name)
        }
        stringData.select(columns: _*)
      }

      convertedData
    }

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        clusterByPartitionColumns: Boolean,
        filterOutNullPartitionValues: Boolean,
        numPartitions: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(numPartitions)
      val tempTableName = s"${name}_text"
      data.createOrReplaceTempView(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
               |SELECT
               |  $columnString
               |FROM
               |  $tempTableName
               |$predicates
               |DISTRIBUTE BY
               |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          logInfo(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // treat non-partitioned tables as "one partition" that we want to coalesce
        if (clusterByPartitionColumns) {
          // in case data has more than maxRecordsPerFile, split into multiple writers to improve
          // datagen speed files will be truncated to maxRecordsPerFile value, so the final
          // result will be the same.
          val numRows = data.count
          val maxRecordPerFile = Try {
            sqlContext.getConf("spark.sql.files.maxRecordsPerFile").toInt
          }.getOrElse(0)

          if (maxRecordPerFile > 0 && numRows > maxRecordPerFile) {
            val numFiles = (numRows.toDouble/maxRecordPerFile).ceil.toInt
            logInfo(s"Coalescing into $numFiles files")
            data.coalesce(numFiles).write
          } else {
            data.coalesce(1).write
          }
        } else {
          data.write
        }
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns: _*)
      }
      logInfo(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }
  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: String = "",
      numPartitions: Int = 100): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    tablesToBeGenerated.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues, numPartitions)
    }
  }
}

/**
 * This class generates TPCDS table data by using tpcds-kit:
 *  - https://github.com/databricks/tpcds-kit
 *
 * To run this:
 * {{{
 *   build/sbt "sql/test:runMain <this class> --dsdgenDir <path> --location <path> --scaleFactor 1"
 * }}}
 */
object GenTPCDSDataFromFile {

  def main(args: Array[String]): Unit = {
    val config = new GenTPCDSDataConfig(args)

    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master(config.master)
      .getOrCreate()

    val tables = new TPCDSTablesFromFile(
      spark.sqlContext,
      dsdgenDir = config.dsdgenDir,
      scaleFactor = config.scaleFactor)

    tables.genData(
      location = config.location,
      format = config.format,
      overwrite = config.overwrite,
      partitionTables = config.partitionTables,
      clusterByPartitionColumns = config.clusterByPartitionColumns,
      filterOutNullPartitionValues = config.filterOutNullPartitionValues,
      tableFilter = config.tableFilter,
      numPartitions = config.numPartitions)

    spark.stop()
  }
}
