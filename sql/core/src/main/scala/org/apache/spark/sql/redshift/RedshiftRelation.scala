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

package org.apache.spark.sql.redshift

import java.sql.{ResultSet, Connection}
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}
import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

private[sql] class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
    val driver = parameters.getOrElse("driver", null)
    val table = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))
    val partitionColumn = parameters.getOrElse("partitionColumn", null)
    val lowerBound = parameters.getOrElse("lowerBound", null)
    val upperBound = parameters.getOrElse("upperBound", null)
    val numPartitions = parameters.getOrElse("numPartitions", null)

    if (driver != null) DriverRegistry.register(driver)

    if (partitionColumn != null
      && (lowerBound == null || upperBound == null || numPartitions == null)) {
      sys.error("Partitioning incompletely specified")
    }

    val partitionInfo = if (partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfo(
        partitionColumn,
        lowerBound.toLong,
        upperBound.toLong,
        numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    JDBCRelation(url, table, parts, properties)(sqlContext)
  }
}

private[sql] case class JDBCRelation(
                                      url: String,
                                      table: String,
                                      parts: Array[Partition],
                                      properties: Properties = new Properties())(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan {

  override val needConversion: Boolean = false

  override val schema: StructType = JDBCRDD.resolveTable(url, table, properties)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val driver: String = DriverRegistry.getDriverClassName(url)

    def escapeSql(value: String): String =
      if (value == null) null else StringUtils.replace(value, "'", "''")

    def compileValue(value: Any): Any = value match {
      case stringValue: UTF8String => s"'${escapeSql(stringValue.toString)}'"
      case _ => value
    }

    def compileFilter(f: Filter): String = f match {
      case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
      case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
      case _ => null
    }

    val conn = JDBCRDD.getConnector(driver, url, properties)()
    val filterWhereClause: String = {
      val filterStrings = filters map compileFilter filter (_ != null)
      if (filterStrings.size > 0) {
        val sb = new StringBuilder("WHERE ")
        filterStrings.foreach(x => sb.append(x).append(" AND "))
        sb.substring(0, sb.length - 5)
      } else ""
    }
    val columnList: String = {
      val sb = new StringBuilder()
      requiredColumns.foreach(x => sb.append(",").append(x))
      if (sb.length == 0) "1" else sb.substring(1)
    }
    val sqlText = s"SELECT $columnList FROM $table $filterWhereClause"
    val stmt = conn.prepareStatement(sqlText,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val fetchSize = properties.getProperty("fetchSize", "0").toInt
    stmt.setFetchSize(fetchSize)
    val rs = stmt.executeQuery()
    var estSize: String = null
    while(rs.next()) {
      estSize = rs.getString(1)
    }
    val numRows = estSize.toInt

    if (numRows > 100) {
      ???
    } else {
      JDBCRDD.scanTable(
        sqlContext.sparkContext,
        schema,
        driver,
        url,
        properties,
        table,
        requiredColumns,
        filters,
        parts).map(_.asInstanceOf[Row])
    }
  }

}
