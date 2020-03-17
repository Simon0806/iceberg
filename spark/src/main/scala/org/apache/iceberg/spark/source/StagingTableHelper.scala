/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.source

import com.google.common.base.Joiner
import com.google.common.collect.Maps
import java.util.UUID
import org.apache.iceberg.{BaseCombinedScanTask, BaseFileScanTask, DataFile, PartitionSpecParser, SchemaParser, Table}
import org.apache.iceberg.TableProperties.{STAGING_TABLE_NAME_TAG, STAGING_TABLE_NAME_TAG_DEFAULT}
import org.apache.iceberg.TableProperties.WRITE_NEW_DATA_LOCATION
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.expressions.{Expressions, ResidualEvaluator}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalogs
import org.apache.iceberg.spark.source.Reader.ReadTask
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, Not}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import scala.collection.JavaConverters._

object StagingTableHelper {

  protected def buildDataFrame(table:SparkTable, files: Seq[DataFile]): DataFrame = {
    val tableSchemaString = SchemaParser.toJson(table.schema)
    val expectedSchemaString = SchemaParser.toJson(table.schema)
    val spec = table.getIcebergTable.spec()
    val residual = ResidualEvaluator.of(spec, Expressions.alwaysTrue(), table.caseSensitive)
    val readTasks = files.map(new BaseFileScanTask(_, tableSchemaString, PartitionSpecParser.toJson(spec), residual))
      .map { scan =>
        new ReadTask(new BaseCombinedScanTask(scan), tableSchemaString, expectedSchemaString,
          table.jsc.broadcast(table.getIcebergTable.io()), table.jsc.broadcast(table.getIcebergTable.encryption()),
          table.caseSensitive, true)
      }

    val rdd = new DataSourceRDD(table.sparkSession.sparkContext, readTasks)
    val logicalPlan = LogicalRDD(table.getSqlUtil.getPlan.output, rdd, isStreaming = false)(table.sparkSession)
    table.getSqlUtil.buildDataFrame(table.sparkSession, logicalPlan)
  }

  private def buildUpdatedDataFrame(table: SparkTable, targetColumnNames: Seq[String], updateExprs: Seq[Expression],
                            condition: Expression, files: Seq[DataFile]): Dataset[Row] = {
    val namedExpressions = table.getSqlUtil.getPlan.output
    val targetColumnNameParts = targetColumnNames.map(UnresolvedAttribute.quotedString)
      .map{ col => UnresolvedAttribute(col.name).nameParts }
    val updatedExpressions = table.getSqlUtil.generateUpdateExpressions(namedExpressions,
      targetColumnNameParts, updateExprs, table.getSqlUtil.getResolver)
    val updatedColumns = table.getSqlUtil.buildUpdatedColumns(updatedExpressions, condition)

    buildDataFrame(table, files).select(updatedColumns:_*)
  }

  def generateUpdatedFiles(table: SparkTable, oldFiles: Seq[DataFile], condition: Expression): Iterable[DataFile] = {
    val dataFrame = buildDataFrame(table, oldFiles).filter(new Column(Not(condition)))
    writeTableAndGetFiles(table, dataFrame)
  }

  def generateUpdatedFiles(table: SparkTable, targetColumnNames: Seq[String], updateExprs: Seq[Expression],
                           condition: Expression, oldFiles: Seq[DataFile]): Iterable[DataFile] = {
    val dataFrame = buildUpdatedDataFrame(table, targetColumnNames, updateExprs, condition, oldFiles)
    writeTableAndGetFiles(table, dataFrame)
  }

  def buildCatalog(table: SparkTable): Catalog = {
    val conf = table.sparkSession().sessionState.newHadoopConf()
    val tableProperties = table.getIcebergTable.properties()
    tableProperties.keySet().asScala
      .filter(key => key.startsWith("hadoop."))
      .foreach{ key => conf.set(key.replaceFirst("hadoop.", ""), tableProperties.get(key))}

    if (table.name().contains("/")) {
      new HadoopCatalog(conf, table.getIcebergTable.location())
    } else {
      HiveCatalogs.loadCatalog(conf)
    }
  }

  private def buildTableIdentifier(catalog: Catalog, table: Table) : TableIdentifier =  {
    if (catalog.isInstanceOf[HadoopCatalog]) {
      TableIdentifier.of(table.location())
    } else {
      val splits = table.toString.split("\\.")
      val len = splits.length
      TableIdentifier.of(Namespace.of(splits(len - 2)), splits(len - 1))
    }
  }

  def writeTableAndGetFiles(table: SparkTable, dataFrame: DataFrame): Iterable[DataFile] = {
    // we need to use catalog service to delete metadata
    val icebergCatalog = buildCatalog(table)

    val baseTableIdentifier = buildTableIdentifier(icebergCatalog, table.getIcebergTable)

    val stagingTag = table.getIcebergTable.properties.getOrDefault(STAGING_TABLE_NAME_TAG,
      STAGING_TABLE_NAME_TAG_DEFAULT)

    val stagingUniquer = Joiner.on("_").join(System.currentTimeMillis().toString,
      UUID.randomUUID().toString.replaceAll("-", "_"))

    // Create staging table
    val (stagingTable, stagingTableIdentifier, tablePath) = createStagingTable(icebergCatalog, table,
      baseTableIdentifier, stagingTag, stagingUniquer)

    dataFrame.write.format("iceberg").mode("append").save(tablePath)

    stagingTable.refresh()
    val snapshot = stagingTable.currentSnapshot()
    val files = snapshot.addedFiles().asScala

    // Drop staging table
    icebergCatalog.dropTable(stagingTableIdentifier, false)

    // Return updated data files
    files
  }

  private def createStagingTable(catalog: Catalog, table: SparkTable, baseTableIdentifier: TableIdentifier,
                                 stagingTag: String, stagingTableUniquer: String): (Table, TableIdentifier, String) = {
    val conf = table.sparkSession().sessionState.newHadoopConf()
    val tableProperties = table.getIcebergTable.properties()
    tableProperties.keySet().asScala
      .filter(key => key.startsWith("hadoop."))
      .foreach{ key => conf.set(key.replaceFirst("hadoop.", ""), tableProperties.get(key))}

    val properties = Maps.newHashMap[String, String](tableProperties)

    if (table.getIcebergTable.toString.contains("/")) {
      properties.remove("location")
      // Separate metadata and data locations of staging table,
      // so as to put its data files (added by "delete" operation) into the data location of the original table
      properties.put(WRITE_NEW_DATA_LOCATION, table.getIcebergTable.locationProvider().newDataLocation(""))
      val stagingTableIdentifier = TableIdentifier.of(Namespace.of(stagingTag), stagingTableUniquer)
      val stagingTable = catalog.createTable(stagingTableIdentifier, table.schema(), table.getIcebergTable.spec(),
        properties)
      val tablePath = stagingTable.location()
      (stagingTable, stagingTableIdentifier, tablePath)
    } else {
      val location = table.getIcebergTable.location()
      properties.put("location", location + "/" + stagingTag + "/" + stagingTableUniquer)
      val stagingTableName = Joiner.on("_").join(baseTableIdentifier.name(), stagingTag, stagingTableUniquer)
      val stagingTableIdentifier = TableIdentifier.of(baseTableIdentifier.namespace(), stagingTableName)
      val stagingTable = catalog.createTable(stagingTableIdentifier, table.schema(), table.getIcebergTable.spec(),
        properties)
      val tablePath =  Joiner.on(".").join(baseTableIdentifier.namespace().toString, stagingTableName)
      (stagingTable, stagingTableIdentifier, tablePath)
    }
  }
}
