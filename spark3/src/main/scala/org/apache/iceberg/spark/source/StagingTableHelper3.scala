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
package org.apache.iceberg.spark.source;

import java.util.UUID

import org.apache.iceberg.{BaseCombinedScanTask, BaseFileScanTask, DataFile, PartitionSpecParser, SchemaParser, Table}
import org.apache.iceberg.TableProperties.{DEFAULT_NAME_MAPPING, STAGING_TABLE_NAME_TAG, STAGING_TABLE_NAME_TAG_DEFAULT}
import org.apache.iceberg.TableProperties.WRITE_NEW_DATA_LOCATION
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.expressions.{Expressions, ResidualEvaluator}
import org.apache.iceberg.hadoop.{HadoopCatalog, HadoopFileIO}
import org.apache.iceberg.hive.HiveCatalogs
import org.apache.iceberg.relocated.com.google.common.base.Joiner
import org.apache.iceberg.relocated.com.google.common.collect.Maps
import org.apache.iceberg.spark.source.SparkBatchScan.{ReadTask, ReaderFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, Not}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.iceberg.AnalysisHelper3
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class StagingTableHelper3(table: Table,
                               sparkSession: SparkSession,
                               plan: LogicalPlan,
                               caseSensitive: Boolean
                             ) extends AnalysisHelper3 {

  def buildDataFrame(files: Seq[DataFile]): Dataset[Row] = {
    val tableSchemaString = SchemaParser.toJson(table.schema)
    val expectedSchemaString = SchemaParser.toJson(table.schema)
    val spec = table.spec()
    val residual = ResidualEvaluator.of(spec, Expressions.alwaysTrue(), caseSensitive)
    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val readTasks = files.map(new BaseFileScanTask(_, null, tableSchemaString, PartitionSpecParser.toJson(spec),
      residual))
      .map { scan =>
        val io = if (table.io().isInstanceOf[HadoopFileIO]) {
          val conf = new SerializableConfiguration(table.io.asInstanceOf[HadoopFileIO].conf)
          new HadoopFileIO(conf.value)
        } else {
          table.io()
        }
        val nameMappingString = table.properties().get(DEFAULT_NAME_MAPPING);
        new ReadTask(new BaseCombinedScanTask(scan), tableSchemaString, expectedSchemaString,
          nameMappingString, jsc.broadcast(io), jsc.broadcast(table.encryption()),
          caseSensitive, true)
      }

    val rdd = new DataSourceRDD(sparkSession.sparkContext, readTasks, new ReaderFactory(0), false)
    val logicalPlan = LogicalRDD(plan.output, rdd, isStreaming = false)(sparkSession)
    buildDataFrame(sparkSession, logicalPlan)
  }

  private def buildUpdatedDataFrame(targetColumnNames: Seq[String],
                                    updateExprs: Seq[Expression],
                                    condition: Expression,
                                    files: Seq[DataFile]): Dataset[Row] = {
    val namedExpressions = plan.output
    val targetColumnNameParts = targetColumnNames.map(UnresolvedAttribute.quotedString)
      .map{ col => UnresolvedAttribute(col.name).nameParts }
    val updatedExpressions = generateUpdateExpressions(namedExpressions,
      targetColumnNameParts, updateExprs, sparkSession.sessionState.analyzer.resolver)
    val updatedColumns = buildUpdatedColumns(plan, updatedExpressions, condition)

    buildDataFrame(files).select(updatedColumns:_*)
  }


  def generateUpdatedFiles(oldFiles: Seq[DataFile],
                           condition: Expression): Iterable[DataFile] = {
    val dataFrame = buildDataFrame(oldFiles).filter(new Column(Not(condition)))
    writeTableAndGetFiles(dataFrame)
  }

  def generateUpdatedFiles(targetColumnNames: Seq[String],
                           updateExprs: Seq[Expression],
                           condition: Expression,
                           oldFiles: Seq[DataFile]): Iterable[DataFile] = {
    val dataFrame = buildUpdatedDataFrame(targetColumnNames, updateExprs, condition, oldFiles)
    writeTableAndGetFiles(dataFrame)
  }

  def buildCatalog(table: Table): Catalog = {
    val conf = sparkSession.sessionState.newHadoopConf()
    val tableProperties = table.properties()
    tableProperties.keySet().asScala
      .filter(key => key.startsWith("hadoop."))
      .foreach{ key => conf.set(key.replaceFirst("hadoop.", ""), tableProperties.get(key))}

    val stagingTag = table.properties.getOrDefault(STAGING_TABLE_NAME_TAG,
      STAGING_TABLE_NAME_TAG_DEFAULT)

    if (table.toString.contains("/")) {
      new HadoopCatalog(conf, Joiner.on("/").join(table.location(), stagingTag))
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

  def writeTableAndGetFiles(dataFrame: Dataset[Row]): Iterable[DataFile] = {
    // we need to use catalog service to delete metadata
    val icebergCatalog = buildCatalog(table)

    val baseTableIdentifier = buildTableIdentifier(icebergCatalog, table)

    val stagingTag = table.properties.getOrDefault(STAGING_TABLE_NAME_TAG,
      STAGING_TABLE_NAME_TAG_DEFAULT)

    val stagingUniquer = Joiner.on("_").join(System.currentTimeMillis().toString,
      UUID.randomUUID().toString.replaceAll("-", "_"))

    // Create staging table
    val (stagingTable, stagingTableIdentifier, tablePath) = createStagingTable(icebergCatalog,
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

  private def createStagingTable(catalog: Catalog,
                                 baseTableIdentifier: TableIdentifier,
                                 stagingTag: String,
                                 stagingTableUniquer: String): (Table, TableIdentifier, String) = {
    val properties = Maps.newHashMap[String, String](table.properties())
    // Separate metadata and data locations of staging table,
    // so as to put its data files (added by "delete" operation) into the data location of the original table
    properties.put(WRITE_NEW_DATA_LOCATION, table.locationProvider().newDataLocation(""))
    if (table.toString.contains("/")) {
      properties.remove("location")
      val stagingTableIdentifier = TableIdentifier.of(stagingTableUniquer)
      val stagingTable = catalog.createTable(stagingTableIdentifier, table.schema(), table.spec(),
        properties)
      val tablePath = stagingTable.location()
      (stagingTable, stagingTableIdentifier, tablePath)
    } else {
      // Manipulate "location" for HiveCatalog only.
      // HadoopCatalog is not allowed to do that, but namespace could be used instead.
      val location = table.location()
      val stagingLocation = location + "/" + stagingTag + "/" + stagingTableUniquer
      properties.put("location", stagingLocation)
      val stagingTableName = Joiner.on("_").join(baseTableIdentifier.name(), stagingTag, stagingTableUniquer)
      val stagingTableIdentifier = TableIdentifier.of(baseTableIdentifier.namespace(), stagingTableName)
      val stagingTable = catalog.createTable(stagingTableIdentifier, table.schema(), table.spec(),
        stagingLocation, properties)
      val tablePath =  Joiner.on(".").join(baseTableIdentifier.namespace().toString, stagingTableName)
      (stagingTable, stagingTableIdentifier, tablePath)
    }
  }

  override def conf: SQLConf = sparkSession.sessionState.conf

}
