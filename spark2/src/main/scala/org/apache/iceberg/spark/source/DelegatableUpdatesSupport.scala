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

import org.apache.iceberg.FindFiles
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.{SparkFilters, SparkSchemaUtil}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.iceberg.AnalysisHelper
import org.apache.spark.sql.internal.SQLConf
import scala.collection.JavaConverters._

class DelegatableUpdatesSupport(private val table: SparkIcebergTable) extends AnalysisHelper{

  def updateTable(assignments: java.util.Map[String, Expression], condition: Expression): Unit = {
    val assign = assignments.asScala.map{case(k, v) => (k, v)}.toSeq
    val columnNames = assign.map(_._1).toArray
    val values = assign.map(_._2).toArray

    val resolvedValues = values.map(resolveAndBind(table.sparkSession(),
      SparkSchemaUtil.convert(table.schema()), table.plan(), _))
    val resolvedCondition = resolve(table.sparkSession(), table.plan(), condition)

    val expression = if (condition.toString() == "true") {
      Expressions.alwaysTrue()
    } else {
      toFilter(resolvedCondition).map(SparkFilters.convert).reduceLeft(Expressions.and)
    }

    val txn = table.getIcebergTable.newTransaction()

    //Step 1: get files that match filters
    val oldFiles = FindFiles.in(table.getIcebergTable).withRecordsMatching(expression).collect().asScala.toList

    //Step 2: generate updated files
    val newFiles = StagingTableHelper(table).generateUpdatedFiles(columnNames, resolvedValues, resolvedCondition,
      oldFiles)

    //Step 3: overwrite
    val overwrite = txn.newOverwrite()
    oldFiles.map(overwrite.deleteFile)
    newFiles.map(overwrite.addFile)

    overwrite
      .validateNoConflictingAppends(table.getIcebergTable.currentSnapshot().snapshotId(), expression)
      .commit()

    txn.commitTransaction()
  }

  override def conf: SQLConf = table.sparkSession().sessionState.conf
}