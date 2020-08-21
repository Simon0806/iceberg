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

import org.apache.iceberg.{FindFiles, Table}
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.SparkFilters
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.iceberg.AnalysisHelper3
import org.apache.spark.sql.internal.SQLConf
import scala.collection.JavaConverters._

class DelegatableDeletesSupport3(table: Table,
                                 plan: LogicalPlan,
                                 sparkSession: SparkSession,
                                 caseSensitive: Boolean
                               ) extends AnalysisHelper3 {
  def deleteWhere(condition: Expression): Unit = {
    val resolveCondition = resolve(sparkSession, plan, condition)
    val expression = if (condition.toString() == "true") {
      Expressions.alwaysTrue()
    } else {
      toFilter(resolveCondition).map(SparkFilters.convert).reduceLeft(Expressions.and)
    }

    try {
      // try to delete file at first
      table.newDelete().deleteFromRowFilter(expression).commit()
    } catch {
      case _: ValidationException =>
        val txn = table.newTransaction()
        //Step 1: delete files that match filters
        val filesToDelete = FindFiles.in(table).withRecordsMatching(expression)
          .collect().asScala.toList

        //Step 2: generate updated files
        val updatedFiles = StagingTableHelper3(table, sparkSession, plan, caseSensitive)
          .generateUpdatedFiles(filesToDelete, resolveCondition)

        //Step 3: overwrite
        if (updatedFiles.nonEmpty) {
          val overwrite = txn.newOverwrite()
          filesToDelete.map(overwrite.deleteFile)
          updatedFiles.map(overwrite.addFile)
          overwrite
            .validateNoConflictingAppends(table.currentSnapshot().snapshotId(), expression)
            .commit()
        } else {
          val deleting = txn.newDelete()
          filesToDelete.map(deleting.deleteFile)
          deleting.commit()
        }
        txn.commitTransaction()
    }
  }

  override def conf: SQLConf = sparkSession.sessionState.conf
}
