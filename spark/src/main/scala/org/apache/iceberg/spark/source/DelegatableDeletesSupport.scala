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
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.SparkFilters
import org.apache.spark.sql.catalyst.expressions.Expression
import scala.collection.JavaConverters._

class DelegatableDeletesSupport(table: SparkIcebergTable) {

  def deleteWhere(condition: Expression): Unit = {
    val resolveCondition = table.getSqlUtil.resolve(table.sparkSession(), condition)
    val expression = if (condition.toString() == "true") {
      Expressions.alwaysTrue()
    } else {
      table.getSqlUtil.toFilter(resolveCondition).map(SparkFilters.convert).reduceLeft(Expressions.and)
    }

    try {
      // try to delete file at first
      table.getIcebergTable.newDelete().deleteFromRowFilter(expression).commit()
    } catch {
      case _: ValidationException => {
        val txn = table.getIcebergTable.newTransaction()
        //Step 1: delete files that match filters
        val filesToDelete = FindFiles.in(table.getIcebergTable).withRecordsMatching(expression)
          .collect().asScala.toList

        //Step 2: generate updated files
        val updatedFiles = StagingTableHelper.generateUpdatedFiles(table, filesToDelete, resolveCondition)

        // Step 3: overwrite
        if (updatedFiles.nonEmpty) {
          val overwrite = txn.newOverwrite()
          filesToDelete.map(overwrite.deleteFile)
          updatedFiles.map(overwrite.addFile)
          overwrite
            .validateNoConflictingAppends(table.getIcebergTable.currentSnapshot().snapshotId(), expression)
            .commit()
        } else {
          val deleting = txn.newDelete()
          filesToDelete.map(deleting.deleteFile)
          deleting.commit()
        }
        txn.commitTransaction()
      }
    }
  }
}
