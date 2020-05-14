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

package org.apache.spark.sql.iceberg

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.internal.SQLConf

case class PreprocessTableMerge(conf: SQLConf) extends UpdateExpressionsSupport {

  def preProcessMatched(mergeInto: IcebergMergeInto) : Seq[IcebergMergeIntoMatchedClause] = {
    val IcebergMergeInto(targetTable, _, _, matched, _) = mergeInto
    val target = targetTable.plan()

    val processedMatched = matched.map {
      case m: IcebergMergeIntoUpdateClause =>
        // Get the operations for columns that already exist...
        val existingUpdateOps = m.resolvedActions.map { a =>
          UpdateOperation(a.targetColNameParts, a.expr)
        }

        // Get expressions for the final schema for alignment. We must use the attribute in the
        // target plan where it exists - for new columns we just construct an attribute reference
        // to be filled in later once we evolve.
        val finalSchemaExprs = target.schema.map { field =>
          target.resolve(Seq(field.name), conf.resolver).getOrElse {
            AttributeReference(field.name, field.dataType)()
          }
        }

        // Use the helper methods for in UpdateExpressionsSupport to generate expressions such
        // that nested fields can be updated (only for existing columns).
        val alignedExprs = generateUpdateExpressions(finalSchemaExprs, existingUpdateOps, conf.resolver)
        val alignedActions: Seq[IcebergMergeAction] = alignedExprs
          .zip(finalSchemaExprs)
          .map { case (expr, attrib) => IcebergMergeAction(Seq(attrib.name), expr) }

        m.copy(m.condition, alignedActions)

      case m: IcebergMergeIntoDeleteClause => m    // Delete does not need reordering
    }
    processedMatched
  }

  // scalastyle:off method.length
  def preProcessNotMatched(mergeInto: IcebergMergeInto) : Option[IcebergMergeIntoInsertClause] = {
    val IcebergMergeInto(targetTable, _, _, matched, notMatched) = mergeInto
    val target = targetTable.plan()

    val processedNotMatched = notMatched.map { m =>
      // Check if columns are distinct. All actions should have targetColNameParts.size = 1.
      m.resolvedActions.foreach { a =>
        if (a.targetColNameParts.size > 1) {
          throw new UnsupportedOperationException("INSERT clause of MERGE operation contains nested field")
        }
      }

      val targetColNames = m.resolvedActions.map(_.targetColNameParts.head)
      if (targetColNames.distinct.size < targetColNames.size) {
        throw new AnalysisException(s"Duplicate column names in INSERT clause")
      }

      val newActionsFromTargetSchema = target.output.filterNot { col =>
        m.resolvedActions.exists { insertAct =>
          conf.resolver(insertAct.targetColNameParts.head, col.name)
        }
      }.map { col =>
        IcebergMergeAction(Seq(col.name), Literal(null, col.dataType))
      }

      val newActionsFromUpdate = matched.flatMap {
        _.resolvedActions.filterNot { updateAct =>
          m.resolvedActions.exists { insertAct =>
            conf.resolver(insertAct.targetColNameParts.head, updateAct.targetColNameParts.head)
          }
        }.toSeq
      }.map { updateAction =>
        IcebergMergeAction(updateAction.targetColNameParts, Literal(null, updateAction.dataType))
      }

      // Reorder actions by the target column order, with columns to be schema evolved that
      // aren't currently in the target at the end.
      val finalSchema = target.schema
      val alignedActions: Seq[IcebergMergeAction] = finalSchema.map { targetAttrib =>
        (m.resolvedActions ++ newActionsFromTargetSchema ++ newActionsFromUpdate).find { a =>
          conf.resolver(targetAttrib.name, a.targetColNameParts.head)
        }.map { a =>
          IcebergMergeAction(Seq(targetAttrib.name), castIfNeeded(a.expr, targetAttrib.dataType))
        }.getOrElse {
          // If a target table column was not found in the INSERT columns and expressions,
          // then throw exception as there must be an expression to set every target column.
          throw new AnalysisException(
            s"Unable to find the column '${targetAttrib.name}' of the target table from " +
              s"the INSERT columns: ${targetColNames.mkString(", ")}. " +
              s"INSERT clause must specify value for all the columns of the target table.")
        }
      }

      m.copy(m.condition, alignedActions)
    }
    processedNotMatched
  }
  // scalastyle:on method.length

  def apply(mergeInto: IcebergMergeInto): MergeIntoCommand = {
    val IcebergMergeInto(target, source, condition, _, _) = mergeInto
    MergeIntoCommand(target, source, condition, preProcessMatched(mergeInto), preProcessNotMatched(mergeInto))
  }

}
