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

import java.util.Collections

import org.apache.iceberg.{DataFile, FindFiles, Table}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.{SparkFilters, SparkSchemaUtil}
import org.apache.iceberg.spark.source.StagingTableHelper3
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BasePredicate, Expression, Literal,
  NamedExpression, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions
import org.apache.spark.util.AccumulatorV2
import scala.collection.JavaConverters._

case class MergeIntoCommand3(target: Table,
                             sparkSession: SparkSession,
                             plan: LogicalPlan,
                             caseSensitive: Boolean,
                             source: DataFrame,
                             condition: Expression,
                             matchedClauses: Seq[IcebergMergeIntoMatchedClause],
                             notMatchedClause: Option[IcebergMergeIntoInsertClause]) extends RunnableCommand
  with PredicateHelper with AnalysisHelper3 {
  val ROW_ID_COL = "_row_id_"
  val FILE_NAME_COL = "_file_name_"
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"

  private def targetOutputCols: Seq[NamedExpression] = {
    plan.output
  }

  // scalastyle:off method.length
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val txn = target.newTransaction()

    val filesToRewrite = findFilesToRewrite(source, condition).toList
    val overwrite = txn.newOverwrite()
    filesToRewrite.map(overwrite.deleteFile)

    val joinDF = {
      val sourceDF = source.withColumn(SOURCE_ROW_PRESENT_COL, functions.lit(true))
      val targetDF = StagingTableHelper3(target, sparkSession, plan, caseSensitive)
        .buildDataFrame(filesToRewrite)
        .withColumn(TARGET_ROW_PRESENT_COL, functions.lit(true))

      sourceDF.join(targetDF, new Column(condition), "fullOuter")
    }

    val joinedPlan = joinDF.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      exprs.map { expr => tryResolveReferences(sparkSession)(expr, joinedPlan) }
    }

    def clauseCondition(clause: Option[IcebergMergeIntoClause]): Option[Expression] = {
      val condExprOption = clause.map(_.condition.getOrElse(Literal(true)))
      resolveOnJoinedPlan(condExprOption.toSeq).headOption
    }

    def matchedClauseOutput(clause: IcebergMergeIntoMatchedClause): Seq[Expression] = {
      val exprs = clause match {
        case u: IcebergMergeIntoUpdateClause =>
          // Generate update expressions and set ROW_DELETED_COL = false
          u.resolvedActions.map(_.expr) :+ Literal(false)
        case _: IcebergMergeIntoDeleteClause =>
          // Generate expressions to set the ROW_DELETED_COL = true
          targetOutputCols :+ Literal(true)
      }
      resolveOnJoinedPlan(exprs)
    }

    def notMatchedClauseOutput(clause: IcebergMergeIntoInsertClause): Seq[Expression] = {
      val exprs = clause.resolvedActions.map(_.expr) :+ Literal(false)
      resolveOnJoinedPlan(exprs)
    }

    val matchedClause1 = matchedClauses.headOption
    val matchedClause2 = matchedClauses.drop(1).headOption
    val joinedRowEncoder = RowEncoder(joinedPlan.schema)
    val outputRowEncoder = RowEncoder(SparkSchemaUtil.convert(target.schema())).resolveAndBind()

    val processor = new JoinedRowProcessor3(
      targetRowHasNoMatch = resolveOnJoinedPlan(Seq(functions.col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head,
      sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(functions.col(TARGET_ROW_PRESENT_COL).isNull.expr)).head,
      matchedCondition1 = clauseCondition(matchedClause1),
      matchedOutput1 = matchedClause1.map(matchedClauseOutput),
      matchedCondition2 = clauseCondition(matchedClause2),
      matchedOutput2 = matchedClause2.map(matchedClauseOutput),
      notMatchedCondition = clauseCondition(notMatchedClause),
      notMatchedOutput = notMatchedClause.map(notMatchedClauseOutput),
      noopCopyOutput = resolveOnJoinedPlan(targetOutputCols :+ Literal(false)),
      deleteRowOutput = resolveOnJoinedPlan(targetOutputCols :+ Literal(true) :+ Literal(true)),
      joinedAttributes = joinedPlan.output,
      joinedRowEncoder = joinedRowEncoder,
      outputRowEncoder = outputRowEncoder)

    val outputDF =
      Dataset.ofRows(sparkSession, joinedPlan).mapPartitions(processor.processPartition)(outputRowEncoder)

    val files = StagingTableHelper3(target, sparkSession, plan, caseSensitive)
      .writeTableAndGetFiles(outputDF)
      .toList

    files.map(overwrite.addFile)
    overwrite.commit()
    txn.commitTransaction()

    Seq.empty
  }

  def findFilesToRewrite(source: Dataset[Row], condition: Expression): Seq[DataFile] = {
    val targetOnlyCondition = projectPredication(plan, condition)
    val filter = if (targetOnlyCondition == null || targetOnlyCondition.isEmpty) {
      Expressions.alwaysTrue()
    } else {
      targetOnlyCondition.flatMap{ cond =>
        toFilter(resolve(sparkSession, plan, cond))
      }.map(SparkFilters.convert).reduceLeft(Expressions.and)
    }

    val partialFilteredFiles = FindFiles.in(target)
        .withRecordsMatching(filter).collect().asScala.toSeq

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    sparkSession.sparkContext.register(touchedFilesAccum, "MergeInto.touchedFiles")

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName = functions.udf { (fileName: String) => {
      touchedFilesAccum.add(fileName)
      1
    }}.asNondeterministic()

    // Join source and target to find the target files to rewrite
    val joinDF = {
      val targetDF = StagingTableHelper3(target, sparkSession, plan, caseSensitive).buildDataFrame(partialFilteredFiles)
        .withColumn(ROW_ID_COL, functions.monotonically_increasing_id())
        .withColumn(FILE_NAME_COL, functions.input_file_name())
      source.join(targetDF, new Column(condition), "inner")
    }

    val touchedFilesDF = joinDF.select(functions.col(ROW_ID_COL),
      recordTouchedFileName(functions.col(FILE_NAME_COL)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = touchedFilesDF.groupBy(ROW_ID_COL).agg(functions.sum("one").as("count"))
    if (matchedRowCounts.filter("count > 1").count() != 0) {
      throw new RuntimeException("multiple source rows matched to update target row.")
    }

    val touchedFileNames = touchedFilesAccum.value
    partialFilteredFiles.filter{file => touchedFileNames.contains(file.path().toString)}
  }

}
// scalastyle:on method.length

/**
 * Accumulator to collect distinct elements as a set.
 */
class SetAccumulator[T] extends AccumulatorV2[T, java.util.Set[T]] {
  private val _set = Collections.synchronizedSet(new java.util.HashSet[T]())

  override def isZero: Boolean = _set.isEmpty

  override def reset(): Unit = _set.clear()

  override def add(v: T): Unit = _set.add(v)

  override def merge(other: AccumulatorV2[T, java.util.Set[T]]): Unit = _set.addAll(other.value)

  override def value: java.util.Set[T] = _set

  override def copy(): AccumulatorV2[T, java.util.Set[T]] = {
    val newAcc = new SetAccumulator[T]()
    newAcc._set.addAll(_set)
    newAcc
  }
}

class JoinedRowProcessor3(targetRowHasNoMatch: Expression,
                          sourceRowHasNoMatch: Expression,
                          matchedCondition1: Option[Expression],
                          matchedOutput1: Option[Seq[Expression]],
                          matchedCondition2: Option[Expression],
                          matchedOutput2: Option[Seq[Expression]],
                          notMatchedCondition: Option[Expression],
                          notMatchedOutput: Option[Seq[Expression]],
                          noopCopyOutput: Seq[Expression],
                          deleteRowOutput: Seq[Expression],
                          joinedAttributes: Seq[Attribute],
                          joinedRowEncoder: ExpressionEncoder[Row],
                          outputRowEncoder: ExpressionEncoder[Row]) extends Serializable {

  private def generateProjection(exprs: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.create(exprs, joinedAttributes)
  }

  private def generatePredicate(expr: Expression): BasePredicate = {
    GeneratePredicate.generate(expr, joinedAttributes)
  }

  def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {

    val targetRowHasNoMatchPred = generatePredicate(targetRowHasNoMatch)
    val sourceRowHasNoMatchPred = generatePredicate(sourceRowHasNoMatch)
    val matchedPred1 = matchedCondition1.map(generatePredicate)
    val matchedOutputProj1 = matchedOutput1.map(generateProjection)
    val matchedPred2 = matchedCondition2.map(generatePredicate)
    val matchedOutputProj2 = matchedOutput2.map(generateProjection)
    val notMatchedPred = notMatchedCondition.map(generatePredicate)
    val notMatchedProj = notMatchedOutput.map(generateProjection)
    val noopCopyProj = generateProjection(noopCopyOutput)
    val deleteRowProj = generateProjection(deleteRowOutput)
    val outputProj = UnsafeProjection.create(outputRowEncoder.schema)

    def shouldDeleteRow(row: InternalRow): Boolean =
      row.getBoolean(outputRowEncoder.schema.fields.length)

    def processRow(inputRow: InternalRow): InternalRow = {
      if (targetRowHasNoMatchPred.eval(inputRow)) {
        // Target row did not match any source row, so just copy it to the output
        noopCopyProj.apply(inputRow)
      } else if (sourceRowHasNoMatchPred.eval(inputRow)) {
        // Source row did not match with any target row, so insert the new source row
        if (notMatchedPred.isDefined && notMatchedPred.get.eval(inputRow)) {
          notMatchedProj.get.apply(inputRow)
        } else {
          deleteRowProj.apply(inputRow)
        }
      } else {
        // Source row matched with target row, so update the target row
        if (matchedPred1.isDefined && matchedPred1.get.eval(inputRow)) {
          matchedOutputProj1.get.apply(inputRow)
        } else if (matchedPred2.isDefined && matchedPred2.get.eval(inputRow)) {
          matchedOutputProj2.get.apply(inputRow)
        } else {
          noopCopyProj(inputRow)
        }
      }
    }

    val toRow = joinedRowEncoder.createSerializer()
    val fromRow = outputRowEncoder.createDeserializer()

    rowIterator
      .map(toRow)
      .map(processRow)
      .filter(!shouldDeleteRow(_))
      .map { notDeletedInternalRow =>
        fromRow(outputProj(notDeletedInternalRow))
      }
  }
}
