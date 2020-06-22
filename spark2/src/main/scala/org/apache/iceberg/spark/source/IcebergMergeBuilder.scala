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

import org.apache.spark.annotation.InterfaceStability.{Evolving, Unstable}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions
import org.apache.spark.sql.iceberg.{AnalysisHelper, IcebergMergeInto, IcebergMergeIntoClause,
  IcebergMergeIntoDeleteClause, IcebergMergeIntoInsertClause, IcebergMergeIntoUpdateClause, PreprocessTableMerge}
import org.apache.spark.sql.internal.SQLConf
import scala.collection.JavaConverters._
import scala.collection.Map

class IcebergMergeBuilder private (val targetTable: SparkIcebergTable,
                                   val source: Dataset[Row],
                                   var onCondition: Expression,
                                   var whenClauses: Seq[IcebergMergeIntoClause]) extends AnalysisHelper {

  def onCondition(condition: String) :IcebergMergeBuilder = {
    this.onCondition = functions.expr(condition).expr
    this
  }

  /**
   * Build the actions to perform when the merge condition was matched.  This returns
   * [[IcebergMergeMatchedActionBuilder]] object which can be used to specify how
   * to update or delete the matched target table row with the source row.
   */
  @Evolving
  def whenMatched(): IcebergMergeMatchedActionBuilder = {
    IcebergMergeMatchedActionBuilder(this, None)
  }

  /**
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns [[IcebergMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a SQL formatted string
   */
  @Evolving
  def whenMatched(condition: String): IcebergMergeMatchedActionBuilder = {
    whenMatched(functions.expr(condition))
  }

  /**
   * :: Evolving ::
   *
   * Build the actions to perform when the merge condition was matched and
   * the given `condition` is true. This returns a [[IcebergMergeMatchedActionBuilder]] object
   * which can be used to specify how to update or delete the matched target table row with the
   * source row.
   *
   * @param condition boolean expression as a Column object
   */
  @Evolving
  def whenMatched(condition: Column): IcebergMergeMatchedActionBuilder = {
    IcebergMergeMatchedActionBuilder(this, Some(condition))
  }

  /**
   * :: Evolving ::
   *
   * Build the action to perform when the merge condition was not matched. This returns
   * [[IcebergMergeNotMatchedActionBuilder]] object which can be used to specify how
   * to insert the new sourced row into the target table.
   */
  @Evolving
  def whenNotMatched(): IcebergMergeNotMatchedActionBuilder = {
    IcebergMergeNotMatchedActionBuilder(this, None)
  }

  /**
   * :: Evolving ::
   *
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[IcebergMergeMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a SQL formatted string
   */
  @Evolving
  def whenNotMatched(condition: String): IcebergMergeNotMatchedActionBuilder = {
    whenNotMatched(functions.expr(condition))
  }

  /**
   * :: Evolving ::
   *
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[IcebergMergeMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression as a Column object
   */
  @Evolving
  def whenNotMatched(condition: Column): IcebergMergeNotMatchedActionBuilder = {
    IcebergMergeNotMatchedActionBuilder(this, Some(condition))
  }

  /**
   * Execute the merge operation based on the built matched and not matched actions.
   */
  @Evolving
  def execute(): Unit = {
    val sparkSession = targetTable.sparkSession()
    val resolvedMergeInto = IcebergMergeInto.resolveReferences(mergePlan)(
        tryResolveReferences(sparkSession))
    if (!resolvedMergeInto.resolved) {
      throw new RuntimeException("Failed to resolve merge expr\n")
    }
    // Preprocess the actions and verify
    val mergeIntoCommand = PreprocessTableMerge(sparkSession.sessionState.conf)(resolvedMergeInto)
    sparkSession.sessionState.analyzer.checkAnalysis(mergeIntoCommand)
    mergeIntoCommand.run(sparkSession)
  }

  def withClause(clause: IcebergMergeIntoClause): IcebergMergeBuilder = {
    new IcebergMergeBuilder(
      this.targetTable, this.source, this.onCondition, this.whenClauses :+ clause)
  }

  private def mergePlan: IcebergMergeInto = {
    IcebergMergeInto(targetTable, source, onCondition, whenClauses)
  }

  override def conf: SQLConf = targetTable.sparkSession().sessionState.conf
}

object IcebergMergeBuilder {
  def apply(targetTable: SparkIcebergTable, source: DataFrame): IcebergMergeBuilder = {
    new IcebergMergeBuilder(targetTable, source, null, Nil)
  }
}

/**
 * Builder class to specify the actions to perform when a target table row has matched a
 * source row based on the given merge condition and optional match condition.
 *
 * See [[IcebergMergeBuilder]] for more information.
 */
@Evolving
class IcebergMergeMatchedActionBuilder private(private val mergeBuilder: IcebergMergeBuilder,
                                               private val matchCondition: Option[Column]) {

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as Column objects.
   */
  @Evolving
  def update(set: Map[String, Column]): IcebergMergeBuilder = {
    addUpdateClause(set)
  }

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   */
  @Evolving
  def updateExpr(set: Map[String, String]): IcebergMergeBuilder = {
    addUpdateClause(toStrColumnMap(set))
  }

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   */
  @Evolving
  def update(set: java.util.Map[String, Column]): IcebergMergeBuilder = {
    addUpdateClause(set.asScala)
  }

  /**
   * :: Evolving ::
   *
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param set rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   */
  @Evolving
  def updateExpr(set: java.util.Map[String, String]): IcebergMergeBuilder = {
    addUpdateClause(toStrColumnMap(set.asScala))
  }

  /**
   * Update all the columns of the matched table row with the values of the
   * corresponding columns in the source row.
   */
  @Evolving
  def updateAll(): IcebergMergeBuilder = {
    val updateClause = IcebergMergeIntoUpdateClause(
      matchCondition.map(_.expr),
      IcebergMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(updateClause)
  }

  /** Delete a matched row from the table */
  def delete(): IcebergMergeBuilder = {
    val deleteClause = IcebergMergeIntoDeleteClause(matchCondition.map(_.expr))
    mergeBuilder.withClause(deleteClause)
  }

  private def addUpdateClause(set: Map[String, Column]): IcebergMergeBuilder = {
    if (set.isEmpty && matchCondition.isEmpty) {
      // Nothing to update = no need to add an update clause
      mergeBuilder
    } else {
      val setActions = set.toSeq
      val updateActions = IcebergMergeIntoClause.toActions(
        colNames = setActions.map(x => UnresolvedAttribute.quotedString(x._1)),
        exprs = setActions.map(x => x._2.expr),
        isEmptySeqEqualToStar = false)
      val updateClause = IcebergMergeIntoUpdateClause(matchCondition.map(_.expr), updateActions)
      mergeBuilder.withClause(updateClause)
    }
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}

object IcebergMergeMatchedActionBuilder {
  def apply(mergeBuilder: IcebergMergeBuilder, matchCondition: Option[Column]): IcebergMergeMatchedActionBuilder = {
    new IcebergMergeMatchedActionBuilder(mergeBuilder, matchCondition)
  }
}

/**
 * Builder class to specify the actions to perform when a source row has not matched any target
 * table row based on the merge condition, but has matched the additional condition if specified.
 *
 * See [[IcebergMergeBuilder]] for more information.
 */
@Evolving
class IcebergMergeNotMatchedActionBuilder private(private val mergeBuilder: IcebergMergeBuilder,
                                                  private val notMatchCondition: Option[Column]) {

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as Column objects.
   */
  @Evolving
  def insert(values: Map[String, Column]): IcebergMergeBuilder = {
    addInsertClause(values)
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as SQL formatted strings.
   */
  @Evolving
  def insertExpr(values: Map[String, String]): IcebergMergeBuilder = {
    addInsertClause(toStrColumnMap(values))
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as Column objects.
   */
  @Evolving
  def insert(values: java.util.Map[String, Column]): IcebergMergeBuilder = {
    addInsertClause(values.asScala)
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as SQL formatted strings.
   */
  @Evolving
  def insertExpr(values: java.util.Map[String, String]): IcebergMergeBuilder = {
    addInsertClause(toStrColumnMap(values.asScala))
  }

  /**
   * Insert a new target table row by assigning the target columns to the values of the
   * corresponding columns in the source row.
   */
  @Evolving
  def insertAll(): IcebergMergeBuilder = {
    val insertClause = IcebergMergeIntoInsertClause(
      notMatchCondition.map(_.expr),
      IcebergMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(insertClause)
  }

  private def addInsertClause(setValues: Map[String, Column]): IcebergMergeBuilder = {
    val values = setValues.toSeq
    val insertActions = IcebergMergeIntoClause.toActions(
      colNames = values.map(x => UnresolvedAttribute.quotedString(x._1)),
      exprs = values.map(x => x._2.expr),
      isEmptySeqEqualToStar = false)
    val insertClause = IcebergMergeIntoInsertClause(notMatchCondition.map(_.expr), insertActions)
    mergeBuilder.withClause(insertClause)
  }

  private def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }
}

object IcebergMergeNotMatchedActionBuilder {
  /**
   * Private method for internal usage only. Do not call this directly.
   */
  @Unstable
  private[iceberg] def apply(mergeBuilder: IcebergMergeBuilder,
                             notMatchCondition: Option[Column]): IcebergMergeNotMatchedActionBuilder = {
    new IcebergMergeNotMatchedActionBuilder(mergeBuilder, notMatchCondition)
  }
}


