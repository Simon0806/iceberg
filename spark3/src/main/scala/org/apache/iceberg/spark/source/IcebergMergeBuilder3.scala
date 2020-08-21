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

import org.apache.iceberg.Table
import org.apache.spark.annotation.Evolving
import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.iceberg.{AnalysisHelper3, IcebergMergeInto3, IcebergMergeIntoClause,
  IcebergMergeIntoDeleteClause, IcebergMergeIntoInsertClause, IcebergMergeIntoUpdateClause, PreprocessTableMerge3}
import org.apache.spark.sql.internal.SQLConf
import scala.collection.JavaConverters._
import scala.collection.Map

class IcebergMergeBuilder3 private(val targetTable: Table,
                                   val sparkSession: SparkSession,
                                   val plan: LogicalPlan,
                                   val caseSensitive: Boolean,
                                   val source: Dataset[Row],
                                   var onCondition: Expression,
                                   var whenClauses: Seq[IcebergMergeIntoClause]) extends AnalysisHelper3 {

  def onCondition(condition: String) :IcebergMergeBuilder3 = {
    this.onCondition = functions.expr(condition).expr
    this
  }

  def onCondition(condition: Expression) :IcebergMergeBuilder3 = {
    this.onCondition = condition
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
  def whenMatched(condition: Expression): IcebergMergeMatchedActionBuilder = {
    IcebergMergeMatchedActionBuilder(this, Some(new Column(condition)))
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
   * :: Evolving ::
   *
   * Build the actions to perform when the merge condition was not matched and
   * the given `condition` is true. This returns [[IcebergMergeMatchedActionBuilder]] object
   * which can be used to specify how to insert the new sourced row into the target table.
   *
   * @param condition boolean expression
   */
  @Evolving
  def whenNotMatched(condition: Expression): IcebergMergeNotMatchedActionBuilder = {
    IcebergMergeNotMatchedActionBuilder(this, Some(new Column(condition)))
  }

  /**
   * Execute the merge operation based on the built matched and not matched actions.
   */
  @Evolving
  def execute(): Unit = {
    val resolvedMergeInto = IcebergMergeInto3.resolveReferences(mergePlan)(
        tryResolveReferences(sparkSession))
    if (!resolvedMergeInto.resolved) {
      throw new RuntimeException("Failed to resolve merge expr\n")
    }
    // Preprocess the actions and verify
    val mergeIntoCommand = PreprocessTableMerge3(sparkSession.sessionState.conf)(resolvedMergeInto)
    sparkSession.sessionState.analyzer.checkAnalysis(mergeIntoCommand)
    mergeIntoCommand.run(sparkSession)
  }

  def withClause(clause: IcebergMergeIntoClause): IcebergMergeBuilder3 = {
    new IcebergMergeBuilder3(targetTable, sparkSession, plan, caseSensitive, source, onCondition, whenClauses :+ clause)
  }

  private def mergePlan: IcebergMergeInto3 = {
    IcebergMergeInto3(targetTable, sparkSession, plan, caseSensitive, source, onCondition, whenClauses)
  }

  override def conf: SQLConf = sparkSession.sessionState.conf
}

object IcebergMergeBuilder3 {
  def apply(targetTable: Table, sparkSession: SparkSession, plan: LogicalPlan, caseSensitive: Boolean,
            source: Dataset[Row]): IcebergMergeBuilder3 = {
    new IcebergMergeBuilder3(targetTable, sparkSession, plan, caseSensitive, source, null, Nil)
  }
}

/**
 * Builder class to specify the actions to perform when a target table row has matched a
 * source row based on the given merge condition and optional match condition.
 *
 * See [[IcebergMergeBuilder3]] for more information.
 */
@Evolving
class IcebergMergeMatchedActionBuilder private(private val mergeBuilder: IcebergMergeBuilder3,
                                               private val matchCondition: Option[Column]) {

  /**
   * Update the matched table rows based on the rules defined by `set`.
   *
   * @param map rules to update a row as a Scala map between target column names and
   *            corresponding update expressions as SQL formatted strings.
   */
  @Evolving
  def updateExpr(map: Map[String, String]): IcebergMergeBuilder3 = {
    addUpdateClause(toStrColumnMap(map))
  }

  /**
   * Update a matched table row based on the rules defined by `map`.
   *
   * @param map rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   */
  @Evolving
  def update(map: Map[String, Expression]): IcebergMergeBuilder3 = {
    addUpdateClause(map.map { case (k, v) => k -> new Column(v) })
  }

  /**
   * :: Evolving ::
   *
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param map rules to update a row as a Java map between target column names and
   *            corresponding expressions as SQL formatted strings.
   */
  @Evolving
  def updateExpr(map: java.util.Map[String, String]): IcebergMergeBuilder3 = {
    addUpdateClause(toStrColumnMap(map.asScala))
  }

  /**
   * Update a matched table row based on the rules defined by `set`.
   *
   * @param map rules to update a row as a Java map between target column names and
   *            corresponding expressions as Column objects.
   */
  @Evolving
  def update(map: java.util.Map[String, Expression]): IcebergMergeBuilder3 = {
    addUpdateClause(map.asScala.map { case (k, v) => k -> new Column(v) })
  }

  /**
   * Update all the columns of the matched table row with the values of the
   * corresponding columns in the source row.
   */
  @Evolving
  def updateAll(): IcebergMergeBuilder3 = {
    val updateClause = IcebergMergeIntoUpdateClause(
      matchCondition.map(_.expr),
      IcebergMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(updateClause)
  }

  /** Delete a matched row from the table */
  def delete(): IcebergMergeBuilder3 = {
    val deleteClause = IcebergMergeIntoDeleteClause(matchCondition.map(_.expr))
    mergeBuilder.withClause(deleteClause)
  }

  private def addUpdateClause(set: Map[String, Column]): IcebergMergeBuilder3 = {
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
  def apply(mergeBuilder: IcebergMergeBuilder3, matchCondition: Option[Column]): IcebergMergeMatchedActionBuilder = {
    new IcebergMergeMatchedActionBuilder(mergeBuilder, matchCondition)
  }
}

/**
 * Builder class to specify the actions to perform when a source row has not matched any target
 * table row based on the merge condition, but has matched the additional condition if specified.
 *
 * See [[IcebergMergeBuilder3]] for more information.
 */
@Evolving
class IcebergMergeNotMatchedActionBuilder private(private val mergeBuilder: IcebergMergeBuilder3,
                                                  private val notMatchCondition: Option[Column]) {

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as Column objects.
   */
  @Evolving
  def insert(values: Map[String, Expression]): IcebergMergeBuilder3 = {
    addInsertClause(values.map{ case (k, v) => k -> new Column(v) })
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Scala map between target column names and
   *               corresponding expressions as SQL formatted strings.
   */
  @Evolving
  def insertExpr(values: Map[String, String]): IcebergMergeBuilder3 = {
    addInsertClause(toStrColumnMap(values))
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as Column objects.
   */
  @Evolving
  def insert(values: java.util.Map[String, Expression]): IcebergMergeBuilder3 = {
    addInsertClause(values.asScala.map{ case (k, v) => k -> new Column(v) } )
  }

  /**
   * Insert a new row to the target table based on the rules defined by `values`.
   *
   * @param values rules to insert a row as a Java map between target column names and
   *               corresponding expressions as SQL formatted strings.
   */
  @Evolving
  def insertExpr(values: java.util.Map[String, String]): IcebergMergeBuilder3 = {
    addInsertClause(toStrColumnMap(values.asScala))
  }

  /**
   * Insert a new target table row by assigning the target columns to the values of the
   * corresponding columns in the source row.
   */
  @Evolving
  def insertAll(): IcebergMergeBuilder3 = {
    val insertClause = IcebergMergeIntoInsertClause(
      notMatchCondition.map(_.expr),
      IcebergMergeIntoClause.toActions(Nil, Nil))
    mergeBuilder.withClause(insertClause)
  }

  private def addInsertClause(setValues: Map[String, Column]): IcebergMergeBuilder3 = {
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
  private[iceberg] def apply(mergeBuilder: IcebergMergeBuilder3,
                             notMatchCondition: Option[Column]): IcebergMergeNotMatchedActionBuilder = {
    new IcebergMergeNotMatchedActionBuilder(mergeBuilder, notMatchCondition)
  }
}


