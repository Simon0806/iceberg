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

import java.util.Locale

import org.apache.iceberg.Table
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExtractValue,
  GetStructField, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Project}
import org.apache.spark.sql.types.DataType

/**
 * Represents an action in MERGE's UPDATE or INSERT clause where a target columns is assigned the
 * value of an expression
 *
 * @param targetColNameParts The name parts of the target column. This is a sequence to support
 *                           nested fields as targets.
 * @param expr Expression to generate the value of the target column.
 */
case class IcebergMergeAction(targetColNameParts: Seq[String], expr: Expression)
  extends UnaryExpression with Unevaluable {
  override def child: Expression = expr
  override def foldable: Boolean = false
  override def dataType: DataType = expr.dataType
  override def sql: String = s"${targetColNameParts.mkString("`", "`.`", "`")} = ${expr.sql}"
  override def toString: String = s"$prettyName ( $sql )"
}

/**
 * Trait that represents a WHEN clause in MERGE. See [[IcebergMergeInto3]]. It extends [[Expression]]
 * so that Catalyst can find all the expressions in the clause implementations.
 */
sealed trait IcebergMergeIntoClause extends Expression with Unevaluable {
  /** Optional condition of the clause */
  def condition: Option[Expression]

  /**
   * Sequence of actions represented as expressions. Note that this can be only be either
   * UnresolvedStar, or MergeAction.
   */
  def actions: Seq[Expression]

  /**
   * Sequence of resolved actions represented as Aliases. Actions, once resolved, must
   * be Aliases and not any other NamedExpressions. So it should be safe to do this casting
   * as long as this is called after the clause has been resolved.
   */
  def resolvedActions: Seq[IcebergMergeAction] = {
    assert(actions.forall(_.resolved), "all actions have not been resolved yet")
    actions.map(_.asInstanceOf[IcebergMergeAction])
  }

  def clauseType: String =
    getClass.getSimpleName.stripPrefix("IcebergMergeInto").stripSuffix("Clause")

  override def toString: String = {
    val condStr = condition.map { c => s"condition: ${c.sql}" }.getOrElse("")
    val actionStr = if (actions.isEmpty) "" else {
      "actions: " + actions.map(_.sql).mkString(", ")
    }
    s"$clauseType [${Seq(condStr, actionStr).mkString(", ")}]"
  }

  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = null
  override def children: Seq[Expression] = condition.toSeq ++ actions

  /** Verify whether the expressions in the actions are of the right type */
  def verifyActions(): Unit = actions.foreach {
    case _: UnresolvedStar =>
    case _: IcebergMergeAction =>
    case a: Any => throw new IllegalArgumentException(s"Unexpected action expression $a in $this")
  }
}

object IcebergMergeIntoClause {
  /**
   * Convert the parsed columns names and expressions into action for MergeInto. Note:
   * - Size of column names and expressions must be the same.
   * - If the sizes are zeros and `emptySeqIsStar` is true, this function assumes
   *   that query had `*` as an action, and therefore generates a single action
   *   with `UnresolvedStar`. This will be expanded later during analysis.
   * - Otherwise, this will convert the names and expressions to MergeActions.
   */
  def toActions(colNames: Seq[UnresolvedAttribute],
                exprs: Seq[Expression],
                isEmptySeqEqualToStar: Boolean = true): Seq[Expression] = {
    assert(colNames.size == exprs.size)
    if (colNames.isEmpty && isEmptySeqEqualToStar) {
      Seq[Expression](UnresolvedStar(None))
    } else {
      colNames.zip(exprs).map { case (col, expr) => IcebergMergeAction(col.nameParts, expr) }
    }
  }
}

/** Trait that represents WHEN MATCHED clause in MERGE. See [[IcebergMergeInto3]]. */
sealed trait IcebergMergeIntoMatchedClause extends IcebergMergeIntoClause

/** Represents the clause WHEN MATCHED THEN UPDATE in MERGE. See [[IcebergMergeInto3]]. */
case class IcebergMergeIntoUpdateClause(condition: Option[Expression], actions: Seq[Expression])
  extends IcebergMergeIntoMatchedClause {

  def this(cond: Option[Expression], cols: Seq[UnresolvedAttribute], exprs: Seq[Expression]) =
    this(cond, IcebergMergeIntoClause.toActions(cols, exprs))
}

/** Represents the clause WHEN MATCHED THEN DELETE in MERGE. See [[IcebergMergeInto3]]. */
case class IcebergMergeIntoDeleteClause(condition: Option[Expression])
  extends IcebergMergeIntoMatchedClause {
  def this(condition: Option[Expression], actions: Seq[IcebergMergeAction]) = this(condition)

  override def actions: Seq[Expression] = Seq.empty
}

/** Represents the clause WHEN NOT MATCHED THEN INSERT in MERGE. See [[IcebergMergeInto3]]. */
case class IcebergMergeIntoInsertClause(condition: Option[Expression], actions: Seq[Expression])
  extends IcebergMergeIntoClause {

  def this(cond: Option[Expression], cols: Seq[UnresolvedAttribute], exprs: Seq[Expression]) =
    this(cond, IcebergMergeIntoClause.toActions(cols, exprs))
}

case class IcebergMergeInto3(targetTable: Table,
                             sparkSession: SparkSession,
                             plan:LogicalPlan,
                             caseSensitive: Boolean,
                             sourceDF: DataFrame,
                             condition: Expression,
                             matchedClauses: Seq[IcebergMergeIntoMatchedClause],
                             notMatchedClause: Option[IcebergMergeIntoInsertClause]) extends Command {

  private lazy val source = sourceDF.queryExecution.analyzed
  private lazy val target = plan

  (matchedClauses ++ notMatchedClause).foreach(_.verifyActions())

  override def children: Seq[LogicalPlan] = Seq(target, source)
  override def output: Seq[Attribute] = Seq.empty
}

object IcebergMergeInto3 {
  def apply(targetTable: Table,
            sparkSession: SparkSession,
            plan:LogicalPlan,
            caseSensitive: Boolean,
            sourceDF: DataFrame,
            condition: Expression,
            whenClauses: Seq[IcebergMergeIntoClause]): IcebergMergeInto3 = {
    val deleteClauses = whenClauses.filter { clause => clause.isInstanceOf[IcebergMergeIntoDeleteClause] }
    val updateClauses = whenClauses.filter { clause => clause.isInstanceOf[IcebergMergeIntoUpdateClause] }
    val insertClauses = whenClauses.filter { clause => clause.isInstanceOf[IcebergMergeIntoInsertClause] }
    val matchedClauses = whenClauses.filter { clause => clause.isInstanceOf[IcebergMergeIntoMatchedClause] }

    if (whenClauses.isEmpty) {
      throw new AnalysisException("There must be at least one WHEN clause in a MERGE query")
    }

    if (matchedClauses.length == 2 && matchedClauses.head.condition.isEmpty) {
      throw new AnalysisException("When there are 2 MATCHED clauses in a MERGE query, " +
        "the first MATCHED clause must have a condition")
    }

    if (matchedClauses.length > 2) {
      throw new AnalysisException("There must be at most two match clauses in a MERGE query")
    }

    if (updateClauses.length >= 2 || deleteClauses.length >= 2 || insertClauses.length >= 2) {
      throw new AnalysisException("INSERT, UPDATE and DELETE cannot appear twice in " +
        "one MERGE query")
    }

    IcebergMergeInto3(targetTable, sparkSession, plan, caseSensitive, sourceDF, condition,
      whenClauses.collect { case x: IcebergMergeIntoMatchedClause => x }.take(2),
      whenClauses.collectFirst { case x: IcebergMergeIntoInsertClause => x })
  }

  def resolveReferences(merge: IcebergMergeInto3)
                       (resolveExpr: (Expression, LogicalPlan) => Expression): IcebergMergeInto3 = {
    val IcebergMergeInto3(targetTable, sparkSession, plan, caseSensitive, sourceDF, condition, matchedClauses,
    notMatchedClause) = merge

    val source = sourceDF.queryExecution.analyzed
    val target = plan

    val fakeSourcePlan = Project(source.output, source)
    val fakeTargetPlan = Project(target.output, target)

    // Resolve everything
    val resolvedCond = resolveExpression(resolveExpr)(condition, merge, "search condition")
    val resolvedMatchedClauses = matchedClauses.map {
      resolveClause(resolveExpr)(_, merge, fakeTargetPlan, fakeSourcePlan, merge, target)
    }

    val resolvedNotMatchedClause = notMatchedClause.map {
      resolveClause(resolveExpr)(_, fakeSourcePlan, fakeTargetPlan, fakeSourcePlan, merge, target)
    }

    IcebergMergeInto3(targetTable, sparkSession, plan, caseSensitive, sourceDF, resolvedCond, resolvedMatchedClauses,
      resolvedNotMatchedClause)
  }

  /**
   * Resolves expression with given plan or fail using given message. It makes a best-effort
   * attempt to throw specific error messages on which part of the query has a problem.
   */
  def resolveExpression(resolveExpr: (Expression, LogicalPlan) => Expression)
                       (expr: Expression, plan: LogicalPlan, mergeClauseType: String): Expression = {
    val resolvedExpr = resolveExpr(expr, plan)
    resolvedExpr.flatMap(_.references).filter(!_.resolved).foreach { a =>
      // Note: This will throw error only on unresolved attribute issues,
      // not other resolution errors like mismatched data types.
      val cols = "columns " + plan.children.flatMap(_.output).map(_.sql).mkString(", ")
      throw new AnalysisException(s"cannot resolve ${a.sql} in $mergeClauseType given $cols")
    }
    resolvedExpr
  }

  /**
   * Resolves a clause using the given plan (used for resolving the action exprs) and
   * returns the resolved clause.
   */
  def resolveClause[T <: IcebergMergeIntoClause](resolveExpr: (Expression, LogicalPlan) => Expression)
                                                (clause: T, planToResolveAction: LogicalPlan,
                                                 fakeTargetPlan: LogicalPlan,
                                                 fakeSourcePlan: LogicalPlan,
                                                 merge: IcebergMergeInto3,
                                                 target: LogicalPlan): T = {
    val typ = clause.clauseType.toUpperCase(Locale.ROOT)
    val resolvedActions: Seq[IcebergMergeAction] = clause.actions.flatMap { action =>
      action match {
        // For actions like `UPDATE SET *` or `INSERT *`
        case _: UnresolvedStar =>
          // Expand `*` into seq of [ `columnName = sourceColumnBySameName` ] for every target
          // column name. The target columns do not need resolution. The right hand side
          // expression (i.e. sourceColumnBySameName) needs to be resolved only by the source
          // plan.
          fakeTargetPlan.output.map(_.name).map { tgtColName =>
            val resolvedExpr = resolveExpression(resolveExpr)(
              UnresolvedAttribute.quotedString(s"`$tgtColName`"),
              fakeSourcePlan, s"$typ clause")
            IcebergMergeAction(Seq(tgtColName), resolvedExpr)
          }

        // For actions like `UPDATE SET x = a, y = b` or `INSERT (x, y) VALUES (a, b)`
        case IcebergMergeAction(colNameParts, expr) =>
          val unresolvedAttrib = UnresolvedAttribute(colNameParts)
          val resolutionErrorMsg =
            s"Cannot resolve ${unresolvedAttrib.sql} in target columns in $typ " +
              s"clause given columns ${target.output.map(_.sql).mkString(", ")}"

          // Resolve the target column name without database/table/view qualifiers
          // If clause allows nested field to be target, then this will return the all the
          // parts of the name (e.g., "a.b" -> Seq("a", "b")). Otherwise, this will
          // return only one string.
          val resolvedNameParts = getNameParts(
            resolveExpression(resolveExpr)(unresolvedAttrib, fakeTargetPlan, s"$typ clause"),
            resolutionErrorMsg,
            merge)

          val resolvedExpr = resolveExpression(resolveExpr)(expr, planToResolveAction, s"$typ clause")
          Seq(IcebergMergeAction(resolvedNameParts, resolvedExpr))

        case _ =>
          throw new AnalysisException(s"Unexpected action expression '$action' in clause $clause")
      }
    }

    val resolvedCondition =
      clause.condition.map(resolveExpression(resolveExpr)(_, planToResolveAction, s"$typ condition"))
    clause.makeCopy(Array(resolvedCondition, resolvedActions)).asInstanceOf[T]
  }

  /**
   * Extracts name parts from a resolved expression referring to a nested or non-nested column
   * - For non-nested column, the resolved expression will be like `AttributeReference(...)`.
   * - For nested column, the resolved expression will be like `Alias(GetStructField(...))`.
   *
   * In the nested case, the function recursively traverses through the expression to find
   * the name parts. For example, a nested field of a.b.c would be resolved to an expression
   *
   *    `Alias(c, GetStructField(c, GetStructField(b, AttributeReference(a)))`
   *
   * for which this method recursively extracts the name parts as follows:
   *
   *    `Alias(c, GetStructField(c, GetStructField(b, AttributeReference(a)))`
   *    ->  `GetStructField(c, GetStructField(b, AttributeReference(a)))`
   *      ->  `GetStructField(b, AttributeReference(a))` ++ Seq(c)
   *        ->  `AttributeReference(a)` ++ Seq(b, c)
   *          ->  [a, b, c]
   */
  def getNameParts(resolvedTargetCol: Expression,
                   errMsg: String,
                   errNode: LogicalPlan): Seq[String] = {

    def fail(extraMsg: String): Nothing = {
      throw new AnalysisException(
        s"$errMsg - $extraMsg", errNode.origin.line, errNode.origin.startPosition)
    }

    def extractRecursively(expr: Expression): Seq[String] = expr match {
      case attr: AttributeReference => Seq(attr.name)

      case Alias(c, _) => extractRecursively(c)

      case GetStructField(c, _, Some(name)) => extractRecursively(c) :+ name

      case _: ExtractValue =>
        fail("Updating nested fields is only supported for StructType.")

      case other: Any =>
        fail(s"Found unsupported expression '$other' while parsing target column name parts")
    }

    extractRecursively(resolvedTargetCol)
  }

}
