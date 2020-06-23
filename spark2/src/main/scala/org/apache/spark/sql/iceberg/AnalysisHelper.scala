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
package org.apache.spark.sql.iceberg;

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BindReferences, Expression, If, PredicateHelper,
  SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

trait AnalysisHelper extends PredicateHelper with UpdateExpressionsSupport {
  import AnalysisHelper._

  protected def tryResolveReferences(sparkSession: SparkSession)(
                                      expr: Expression,
                                      planContainingExpr: LogicalPlan): Expression = {
    val newPlan = FakeLogicalPlan(expr, planContainingExpr.children)
    sparkSession.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExpr, _) =>
        // Return even if it did not successfully resolve
        resolvedExpr
      case _ =>
        // This is unexpected
        throw new AnalysisException(s"Could not resolve expression $expr")
    }
  }

  def resolve(sparkSession: SparkSession,
              plan: LogicalPlan,
              expr: Expression): Expression = {
    if (expr.resolved) {
      expr
    } else {
      // resolve children first because it cannot resolve functions at once
      val partialResolvedAttr = expr.transform {
        case attr: UnresolvedAttribute =>
          plan.resolve(attr.nameParts, sparkSession.sessionState.analyzer.resolver) match {
            case Some(resolvedAttr) => resolvedAttr
            case None => throw new IllegalArgumentException(s"Could not resolve $attr using columns: ${plan.output}")
          }
      }
      tryResolveReferences(sparkSession)(partialResolvedAttr, plan)
    }
  }

  def resolveAndBind(spark: SparkSession, struct: StructType, plan: LogicalPlan, expr: Expression): Expression = {
    BindReferences.bindReference(resolve(spark, plan, expr),
      plan.resolve(struct, spark.sessionState.analyzer.resolver))
  }

  def toFilter(expr : Expression) : Array[Filter] = {
    if (SubqueryExpression.hasSubquery(expr)) {
      throw new Exception(s"condition with subquery is not supported: $expr")
    }

    splitConjunctivePredicates(expr).map{ expr =>
      val filter = DataSourceStrategy.translateFilter(expr)
      if (filter.isEmpty) {
        throw new Exception(s"Exec update failed: cannot translate expression to source filter: $expr")
      }
      filter.get
    }.toArray
  }

  def buildUpdatedColumns(plan: LogicalPlan, updateExpressions: Seq[Expression], condition: Expression): Seq[Column] = {
    updateExpressions.zip(plan.output).map { case (update, original) =>
      val updated = If(condition, update, original)
      new Column(Alias(updated, original.name)())
    }
  }

  def projectPredication(plan: LogicalPlan, expr: Expression) : Seq[Expression] = {
    splitConjunctivePredicates(expr).filter(_.references.subsetOf(plan.outputSet))
  }

  def buildDataFrame(spark: SparkSession, logicalPlan: LogicalPlan) : DataFrame = {
    Dataset.ofRows(spark, logicalPlan)
  }

}

object AnalysisHelper {
  /** LogicalPlan to help resolve the given expression */
  case class FakeLogicalPlan(expr: Expression, children: Seq[LogicalPlan])
    extends LogicalPlan {
    override def output: Seq[Attribute] = Nil
  }
}
