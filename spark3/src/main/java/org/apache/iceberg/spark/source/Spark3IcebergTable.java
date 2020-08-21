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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.IcebergTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.functions;

// This is used for DataFrame UPDATE/DELETE/MERGE INTO
public class Spark3IcebergTable extends IcebergSource implements IcebergTable {
  private final Table icebergTable;
  private final SparkSession spark;
  private final boolean isCaseSensitive;
  private String alias;

  private DelegatableDeletesSupport3 deletesSupport = null;
  private DelegatableUpdatesSupport3 updatesSupport = null;

  public Spark3IcebergTable(String tableName) {
    spark = SparkSession.active();

    Map<String, String> options = ImmutableMap.of("path", tableName);
    Configuration conf = new Configuration(spark.sessionState().newHadoopConf());
    icebergTable = findTable(options, conf);

    isCaseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive", "false"));
  }

  public String name() {
    return this.icebergTable.toString();
  }

  public SparkSession sparkSession() {
    return spark;
  }

  public boolean caseSensitive() {
    return isCaseSensitive;
  }

  private String normalizedName() {
    String[] nameParts = name().split("\\.");
    String tableName;

    if (nameParts.length > 2) {
      tableName = nameParts[nameParts.length - 2] + "." + nameParts[nameParts.length - 1];
    } else {
      tableName = name();
    }

    return tableName;
  }

  public Schema schema() {
    return icebergTable.schema();
  }

  private LogicalPlan plan() {
    Dataset<Row> df = spark.read().format("iceberg").load(normalizedName());
    if (alias != null) {
      df = df.as(alias);
    }

    return df.queryExecution().analyzed();
  }

  @Override
  public Spark3IcebergTable as(String newAlias) {
    this.alias = newAlias;
    return this;
  }

  private DelegatableUpdatesSupport3 getUpdateSupport() {
    if (updatesSupport == null) {
      updatesSupport = new DelegatableUpdatesSupport3(icebergTable, plan(), spark, isCaseSensitive);
    }
    return updatesSupport;
  }

  private DelegatableDeletesSupport3 getDeletesSupport() {
    if (deletesSupport == null) {
      deletesSupport = new DelegatableDeletesSupport3(icebergTable, plan(), spark, isCaseSensitive);
    }
    return deletesSupport;
  }

  @Override
  public void delete(Expression conditionExpr) {
    getDeletesSupport().deleteWhere(conditionExpr);
  }

  @Override
  public void delete(String condition) {
    Expression conditionExpr;
    if (condition == null || condition.equals("")) {
      conditionExpr = functions.lit(true).expr();
    } else {
      conditionExpr = functions.expr(condition).expr();
    }

    delete(conditionExpr);
  }

  @Override
  public void update(String condition, Map<String, String> assignments) {
    Expression resolvedCondition;
    if (condition == null || condition.equals("")) {
      resolvedCondition = functions.lit(true).expr();
    } else {
      resolvedCondition = functions.expr(condition).expr();
    }

    Map<String, Expression> assign = Maps.newHashMap();
    for (Map.Entry<String, String> entry : assignments.entrySet()) {
      assign.put(entry.getKey(), functions.expr(entry.getValue()).expr());
    }

    getUpdateSupport().updateTable(assign, resolvedCondition);
  }

  @Override
  public void updateExpr(String condition, Map<String, Expression> assignments) {
    Expression resolvedCondition;
    if (condition == null || condition.equals("")) {
      resolvedCondition = functions.lit(true).expr();
    } else {
      resolvedCondition = functions.expr(condition).expr();
    }
    getUpdateSupport().updateTable(assignments, resolvedCondition);
  }

  @Override
  public IcebergMergeBuilder3 merge(Dataset<Row> source) {
    return IcebergMergeBuilder3.apply(icebergTable, sparkSession(), plan(), caseSensitive(), source);
  }
}
