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

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.iceberg.SparkSQLUtil;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class SparkTable extends IcebergSource {
  private Table icebergTable;
  private SparkSQLUtil sqlUtil;
  private JavaSparkContext jsc;
  private SparkSession spark = SparkSession.active();
  private boolean isCaseSensitive;
  private DelegatableDeletesSupport deletesSupport = null;
  private DelegatableUpdatesSupport updatesSupport = null;

  public SparkTable(String tableName) {

    Map<String, String> options = Maps.newHashMap();
    options.put("path", tableName);
    Configuration conf = new Configuration(spark.sessionState().newHadoopConf());
    icebergTable = findTable(new DataSourceOptions(options), conf);
    jsc = new JavaSparkContext(spark.sparkContext());
    isCaseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
    sqlUtil = new SparkSQLUtil(spark.read().format("iceberg").load(normalizedName()).queryExecution().analyzed(),
        spark.sessionState().analyzer().resolver());
  }

  public String name() {
    return this.icebergTable.toString();
  }

  public JavaSparkContext jsc() {
    return jsc;
  }

  public SparkSQLUtil getSqlUtil() {
    return sqlUtil;
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

  public Table getIcebergTable() {
    return this.icebergTable;
  }

  private DelegatableUpdatesSupport getUpdateSupport() {
    if (updatesSupport == null) {
      updatesSupport = new DelegatableUpdatesSupport(this);
    }
    return updatesSupport;
  }

  private DelegatableDeletesSupport getDeletesSupport() {
    if (deletesSupport == null) {
      deletesSupport = new DelegatableDeletesSupport(this);
    }
    return deletesSupport;
  }

  public void deleteWhere(Expression conditionExpr) {
    getDeletesSupport().deleteWhere(conditionExpr);
  }

  public void deleteWhere(String condition) {
    Expression conditionExpr;
    if (condition == null || condition.equals("")) {
      conditionExpr = functions.lit(true).expr();
    } else {
      conditionExpr = functions.expr(condition).expr();
    }

    deleteWhere(conditionExpr);
  }

  public void update(Map<String, String> assignments, String condition) {
    Expression resolvedCondition;
    if (condition == null || condition.equals("")) {
      resolvedCondition = functions.lit(true).expr();
    } else {
      resolvedCondition = functions.expr(condition).expr();
    }

    Map<String, Expression> assign = new HashMap<>();
    for (Map.Entry<String, String> entry : assignments.entrySet()) {
      assign.put(entry.getKey(), functions.expr(entry.getValue()).expr());
    }

    getUpdateSupport().updateTable(assign, resolvedCondition);
  }

  public void updateExpr(Map<String, Expression> assignments, String condition) {
    Expression resolvedCondition;
    if (condition == null || condition.equals("")) {
      resolvedCondition = functions.lit(true).expr();
    } else {
      resolvedCondition = functions.expr(condition).expr();
    }
    getUpdateSupport().updateTable(assignments, resolvedCondition);
  }

}
