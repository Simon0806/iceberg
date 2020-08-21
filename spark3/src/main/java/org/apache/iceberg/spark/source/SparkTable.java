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
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsMerge;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsUpdate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkTable implements org.apache.spark.sql.connector.catalog.Table,
    SupportsRead, SupportsWrite, SupportsDelete, SupportsUpdate, SupportsMerge {

  private static final Set<String> RESERVED_PROPERTIES = Sets.newHashSet("provider", "format", "current-snapshot-id");
  private static final Set<TableCapability> CAPABILITIES = ImmutableSet.of(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.OVERWRITE_DYNAMIC);

  private final Table icebergTable;
  private final StructType requestedSchema;
  private StructType lazyTableSchema = null;
  private SparkSession lazySpark = null;
  private DelegatableDeletesSupport3 deletesSupport = null;
  private DelegatableUpdatesSupport3 updatesSupport = null;
  private final Boolean caseSensitive;

  public SparkTable(Table icebergTable) {
    this(icebergTable, null);
  }

  public SparkTable(Table icebergTable, StructType requestedSchema) {
    this.icebergTable = icebergTable;
    this.requestedSchema = requestedSchema;

    if (requestedSchema != null) {
      // convert the requested schema to throw an exception if any requested fields are unknown
      SparkSchemaUtil.convert(icebergTable.schema(), requestedSchema);
    }
    caseSensitive = Boolean.parseBoolean(sparkSession().conf().get("spark.sql.caseSensitive"));
  }

  public SparkSession sparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }

    return lazySpark;
  }

  public Table table() {
    return icebergTable;
  }

  private LogicalPlan plan(String alias) {
    if (alias == null || alias.equals("")) {
      return sparkSession().table(icebergTable.toString()).queryExecution().analyzed();
    } else {
      return sparkSession().table(icebergTable.toString()).as(alias).queryExecution().analyzed();
    }
  }

  @Override
  public String name() {
    return icebergTable.toString();
  }

  @Override
  public StructType schema() {
    if (lazyTableSchema == null) {
      if (requestedSchema != null) {
        this.lazyTableSchema = SparkSchemaUtil.convert(SparkSchemaUtil.prune(icebergTable.schema(), requestedSchema));
      } else {
        this.lazyTableSchema = SparkSchemaUtil.convert(icebergTable.schema());
      }
    }

    return lazyTableSchema;
  }

  @Override
  public Transform[] partitioning() {
    return Spark3Util.toTransforms(icebergTable.spec());
  }

  @Override
  public Map<String, String> properties() {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    String fileFormat = icebergTable.properties()
        .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    propsBuilder.put("format", "iceberg/" + fileFormat);
    propsBuilder.put("provider", "iceberg");
    String currentSnapshotId = icebergTable.currentSnapshot() != null ?
        String.valueOf(icebergTable.currentSnapshot().snapshotId()) : "none";
    propsBuilder.put("current-snapshot-id", currentSnapshotId);

    icebergTable.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    SparkScanBuilder scanBuilder = new SparkScanBuilder(sparkSession(), icebergTable, options);

    if (requestedSchema != null) {
      scanBuilder.pruneColumns(requestedSchema);
    }

    return scanBuilder;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new SparkWriteBuilder(sparkSession(), icebergTable, info);
  }

  @Override
  public String toString() {
    return icebergTable.toString();
  }

  private DelegatableUpdatesSupport3 getUpdateSupport() {
    if (updatesSupport == null) {
      updatesSupport = new DelegatableUpdatesSupport3(icebergTable, plan(null), sparkSession(), caseSensitive);
    }
    return updatesSupport;
  }

  private DelegatableDeletesSupport3 getDeletesSupport() {
    if (deletesSupport == null) {
      deletesSupport = new DelegatableDeletesSupport3(icebergTable, plan(null), sparkSession(), caseSensitive);
    }
    return deletesSupport;
  }

  @Override
  public void mergeIntoWithTable(org.apache.spark.sql.connector.catalog.Table table,
                                 String targetAlias,
                                 String sourceTableName,
                                 String sourceAlias,
                                 Expression mergeCondition,
                                 Map<String, Expression> matchedActions,
                                 Map<String, Expression> notMatchedActions,
                                 Expression deleteExpression,
                                 Expression updateExpression,
                                 Expression insertExpression) {
    Dataset<Row> sourceDF = sparkSession().table(sourceTableName);
    if (sourceAlias != null && !sourceAlias.isEmpty()) {
      sourceDF = sourceDF.as(sourceAlias);
    }

    doMerge(plan(targetAlias), sourceDF, mergeCondition, matchedActions, notMatchedActions,
        deleteExpression, updateExpression, insertExpression);
  }

  @Override
  public void mergeIntoWithQuery(String targetAlias,
                                 String query,
                                 String sourceAlias,
                                 Expression mergeCondition,
                                 Map<String, Expression> matchedActions,
                                 Map<String, Expression> notMatchedActions,
                                 Expression deleteExpression,
                                 Expression updateExpression,
                                 Expression insertExpression) {
    Dataset<Row> sourceDF = sparkSession().sql(query);
    if (sourceAlias == null || sourceAlias.equals("")) {
      sourceDF = sourceDF.as(sourceAlias);
    }

    doMerge(plan(targetAlias), sourceDF, mergeCondition, matchedActions, notMatchedActions,
        deleteExpression, updateExpression, insertExpression);
  }

  @Override
  public void deleteWhere(Expression filter) {
    getDeletesSupport().deleteWhere(filter);
  }

  @Override
  public void update(Map<String, Expression> assignments, Expression filter) {
    getUpdateSupport().updateTable(assignments, filter);
  }

  private Expression toUnresolvedExpression(Expression expr) {
    return functions.expr(expr.sql()).expr();
  }

  private Map<String, Expression> toUnresolvedAction(Map<String, Expression> actions) {
    Map<String, Expression> act = Maps.newHashMap();
    for (Map.Entry<String, Expression> entry : actions.entrySet()) {
      act.put(entry.getKey(), toUnresolvedExpression(entry.getValue()));
    }

    return act;
  }

  private void doMerge(LogicalPlan target,
                       Dataset<Row> sourceDF,
                       Expression mergeCondition,
                       Map<String, Expression> matchedActions,
                       Map<String, Expression> notMatchedActions,
                       Expression deleteExpression,
                       Expression updateExpression,
                       Expression insertExpression) {
    IcebergMergeBuilder3 builder = IcebergMergeBuilder3.apply(icebergTable,
        sparkSession(),
        target,
        caseSensitive,
        sourceDF
    );

    builder = builder.onCondition(toUnresolvedExpression(mergeCondition));

    IcebergMergeMatchedActionBuilder updateActionBuilder = updateExpression == null ?
        builder.whenMatched() : builder.whenMatched(toUnresolvedExpression(updateExpression));

    builder = updateActionBuilder.update(toUnresolvedAction(matchedActions));

    if (deleteExpression != null) {
      IcebergMergeMatchedActionBuilder deleteActionBuilder = builder
          .whenMatched(toUnresolvedExpression(deleteExpression));
      builder = deleteActionBuilder.delete();
    }

    IcebergMergeNotMatchedActionBuilder insertActionBuilder = insertExpression == null ?
        builder.whenNotMatched() : builder.whenNotMatched(toUnresolvedExpression(insertExpression));

    builder = insertActionBuilder.insert(toUnresolvedAction(notMatchedActions));

    builder.execute();
  }
}
