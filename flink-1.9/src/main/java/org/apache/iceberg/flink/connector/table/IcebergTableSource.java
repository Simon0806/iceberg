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

package org.apache.iceberg.flink.connector.table;

import java.util.Arrays;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.flink.connector.source.IcebergSource;

public class IcebergTableSource implements ProjectableTableSource<Row>, StreamTableSource<Row> {
  private final String tableIdentifier;
  private final Configuration conf;
  private final long fromSnapshotId;
  private final long minPollingIntervalMillis;
  private final long maxPollingIntervalMillis;
  private final long asOfTime;
  private final boolean caseSensitive;
  private final TableSchema tableSchema;
  private final int[] projectedFields;

  IcebergTableSource(String tableIdentifier, Configuration conf, TableSchema tableSchema) {
    this(tableIdentifier, conf, tableSchema, null);
  }

  private IcebergTableSource(String tableIdentifier, Configuration conf, TableSchema tableSchema,
                             int[] projectedFields) {
    this.tableIdentifier = tableIdentifier;
    this.conf = conf;
    this.tableSchema = tableSchema;
    this.projectedFields = projectedFields;

    // Get the optional config keys.
    this.fromSnapshotId = conf.getLong(IcebergConnectorConstant.FROM_SNAPSHOT_ID,
        IcebergConnectorConstant.DEFAULT_FROM_SNAPSHOT_ID);
    this.minPollingIntervalMillis = conf.getLong(IcebergConnectorConstant.MIN_SNAPSHOT_POLLING_INTERVAL_MILLIS,
        IcebergConnectorConstant.DEFAULT_MIN_SNAPSHOT_POLLING_INTERVAL_MILLIS);
    this.maxPollingIntervalMillis = conf.getLong(IcebergConnectorConstant.MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS,
        IcebergConnectorConstant.DEFAULT_MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS);
    this.asOfTime = conf.getLong(IcebergConnectorConstant.AS_OF_TIME,
        IcebergConnectorConstant.DEFAULT_AS_OF_TIME);
    this.caseSensitive = conf.getBoolean(IcebergConnectorConstant.CASE_SENSITIVE,
        IcebergConnectorConstant.DEFAULT_CASE_SENSITIVE);
  }

  @Override
  public DataType getProducedDataType() {
    DataType result;
    if (projectedFields == null) {
      result = tableSchema.toRowDataType();
    } else {
      String[] fullNames = tableSchema.getFieldNames();
      DataType[] fullTypes = tableSchema.getFieldDataTypes();
      result = TableSchema.builder().fields(
          Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
          Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new))
          .build()
          .toRowDataType();
    }
    return result.bridgedTo(Row.class);
  }

  @Override
  public TableSchema getTableSchema() {
    return this.tableSchema;
  }

  @Override
  public String explainSource() {
    return TableConnectorUtils.generateRuntimeName(this.getClass(), tableSchema.getFieldNames());
  }

  @Override
  public TableSource<Row> projectFields(int[] fields) {
    return new IcebergTableSource(tableIdentifier, conf, tableSchema, fields);
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
    return IcebergSource.createSource(env, conf, fromSnapshotId, minPollingIntervalMillis,
        maxPollingIntervalMillis, asOfTime, caseSensitive, tableSchema);
  }
}
