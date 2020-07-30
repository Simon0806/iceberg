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

package org.apache.iceberg.flink.connector.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.connector.FlinkSchemaUtil;
import org.apache.iceberg.flink.connector.IcebergSnapshotFunction;
import org.apache.iceberg.flink.connector.IcebergTableUtil;
import org.apache.iceberg.flink.connector.table.RowReader;
import org.apache.iceberg.types.TypeUtil;

public class IcebergSource {
  private IcebergSource() {
  }

  public static DataStream<Row> createSource(StreamExecutionEnvironment env, Configuration conf, long fromSnapshotId,
                                             long minSnapshotPollingMillis, long maxSnapshotPollingMillis,
                                             long asOfTime, boolean caseSensitive, TableSchema readSchema) {
    return createSource(env, conf, fromSnapshotId, minSnapshotPollingMillis,  maxSnapshotPollingMillis, asOfTime,
        caseSensitive, Long.MAX_VALUE, readSchema);
  }

  public static DataStream<Row> createSource(StreamExecutionEnvironment env, Configuration conf, long fromSnapshotId,
                                             long minSnapshotPollingMillis, long maxSnapshotPollingMillis,
                                             long asOfTime, boolean caseSensitive, long remainingSnapshot,
                                             TableSchema readSchema) {
    IcebergSnapshotFunction snapshotFunc = new IcebergSnapshotFunction(conf, fromSnapshotId, minSnapshotPollingMillis,
        maxSnapshotPollingMillis, asOfTime, caseSensitive, remainingSnapshot);
    Table table = IcebergTableUtil.findTable(conf);
    Schema icebergReadSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(readSchema), table.schema());

    return env.addSource(snapshotFunc, IcebergSnapshotFunction.class.getCanonicalName())
        .setParallelism(1)
        .flatMap((FlatMapFunction<CombinedScanTask, Row>) (task, collector) -> {
          Table tableInFlatMap = IcebergTableUtil.findTable(conf);
          RowReader reader = new RowReader(task, tableInFlatMap.io(), icebergReadSchema, tableInFlatMap.encryption(),
              caseSensitive);
          reader.forEachRemaining(collector::collect);
          reader.close();
        }).returns(readSchema.toRowType());
  }
}

