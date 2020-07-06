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

package org.apache.iceberg.flink.connector.sink;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkIcebergSink extends AbstractTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String tableLocation;
  private static final int TOTAL_COUNT = 20000;
  private static final String NAMESPACE = "default";
  private static final String TABLE = "table1";

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tableLocation = folder.getAbsolutePath();
  }

  @Test
  public void testDataStreamParallelismOne() throws Exception {
    testDataStream(1, false, false);
  }

  @Test
  public void testDataStreamWithCheckpoint() throws Exception {
    testDataStream(1, false, true);
  }

  private void checkData() {
    Configuration conf = new Configuration();
    Catalog catalog = new HadoopCatalog(conf, tableLocation);
    Table newTable = catalog.loadTable(TableIdentifier.parse(NAMESPACE + "." + TABLE));
    List<Record> results = Lists.newArrayList(IcebergGenerics.read(newTable).build());
    Assert.assertEquals(results.size(), TOTAL_COUNT);
  }

  private void testDataStream(int parallelism, boolean partitionTable, boolean enableCheckpoint) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Enable the checkpoint.
    if (enableCheckpoint) {
      env.enableCheckpointing(1000);
    }
    env.setParallelism(parallelism);

    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();

    Schema schema = new Schema(
        Types.NestedField.required(1, "int", Types.IntegerType.get()),
        Types.NestedField.required(2, "string", Types.StringType.get()));

    DataStream<Row> dataStream = env
        .addSource(new TestSource())
        .setParallelism(parallelism);

    Table table = createIcebergTable(tableLocation, partitionSpec, schema);
    Assert.assertNotNull(table);

    org.apache.flink.configuration.Configuration flinkConf = new org.apache.flink.configuration.Configuration();
    flinkConf.setString(IcebergConnectorConstant.IDENTIFIER, NAMESPACE + "." + TABLE);
    flinkConf.setString(IcebergConnectorConstant.CATALOG_TYPE, IcebergConnectorConstant.HADOOP_CATALOG);
    flinkConf.setString(IcebergConnectorConstant.WAREHOUSE_LOCATION, tableLocation);
    flinkConf.setLong(IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, 60 * 1000L);

    IcebergSinkAppender appender = new IcebergSinkAppender(table, flinkConf,
              PassThroughRecordSerializer.getInstance(), "IcebergSink");
    appender.append(dataStream);
    env.execute("Test Iceberg DataStream");
    table.refresh();
    checkData();
  }

  private Table createIcebergTable(String newTableLocation, PartitionSpec spec, Schema schema) {
    Configuration conf = new Configuration();
    Catalog catalog = new HadoopCatalog(conf, newTableLocation);
    TableIdentifier tableIdentifier = TableIdentifier.parse(NAMESPACE + "." + TABLE);
    if (catalog.tableExists(tableIdentifier)) {
      catalog.dropTable(tableIdentifier, true);
    }
    return catalog.createTable(tableIdentifier, schema, spec, new HashMap<>());
  }

  private static class TestSource implements ParallelSourceFunction<Row> {
    private int cnt = 0;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      while (isRunning && cnt < TOTAL_COUNT) {
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(Row.of(cnt, String.valueOf(cnt)));
          cnt++;
        }
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
