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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.connector.TestUtility;
import org.apache.iceberg.flink.connector.WordCountData;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkTableSource {
  private final FileFormat fileFormat = FileFormat.PARQUET;

  @ClassRule
  public static MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
          .setNumberTaskManagers(1)
          .setNumberSlotsPerTaskManager(4)
          .build());

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String srcTableLocation;
  private String dstTableLocation;

  @Before
  public void before() throws IOException {
    this.srcTableLocation = tempFolder.newFolder().getAbsolutePath();
    this.dstTableLocation = tempFolder.newFolder().getAbsolutePath();
    Table srcTable = WordCountData.createTable(srcTableLocation, false);
    WordCountData.createTable(dstTableLocation, false);

    // Write few records to the source table.
    writeFewRecords(srcTable, srcTableLocation);
  }

  private void writeFewRecords(Table table, String tableLocation) throws IOException {
    for (int i = 1; i <= 10; i++) {
      List<Row> records = Arrays.asList(Row.of("hello", i), Row.of("word", i));
      DataFile file = TestUtility.writeRecords(records, WordCountData.SCHEMA,
          new Path(tableLocation, fileFormat.addExtension("file" + i)));
      table.newAppend().appendFile(file).commit();
    }
  }

  @Test
  public void testSource() throws Exception {
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    sEnv.enableCheckpointing(1000);

    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);
    String ddlFormat = "CREATE TABLE %s(" +
        "  word string, " +
        "  num int) " +
        "WITH (" +
        "  'connector.type'='iceberg', " +
        "  'connector.version'='0.8.0', " +
        "  'connector.iceberg.identifier'='%s'," +
        "  'connector.iceberg.writer-parallelism'='%d', " +
        "  'update-mode'='upsert')";

    tEnv.sqlUpdate(String.format(ddlFormat,
        "srcTable",
        srcTableLocation,
        1));

    tEnv.sqlUpdate(String.format(ddlFormat,
        "dstTable",
        dstTableLocation,
        2));

    tEnv.sqlUpdate("INSERT INTO dstTable SELECT word, num from srcTable");
    ClusterClient<?> flinkClient = miniClusterResource.getClusterClient();
    CompletableFuture<JobSubmissionResult> result =  flinkClient.submitJob(sEnv.getStreamGraph().getJobGraph());

    // flink source get one snapshot per second, flink sink commit once per second, so we sleep 15 seconds
    // to let it proceed completely.
    Thread.sleep(15000L);
    flinkClient.cancel(result.get().getJobID());
    TestUtility.checkTableSameRecords(srcTableLocation, dstTableLocation, WordCountData.RECORD_COMPARATOR);
  }
}
