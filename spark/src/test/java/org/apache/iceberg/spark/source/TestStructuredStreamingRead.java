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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestStructuredStreamingRead {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static SparkSession spark = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void startSpark() {
    TestStructuredStreamingRead.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestStructuredStreamingRead.spark;
    TestStructuredStreamingRead.spark = null;
    currentSpark.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChanges() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test-get-changes");
    File checkpoint = new File(parent, "checkpoint");
    Table table = createTable(location.toString());

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    long initialSnapshotId = snapshotIds.get(snapshotIds.size() - 1);

    // Getting all appends from initial snapshot.
    List<StreamingReader.IndexedTask> pendingTasks = Lists.newArrayList(streamingReader.getChangesWithRateLimit(
        initialSnapshotId, -1, true, 1000));
    Assert.assertEquals(pendingTasks.size(), 4);

    AssertHelpers.assertThrows(
        "Should throw an exception when getting non-existed snapshot id",
        IllegalStateException.class,
        "Start snapshot id should exist",
        () -> streamingReader.getChangesWithRateLimit(initialSnapshotId, -1, false, 1000));

    // Getting appends from initial snapshot with index, 1st snapshot will be filtered out.
    List<StreamingReader.IndexedTask> pendingTasks1 = Lists.newArrayList(streamingReader.getChangesWithRateLimit(
        initialSnapshotId, 0, true, 1000));
    Assert.assertEquals(pendingTasks1.size(), 3);
    Assert.assertEquals(pendingTasks1.stream().filter(t -> t.snapshotId() == initialSnapshotId).count(), 0);

    // Getting appends from 2nd snapshot, 1st snapshot should be filtered out.
    long snapshotId2 = snapshotIds.get(2);
    List<StreamingReader.IndexedTask> pendingTasks2 = Lists.newArrayList(streamingReader.getChangesWithRateLimit(
        snapshotId2, -1, false, 1000));
    Assert.assertEquals(pendingTasks2.size(), 3);
    Assert.assertEquals(pendingTasks2.stream().filter(t -> t.snapshotId() == initialSnapshotId).count(), 0);

    // Getting appends from last snapshot with index, should have no task included.
    long lastSnapshotId = snapshotIds.get(0);
    List<StreamingReader.IndexedTask> pendingTasks3 = Lists.newArrayList(streamingReader.getChangesWithRateLimit(
        lastSnapshotId, 0, false, 1000));
    Assert.assertEquals(pendingTasks3.size(), 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit() throws IOException {
    File parent = temp.newFolder("parquet-changes-ratelimit");
    File location = new File(parent, "test-changes-ratelimit");
    File checkpoint = new File(parent, "checkpoint-changes-ratelimit");
    Table table = createTable(location.toString());

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    long initialSnapshotId = snapshotIds.get(snapshotIds.size() - 1);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // default rate limit
    List<StreamingReader.IndexedTask> rateLimitedTasks = Lists.newArrayList(streamingReader.getChangesWithRateLimit(
        initialSnapshotId, -1, true, 1000));
    Assert.assertEquals(rateLimitedTasks.size(), 4);

    // rate limit 2
    List<StreamingReader.IndexedTask> rateLimitedTasks1 = Lists.newArrayList(streamingReader.getChangesWithRateLimit(
        initialSnapshotId, -1, true, 2));
    Assert.assertEquals(rateLimitedTasks1.size(), 2);
    Assert.assertEquals(rateLimitedTasks1.get(0).snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(rateLimitedTasks1.get(1).snapshotId(), snapshotIds.get(2).longValue());

    // rate limit 1
    List<StreamingReader.IndexedTask> rateLimitedTasks2 = Lists.newArrayList(streamingReader.getChangesWithRateLimit(
        initialSnapshotId, -1, true, 1));
    Assert.assertEquals(rateLimitedTasks2.size(), 1);
    Assert.assertEquals(rateLimitedTasks2.get(0).snapshotId(), snapshotIds.get(3).longValue());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffset() throws IOException {
    File parent = temp.newFolder("parquet-offset");
    File location = new File(parent, "test-offset");
    File checkpoint = new File(parent, "checkpoint-offset");
    Table table = createTable(location.toString());

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);

    // default rate limit
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);
    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());

    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals(start.snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(start.index(), -1);
    Assert.assertTrue(start.isStartingSnapshotId());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end.snapshotId(), snapshotIds.get(0).longValue());
    Assert.assertEquals(end.index(), 0);
    Assert.assertFalse(end.isStartingSnapshotId());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end, end1);

    // rate limit 1
    DataSourceOptions options1 = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-files-per-batch", "1"));
    StreamingReader streamingReader1 = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options1);

    streamingReader1.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start1 = (StreamingOffset) streamingReader1.getStartOffset();
    Assert.assertEquals(start1.snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(start1.index(), -1);
    Assert.assertTrue(start1.isStartingSnapshotId());

    StreamingOffset end2 = (StreamingOffset) streamingReader1.getEndOffset();
    Assert.assertEquals(end2.snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(end2.index(), 0);
    Assert.assertTrue(end2.isStartingSnapshotId());

    streamingReader1.setOffsetRange(Optional.of(end2), Optional.empty());
    StreamingOffset end3 = (StreamingOffset) streamingReader1.getEndOffset();
    Assert.assertEquals(end3.snapshotId(), snapshotIds.get(2).longValue());
    Assert.assertEquals(end3.index(), 0);
    Assert.assertFalse(end3.isStartingSnapshotId());

    streamingReader1.setOffsetRange(Optional.of(end3), Optional.empty());
    StreamingOffset end4 = (StreamingOffset) streamingReader1.getEndOffset();
    Assert.assertEquals(end4.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(end4.index(), 0);
    Assert.assertFalse(end4.isStartingSnapshotId());

    // rate limit 3
    DataSourceOptions options2 = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-files-per-batch", "3"));
    StreamingReader streamingReader2 = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options2);

    streamingReader2.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start2 = (StreamingOffset) streamingReader2.getStartOffset();
    Assert.assertEquals(start2.snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(start2.index(), -1);
    Assert.assertTrue(start2.isStartingSnapshotId());

    StreamingOffset end5 = (StreamingOffset) streamingReader2.getEndOffset();
    Assert.assertEquals(end5.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(end5.index(), 0);
    Assert.assertFalse(end5.isStartingSnapshotId());

    streamingReader2.setOffsetRange(Optional.of(end5), Optional.empty());
    StreamingOffset end6 = (StreamingOffset) streamingReader2.getEndOffset();
    Assert.assertEquals(end6.snapshotId(), snapshotIds.get(0).longValue());
    Assert.assertEquals(end6.index(), 0);
    Assert.assertFalse(end6.isStartingSnapshotId());

    streamingReader2.setOffsetRange(Optional.of(end6), Optional.empty());
    StreamingOffset end7 = (StreamingOffset) streamingReader2.getEndOffset();
    Assert.assertEquals(end6, end7);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWithSnapshotId() throws IOException {
    File parent = temp.newFolder("parquet-snapshot-id");
    File location = new File(parent, "test-snapshot-id");
    File checkpoint = new File(parent, "checkpoint-snapshot-id");
    Table table = createTable(location.toString());

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);

    // test invalid snapshot id
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "starting-snapshot-id", "-1"));
    AssertHelpers.assertThrows("Test invalid snapshot id",
        IllegalStateException.class, "The option starting-snapshot-id -1 is not an ancestor",
        () -> source.createMicroBatchReader(Optional.empty(), checkpoint.toString(), options));

    // test specify snapshot-id
    DataSourceOptions options1 = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "starting-snapshot-id", snapshotIds.get(1).toString(),
        "max-files-per-batch", "3"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options1);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals(start.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(start.index(), -1);
    Assert.assertTrue(start.isStartingSnapshotId());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(end.index(), 2);
    Assert.assertTrue(end.isStartingSnapshotId());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end1.snapshotId(), snapshotIds.get(0).longValue());
    Assert.assertEquals(end1.index(), 0);
    Assert.assertFalse(end1.isStartingSnapshotId());

    streamingReader.setOffsetRange(Optional.of(end1), Optional.empty());
    StreamingOffset end2 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end1, end2);
  }

  private Table createTable(String location) {
    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location);

    List<List<SimpleRecord>> expected = Lists.newArrayList(
        Lists.newArrayList(new SimpleRecord(1, "1")),
        Lists.newArrayList(new SimpleRecord(2, "2")),
        Lists.newArrayList(new SimpleRecord(3, "3")),
        Lists.newArrayList(new SimpleRecord(4, "4"))
    );

    // Write records one by one to generate 4 snapshots.
    for (List<SimpleRecord> l : expected) {
      Dataset<Row> df = spark.createDataFrame(l, SimpleRecord.class);
      df.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(location);
    }
    table.refresh();

    return table;
  }
}
