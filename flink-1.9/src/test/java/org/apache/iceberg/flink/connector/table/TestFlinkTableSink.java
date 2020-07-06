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
import java.util.HashMap;
import java.util.List;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.connector.TestUtility;
import org.apache.iceberg.flink.connector.WordCountData;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends AbstractTestBase {

  private static final String HIVE_DB = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String NAMESPACE_DOT_TABLE = HIVE_DB + "." + TABLE_NAME;

  private static Catalog catalog;

  private static TestHiveMetastore metastore;
  private static HiveConf hiveConf;
  private static HiveClientPool clients;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Parameterized.Parameter
  public boolean useOldPlanner;

  @Parameterized.Parameters(name = "{index}: useOldPlanner={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[]{true}, new Object[]{false});
  }

  public TestFlinkTableSink() throws TException, InterruptedException {
    // Hive catalog
    metastore = new TestHiveMetastore();
    metastore.start();

    hiveConf = metastore.hiveConf();
    clients = new HiveClientPool(1, hiveConf);

    // Create DB in Hive
    String dbPath = metastore.getDatabasePath(HIVE_DB);
    Database db = new Database(HIVE_DB, "desc", dbPath, new HashMap<>());
    clients.run(client -> {
      client.createDatabase(db);
      return null;
    });

    catalog = new HiveCatalog(hiveConf);
  }

  @AfterClass
  public static void stopMetastore() {
    if (clients != null) {
      clients.close();
    }
    clients = null;
    if (metastore != null) {
      metastore.stop();
    }
    metastore = null;
  }

  @Before
  public void createTable() {
    WordCountData.createTable(catalog, NAMESPACE_DOT_TABLE, true);
  }

  @After
  public void dropTable() {
    // drop table to clean
    catalog.dropTable(TableIdentifier.parse(NAMESPACE_DOT_TABLE), true);
  }

  private void testSQL(int parallelism, boolean useDDL) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(400);
    StreamTableEnvironment tEnv;
    if (useOldPlanner) {
      tEnv = StreamTableEnvironment.create(env);
    } else {
      EnvironmentSettings settings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build();
      tEnv = StreamTableEnvironment.create(env, settings);
    }

    String[] worlds = new String[]{"hello", "world", "foo", "bar", "apache", "foundation"};
    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < worlds.length; i++) {
      rows.add(Row.of(worlds[i], i + 1));
    }
    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), WordCountData.FLINK_SCHEMA.toRowType());

    tEnv.registerDataStream("words", dataStream);

    if (useDDL) {
      // The following DDL to create table ONLY takes effect in Flink and does NOT trigger table creation in Iceberg.
      // It is also used to pass schema info and connector parameters to Flink Iceberg sink.
      String ddl = String.format(
          "CREATE TABLE IcebergTable(" +
              "word string, " +
              "num int) " +
              "WITH (" +
              "'connector.type'='iceberg', " +
              "'connector.version'='0.8.0', " +
              "'connector.iceberg.catalog-type'='HIVE', " +
              "'connector.iceberg.hive-metastore-uris'='%s', " +
              "'connector.iceberg.identifier'='%s', " +
              "'connector.iceberg.writer-parallelism'='%s', " +
              "'update-mode'='upsert')",
          hiveConf.get(METASTOREURIS.varname),
          NAMESPACE_DOT_TABLE,
          parallelism);
      tEnv.sqlUpdate(ddl);
    } else {
      // Use connector descriptor to create the iceberg table.
      tEnv
          .connect(Iceberg.newInstance()
              .withVersion(IcebergValidator.CONNECTOR_VERSION_VALUE)
              .withCatalogType("HIVE")
              .withHiveMetastoreUris(hiveConf.get(METASTOREURIS.varname))
              .withIdentifier(NAMESPACE_DOT_TABLE)
              .withWriterParallelism(parallelism))
          .withSchema(new Schema().schema(WordCountData.FLINK_SCHEMA))
          .inUpsertMode()
          .registerTableSink("IcebergTable");
    }

    tEnv.sqlUpdate("INSERT INTO IcebergTable SELECT word, num from words");

    env.execute();

    // Assert the table records as expected.
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 2; i++) { // two checkpoints in the FiniteTestSource.
      for (int k = 0; k < worlds.length; k++) {
        expected.add(WordCountData.RECORD.copy(ImmutableMap.of("word", worlds[k], "num", k + 1)));
      }
    }
    TestUtility.checkIcebergTableRecords(catalog, TableIdentifier.parse(NAMESPACE_DOT_TABLE),
        expected, WordCountData.RECORD_COMPARATOR);
  }

  @Test
  public void testParallelismOneByDDL() throws Exception {
    testSQL(1, true);
  }

  @Test
  public void testParallelismOneByDescriptor() throws Exception {
    testSQL(1, false);
  }

  @Test
  public void testMultipleParallelismByDDL() throws Exception {
    testSQL(4, true);
  }

  @Test
  public void testMultipleParallelismByDescriptor() throws Exception {
    testSQL(4, false);
  }
}
