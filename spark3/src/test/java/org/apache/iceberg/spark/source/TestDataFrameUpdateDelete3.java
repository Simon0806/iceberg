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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.IcebergTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.functions;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.TableProperties.STAGING_TABLE_NAME_TAG;
import static org.apache.iceberg.TableProperties.STAGING_TABLE_NAME_TAG_DEFAULT;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestDataFrameUpdateDelete3 {
  private static final String HIVE_DB = "test_db";
  private static TestHiveMetastore metastore;
  private static HiveClientPool clients;
  private static final String TABLE_NAME = "test_table";
  private static String sourceType;

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  private static SparkSession spark;
  private static String dbTable;
  private static HadoopTables tables;
  private static HiveCatalog catalog;

  public TestDataFrameUpdateDelete3(String source) {
    sourceType = source;
  }

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "hive" },
        new Object[] { "hadoop" }
    };
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void start() throws TException, InterruptedException {
    // clear the previous session to pass in the hive metastore uri
    try {
      spark = SparkSession.active();
      spark.stop();
    } catch (IllegalStateException e) {
      spark = null;
    }

    metastore = new TestHiveMetastore();
    metastore.start();

    HiveConf hiveConf = metastore.hiveConf();
    clients = new HiveClientPool(1, hiveConf);

    // Create DB in Hive
    String dbPath = metastore.getDatabasePath(HIVE_DB);
    Database db = new Database(HIVE_DB, "desc", dbPath, new HashMap<>());
    clients.run(client -> {
      client.createDatabase(db);
      return null;
    });
    catalog = new HiveCatalog(hiveConf);
    spark = SparkSession.builder().master("local[2]")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .getOrCreate();

    tables = new HadoopTables(spark.sessionState().newHadoopConf());
  }

  @AfterClass
  public static void stop() {
    if (clients != null) {
      clients.close();
    }
    clients = null;

    if (metastore != null) {
      metastore.stop();
    }

    metastore = null;
    if (spark != null) {
      spark.stop();
    }
    spark = null;
  }

  @Before
  public void startUnitTest() throws IOException {
    Path tablePath = temp.newFolder().toPath();
    if (sourceType.equals("hadoop")) {
      dbTable = Joiner.on('/').join(tablePath.toAbsolutePath().toUri().toString(), TABLE_NAME);
    } else {
      dbTable = Joiner.on('.').join(HIVE_DB, TABLE_NAME);
    }
  }

  @After
  public void stopUnitTest() {
    if (sourceType.equals("hive")) {
      catalog.dropTable(TableIdentifier.of(HIVE_DB, TABLE_NAME));
    }
  }

  private Table createTable(Schema schema, PartitionSpec spec) {
    if (TestDataFrameUpdateDelete3.sourceType.equals("hadoop")) {
      return tables.create(schema, spec, dbTable);
    } else {
      return catalog.createTable(TableIdentifier.of(HIVE_DB, TABLE_NAME), schema, spec);
    }
  }

  @Test
  public void testDeleteFromUnpartitionedTable() {
    createTable(SCHEMA, PartitionSpec.unpartitioned());
    // Create
    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );
    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);
    verify(create);

    IcebergTable table = IcebergTable.of(dbTable);
    table.delete("data = 'a'");
    verify(Lists.newArrayList(
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    table.delete("id = 2");
    verify(Lists.newArrayList(
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    // delete with IN expression
    table.delete("data in ('c')");
    verify(Lists.newArrayList(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    // delete with In expression
    table.delete("data in ('d')");
    verify(Lists.newArrayList(
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    // delete with Or expression
    table.delete("id = 5 or data = 'f'");
    verify(Lists.newArrayList(
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );

    // delete with GreatThan expression
    table.delete("id > 7");
    verify(Lists.newArrayList(new SimpleRecord(7, "g")));

    // delete with empty expression
    table.delete("");
    verify(Lists.newArrayList());
  }

  @Test
  public void testDeleteFromPartitionedTable() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());

    // Create
    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2")
    );
    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);
    verify(create);

    IcebergTable table = IcebergTable.of(dbTable);
    table.delete("data = 'a1'");
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete one row with partition column
    table.delete("id = 1");
    verify(Lists.newArrayList(
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete multiple rows with partition column
    table.delete("id =2");
    verify(Lists.newArrayList(
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete with In expression
    table.delete("data in ('c1', 'c2')");
    verify(Lists.newArrayList(
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"),
        new SimpleRecord(5, "e1"),
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete with Or expression
    table.delete("id = 4 or data = 'e1'");
    verify(Lists.newArrayList(
        new SimpleRecord(5, "e2"),
        new SimpleRecord(6, "f1"),
        new SimpleRecord(6, "f2"))
    );

    // delete with GreatThan expression
    table.delete("id > 5");
    verify(Lists.newArrayList(new SimpleRecord(5, "e2")));

    // delete with empty expression
    table.delete("");
    verify(Lists.newArrayList());
  }

  @Test
  public void testUpdateWithUnpartitionedTable() {
    createTable(SCHEMA, PartitionSpec.unpartitioned());

    Map<String, Expression> assignments1 = new HashMap<>();
    Map<String, String> assignments2 = new HashMap<>();

    // Create
    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2")
    );
    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);
    verify(create);

    IcebergTable table = IcebergTable.of(dbTable);
    assignments2.put("id", "0");
    table.update("data='a1'", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(0, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments2.put("id", "id+1");
    table.update("data = 'a1' or data = 'a2'", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(2, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments2.put("data", "concat(data, '_test')");
    table.update("id in (1, 2)", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments1.put("data", functions.expr("'or_test'").expr());
    table.updateExpr("id=3 or id=4", assignments1);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(4, "or_test"),
        new SimpleRecord(4, "or_test"))
    );
    assignments1.clear();

    assignments1.put("data", functions.expr("'gt3'").expr());
    table.updateExpr("id>3", assignments1);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(4, "gt3"),
        new SimpleRecord(4, "gt3"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(3, "or_test"))
    );
    assignments1.clear();

    // update with empty condition
    assignments2.put("data", "'same'");
    table.update("", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(3, "same"),
        new SimpleRecord(3, "same"),
        new SimpleRecord(4, "same"),
        new SimpleRecord(4, "same"))
    );
    assignments2.clear();
  }

  @Test
  public void testUpdateWithPartitionedTable() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());

    Map<String, Expression> assignments1 = new HashMap<>();
    Map<String, String> assignments2 = new HashMap<>();

    // Create
    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2")
    );
    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);
    verify(create);

    IcebergTable table = IcebergTable.of(dbTable);
    assignments2.put("id", "0");
    table.update("data='a1'", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(0, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments2.put("id", "id+1");
    table.update("data in ('a1', 'a2')", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(2, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments1.put("data", functions.expr("concat(data, '_test')").expr());
    table.updateExpr("id in (1, 2)", assignments1);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments1.clear();

    assignments2.put("data", "'or_test'");
    table.update("id=4 or id=3", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(4, "or_test"),
        new SimpleRecord(4, "or_test"))
    );
    assignments2.clear();

    assignments1.put("data", functions.expr("'gt3'").expr());
    table.updateExpr("id>3", assignments1);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(4, "gt3"),
        new SimpleRecord(4, "gt3"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(3, "or_test"))
    );
    assignments1.clear();

    // update with empty condition
    assignments1.put("data", functions.expr("'same'").expr());
    table.updateExpr("", assignments1);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(3, "same"),
        new SimpleRecord(3, "same"),
        new SimpleRecord(4, "same"),
        new SimpleRecord(4, "same"))
    );
    assignments1.clear();
  }

  @Test
  public void testUpdateWithMapAssignment() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());

    Map<String, Expression> assignments1 = new HashMap<>();
    Map<String, String> assignments2 = new HashMap<>();

    // Create
    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2")
    );
    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);
    verify(create);

    IcebergTable table = IcebergTable.of(dbTable);
    assignments2.put("id", "0");
    table.update("data='a1'", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(0, "a1"),
        new SimpleRecord(1, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments2.put("id", "id+1");
    table.update("data in ('a1', 'a2')", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(2, "a2"),
        new SimpleRecord(2, "b1"),
        new SimpleRecord(2, "b2"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments1.put("data", functions.expr("concat(data, '_test')").expr());
    table.updateExpr("id in (1, 2)", assignments1);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(3, "c1"),
        new SimpleRecord(3, "c2"),
        new SimpleRecord(4, "d1"),
        new SimpleRecord(4, "d2"))
    );
    assignments2.clear();

    assignments2.put("data", "'or_test'");
    table.update("id=4 or id=3", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(4, "or_test"),
        new SimpleRecord(4, "or_test"))
    );
    assignments2.clear();

    assignments1.put("data", functions.expr("'gt3'").expr());
    table.updateExpr("id>3", assignments1);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "a1_test"),
        new SimpleRecord(2, "a2_test"),
        new SimpleRecord(2, "b1_test"),
        new SimpleRecord(2, "b2_test"),
        new SimpleRecord(4, "gt3"),
        new SimpleRecord(4, "gt3"),
        new SimpleRecord(3, "or_test"),
        new SimpleRecord(3, "or_test"))
    );
    assignments1.clear();

    assignments2.put("data", "'same'");
    // update with empty condition
    table.update("", assignments2);
    verify(Lists.newArrayList(
        new SimpleRecord(1, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(2, "same"),
        new SimpleRecord(3, "same"),
        new SimpleRecord(3, "same"),
        new SimpleRecord(4, "same"),
        new SimpleRecord(4, "same"))
    );
    assignments2.clear();
  }

  @Test
  public void testStagingTableLocation() throws IOException, URISyntaxException {
    Table baseTable = createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    Map<String, String> assignments = new HashMap<>();

    List<SimpleRecord> create = Lists.newArrayList(
        new SimpleRecord(1, "a1"),
        new SimpleRecord(1, "a2")
    );

    Dataset<Row> writeDF = spark.createDataFrame(create, SimpleRecord.class);
    writeDF.select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);

    IcebergTable sparkTable = IcebergTable.of(dbTable);
    assignments.put("id", "0");
    sparkTable.update("data='a1'", assignments);

    String stagingTag = baseTable.properties().getOrDefault(STAGING_TABLE_NAME_TAG,
        STAGING_TABLE_NAME_TAG_DEFAULT);
    String stagingLocation = String.join("/", baseTable.location(), stagingTag);
    org.apache.hadoop.fs.Path stagingPath = new org.apache.hadoop.fs.Path(stagingLocation);
    FileSystem fs = Util.getFs(stagingPath, new Configuration());
    Assert.assertTrue(fs.isDirectory(stagingPath));

    String baseTablePath = new URI(baseTable.locationProvider().newDataLocation("")).normalize().toString();
    for (DataFile file : baseTable.currentSnapshot().addedFiles()) {
      String stagingFilePath = new URI(file.path().toString()).normalize().toString();
      Assert.assertTrue(stagingFilePath.startsWith(baseTablePath));
    }
  }

  private void verify(List<SimpleRecord> expected) {
    List<String> actual = spark.read().format("iceberg").load(dbTable).select("data").orderBy("data")
        .collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    Assert.assertEquals(expected.stream().map(SimpleRecord::getData).collect(Collectors.toList()), actual);
  }
}
