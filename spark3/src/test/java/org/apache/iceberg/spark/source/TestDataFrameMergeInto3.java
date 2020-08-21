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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
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
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestDataFrameMergeInto3 {
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

  public TestDataFrameMergeInto3(String source) {
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

  private void createTable(Schema schema, PartitionSpec spec) {
    if (TestDataFrameMergeInto3.sourceType.equals("hadoop")) {
      tables.create(schema, spec, dbTable);
    } else {
      catalog.createTable(TableIdentifier.of(HIVE_DB, TABLE_NAME), schema, spec);
    }
  }

  @Test
  public void testMergeIntoBasicApi() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    // Create
    List<SimpleRecord> target = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );

    Dataset<Row> writeDF = spark.createDataFrame(target, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);

    List<SimpleRecord> source = Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
    );
    Dataset<Row> sourceDF = spark.createDataFrame(source, SimpleRecord.class)
        .select("id", "data").toDF("sid", "sdata");

    IcebergTable table = IcebergTable.of(dbTable);

    Map<String, String> updateMap = new HashMap<String, String>() {
      {
        put("data", "sdata");
      }
    };

    Map<String, String> insertMap = new HashMap<String, String>() {
      {
        put("id", "sid");
        put("data", "sdata");
      }
    };

    table.merge(sourceDF).onCondition("id = sid")
        .whenMatched().updateExpr(updateMap)
        .whenNotMatched().insertExpr(insertMap)
        .execute();

    verify(Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j"))
    );
  }

  @Test
  public void testMergeIntoDeleteAndInsert() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    // Create
    List<SimpleRecord> target = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );

    Dataset<Row> writeDF = spark.createDataFrame(target, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);

    List<SimpleRecord> source = Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
    );
    Dataset<Row> sourceDF = spark.createDataFrame(source, SimpleRecord.class)
        .select("id", "data").toDF("sid", "sdata");

    IcebergTable table = IcebergTable.of(dbTable);

    Map<String, String> insertMap = new HashMap<String, String>() {
      {
        put("id", "sid");
        put("data", "sdata");
      }
    };

    table.merge(sourceDF).onCondition("id = sid")
        .whenMatched().delete()
        .whenNotMatched().insertExpr(insertMap)
        .execute();

    verify(Lists.newArrayList(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j"))
    );
  }

  @Test
  public void testMergeIntoUpdateAllAndInsertAll() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    // Create
    List<SimpleRecord> target = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );

    Dataset<Row> writeDF = spark.createDataFrame(target, SimpleRecord.class);
    writeDF
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);

    List<SimpleRecord> source = Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
    );
    Dataset<Row> sourceDF = spark.createDataFrame(source, SimpleRecord.class)
        .select("id", "data").as("s");

    IcebergTable table = IcebergTable.of(dbTable);

    table.as("t").merge(sourceDF).onCondition("t.id = s.id")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute();

    verify(Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j"))
    );
  }

  @Test
  public void testMergeIntoWithActionConditions() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    // Create
    List<SimpleRecord> target = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );

    Dataset<Row> writeDF = spark.createDataFrame(target, SimpleRecord.class);
    writeDF.select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);

    List<SimpleRecord> source = Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
    );

    Dataset<Row> sourceDF = spark.createDataFrame(source, SimpleRecord.class)
        .select("id", "data").as("s");

    IcebergTable table = IcebergTable.of(dbTable);

    Map<String, String> updateMap = new HashMap<String, String>() {
      {
        put("t.data", "s.data");
      }
    };

    Map<String, String> insertMap = new HashMap<String, String>() {
      {
        put("t.id", "s.id");
        put("t.data", "s.data");
      }
    };

    table.as("t").merge(sourceDF).onCondition("t.id = s.id")
        .whenMatched("s.data = 'aa'").updateExpr(updateMap)
        .whenNotMatched("s.data = 'j'").insertExpr(insertMap)
        .execute();

    verify(Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(10, "j"))
    );
  }

  @Test
  public void testMergeIntoOnlyDelete() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    // Create
    List<SimpleRecord> target = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );

    Dataset<Row> writeDF = spark.createDataFrame(target, SimpleRecord.class);
    writeDF.select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);

    List<SimpleRecord> source = Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
    );

    Dataset<Row> sourceDF = spark.createDataFrame(source, SimpleRecord.class)
        .select("id", "data").as("s");

    IcebergTable table = IcebergTable.of(dbTable);

    table.as("t").merge(sourceDF).onCondition("t.id = s.id")
        .whenMatched("s.data = 'aa' or t.data = 'b'").delete()
        .execute();

    verify(Lists.newArrayList(
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"))
    );
  }

  @Test
  public void testMergeIntoOnlyInsert() {
    createTable(SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("id").build());
    // Create
    List<SimpleRecord> target = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h")
    );

    Dataset<Row> writeDF = spark.createDataFrame(target, SimpleRecord.class);
    writeDF.select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .save(dbTable);

    List<SimpleRecord> source = Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
    );

    Dataset<Row> sourceDF = spark.createDataFrame(source, SimpleRecord.class)
        .select("id", "data").as("s");

    IcebergTable table = IcebergTable.of(dbTable);

    table.as("t").merge(sourceDF).onCondition("t.id = s.id")
        .whenNotMatched("s.data in ('i', 'j')").insertAll()
        .execute();

    verify(Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
        )
    );
  }

  private void verify(List<SimpleRecord> expected) {
    List<String> actual = spark.read().format("iceberg").load(dbTable).select("data").orderBy("data")
        .collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    Assert.assertEquals(expected.stream().map(SimpleRecord::getData).collect(Collectors.toList()), actual);
  }

}
