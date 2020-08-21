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

package org.apache.iceberg.spark.sql;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.AnalysisException;
import org.junit.Test;

public class TestMergeInto extends SparkCatalogTestBase {
  public TestMergeInto(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testMergeIntoTable() throws AnalysisException {
    spark.sql("CREATE TABLE IF NOT EXISTS " + tableName + " (id int, data string) USING iceberg ");
    spark.sql("DELETE FROM " + tableName + " WHERE id > 0");

    spark.sql("INSERT INTO TABLE " + tableName +
        " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h')");

    List<SimpleRecord> source1 = Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")
    );

    spark.createDataFrame(source1, SimpleRecord.class)
        .select("id", "data").toDF("id", "data").createGlobalTempView(catalogName + "_source1");

    spark.sql("MERGE INTO " + tableName + " AS t" +
        " USING global_temp." + catalogName + "_source1" + " AS s" +
        " ON t.id = s.id" +
        " WHEN MATCHED THEN" +
        "     UPDATE SET t.data = s.data" +
        " WHEN NOT MATCHED THEN INSERT *");

    verifyData(tableName, Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")));

    List<SimpleRecord> source2 = Lists.newArrayList(
        new SimpleRecord(4, "dd"),
        new SimpleRecord(5, "ee"),
        new SimpleRecord(6, "ff"),
        new SimpleRecord(9, "ii"),
        new SimpleRecord(10, "jj")
    );

    spark.createDataFrame(source2, SimpleRecord.class)
        .select("id", "data").toDF("id", "data").createGlobalTempView(catalogName + "_source2");

    spark.sql("MERGE INTO " + tableName + " AS t" +
        " USING global_temp." + catalogName + "_source2" + " AS s" +
        " ON t.id = s.id" +
        " WHEN MATCHED AND t.id < 6 THEN" +
        "     UPDATE SET t.data = s.data" +
        " WHEN NOT MATCHED THEN INSERT *");

    verifyData(tableName, Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(4, "dd"),
        new SimpleRecord(5, "ee"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")));

    spark.sql("MERGE INTO " + tableName + " AS t" +
        " USING global_temp." + catalogName + "_source1" + " AS s" +
        " ON t.id = s.id" +
        " WHEN MATCHED AND t.id < 4 THEN DELETE");

    verifyData(tableName, Lists.newArrayList(
        new SimpleRecord(4, "dd"),
        new SimpleRecord(5, "ee"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")));

    spark.sql("MERGE INTO " + tableName + " AS t" +
        " USING global_temp." + catalogName + "_source1" + " AS s" +
        " ON t.id = s.id" +
        " WHEN NOT MATCHED THEN INSERT *");

    verifyData(tableName, Lists.newArrayList(
        new SimpleRecord(1, "aa"),
        new SimpleRecord(2, "bb"),
        new SimpleRecord(3, "cc"),
        new SimpleRecord(4, "dd"),
        new SimpleRecord(5, "ee"),
        new SimpleRecord(6, "f"),
        new SimpleRecord(7, "g"),
        new SimpleRecord(8, "h"),
        new SimpleRecord(9, "i"),
        new SimpleRecord(10, "j")));
  }
}
