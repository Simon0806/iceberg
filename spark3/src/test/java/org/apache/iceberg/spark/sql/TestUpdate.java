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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestUpdate extends SparkCatalogTestBase {
  public TestUpdate(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testUpdateUnpartitionedTable() {
    spark.sql("CREATE TABLE IF NOT EXISTS " + tableName + " (id int, data string) USING iceberg ");
    spark.sql("DELETE FROM " + tableName + " WHERE id > 0");

    spark.sql("INSERT INTO TABLE " + tableName + " VALUES (1, '1'), (2, '2'), (3, '3')");
    List<String> ret1 = spark.sql("SELECT data FROM " + tableName + " ORDER BY id").collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList("1", "2", "3"), ret1);

    spark.sql("UPDATE " + tableName + " set data = '2' WHERE id = 1");

    verifyData(tableName, Lists.newArrayList(
        new SimpleRecord(1, "2"),
        new SimpleRecord(2, "2"),
        new SimpleRecord(3, "3"))
    );
  }

  @Test
  public void testUpdatePartitionedTable() {
    String originalParallelism = spark.conf().get("spark.sql.shuffle.partitions");
    spark.conf().set("spark.sql.shuffle.partitions", "1");

    try {
      sql("CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) USING iceberg " +
          "PARTITIONED BY (truncate(id, 2))", tableName);
      sql("DELETE FROM " + tableName + " WHERE id > 0");
      sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'c'), (3, 'c')", tableName);

      sql("UPDATE " + tableName + " set data = 'b' WHERE id = 2");
      verifyData(tableName, Lists.newArrayList(
          new SimpleRecord(1, "a"),
          new SimpleRecord(2, "b"),
          new SimpleRecord(3, "c"))
      );

    } finally {
      spark.conf().set("spark.sql.shuffle.partitions", originalParallelism);
    }
  }
}
