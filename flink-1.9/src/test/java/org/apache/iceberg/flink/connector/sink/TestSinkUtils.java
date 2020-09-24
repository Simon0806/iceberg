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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.connector.RandomData;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;

public class TestSinkUtils {

  private static final String NAMESPACE = "default";
  private static final String TABLE = "table1";
  private static final TableIdentifier tableIdentifier =
      TableIdentifier.parse(NAMESPACE + "." + TABLE);
  private static final long SLEEPMILLS = 1000L;

  private static Configuration conf = new Configuration();
  private static Catalog catalog;

  private TestSinkUtils(){
  }

  public static void init(String warehouseLocation) {
    catalog = new HadoopCatalog(conf, warehouseLocation);
  }

  public static void checkResult(int expectCnt) {
    Table newTable = catalog.loadTable(TableIdentifier.parse(NAMESPACE + "." + TABLE));
    List<Record> results = Lists.newArrayList(IcebergGenerics.read(newTable).build());
    Assert.assertEquals(results.size(), expectCnt);
  }

  public static Table getTable() {
    return catalog.loadTable(tableIdentifier);
  }

  public static Table createIcebergTable(PartitionSpec spec,
                                         Schema schema,
                                         Map<String, String> props) {
    if (catalog.tableExists(tableIdentifier)) {
      catalog.dropTable(tableIdentifier);
    }
    return catalog.createTable(tableIdentifier, schema, spec, props);
  }

  public static class TestFlinkSource implements ParallelSourceFunction<Row> {
    private int cnt = 0;
    private long totalSeconds;
    private volatile boolean isRunning = true;


    public TestFlinkSource(long totalSeconds) {
      this.totalSeconds = totalSeconds;

    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      while (isRunning && cnt < totalSeconds) {
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(Row.of(cnt % 3, String.valueOf(cnt)));
          cnt++;
          Thread.sleep(SLEEPMILLS);
        }
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }

  public static class TestFlinkRandomSource extends RichParallelSourceFunction<Row> {
    private int cnt = 0;
    private int totalNums;
    private int seeds;
    private Schema schema;
    private transient Iterable<Row> data;
    private volatile boolean isRunning = true;

    public TestFlinkRandomSource(Schema schema, int totalNums, int seeds) {
      this.totalNums = totalNums;
      this.seeds = seeds;
      this.schema = schema;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
      super.open(parameters);
      this.data = RandomData.generate(schema, totalNums, seeds);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      Iterator<Row> iter = data.iterator();
      while (isRunning && cnt < totalNums && iter.hasNext()) {
        synchronized (ctx.getCheckpointLock()) {
          ++cnt;
          Row row = iter.next();
          ctx.collect(row);
          Thread.sleep(SLEEPMILLS);
        }
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
