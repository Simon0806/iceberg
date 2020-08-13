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

package org.apache.iceberg.flink.connector.data;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.flink.connector.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class TestFlinkOrcWriter extends DataTest {
  private static final int NUM_RECORDS = 1;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> records = RandomGenericData.generate(schema, NUM_RECORDS, 1990L);

    File recordsFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", recordsFile.delete());

    try (FileAppender<Record> writer = ORC.write(Files.localOutput(recordsFile))
        .schema(schema)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      writer.addAll(records);
    }

    // transfer to flink row list.
    List<Row> rowDataList = Lists.newArrayList();
    try (CloseableIterable<Row> reader = ORC.read(Files.localInput(recordsFile))
        .project(schema)
        .createReaderFunc(type -> FlinkOrcReader.buildReader(schema, type))
        .build()) {
      reader.forEach(rowDataList::add);
    }

    // Test flink row write into orc and read it.
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    RowType rowType = FlinkSchemaUtil.convert(schema);
    try (FileAppender<Row> writer = ORC.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc((iSchema, typeDesc) -> FlinkOrcWriter.buildWriter(rowType, iSchema))
        .build()) {
      writer.addAll(rowDataList);
    }

    try (CloseableIterable<Row> reader = ORC.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(type -> FlinkOrcReader.buildReader(schema, type))
        .build()) {
      Iterator<Row> expected = rowDataList.iterator();
      Iterator<Row> rows = reader.iterator();
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        Assert.assertEquals(expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
      Assert.assertFalse("Should not have extra rows", expected.hasNext());
    }
  }

}
