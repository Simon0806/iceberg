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

import java.nio.ByteBuffer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.connector.FlinkSchemaUtil;
import org.apache.iceberg.flink.connector.RandomData;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestRowWrapper extends AbstractTestBase {

  private static final Types.StructType SUPPORTED_PRIMITIVES = Types.StructType.of(
      required(100, "id", Types.LongType.get()),
      optional(101, "data", Types.StringType.get()),
      required(102, "b", Types.BooleanType.get()),
      optional(103, "i", Types.IntegerType.get()),
      required(104, "l", Types.LongType.get()),
      optional(105, "f", Types.FloatType.get()),
      required(106, "d", Types.DoubleType.get()),
      optional(107, "date", Types.DateType.get()),
      required(108, "ts_tz", Types.TimestampType.withZone()),
      required(109, "ts", Types.TimestampType.withoutZone()),
      required(110, "s", Types.StringType.get()),
      required(111, "fixed", Types.FixedType.ofLength(7)),
      optional(112, "bytes", Types.BinaryType.get()),
      required(113, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(114, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(115, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
      required(116, "time", Types.TimeType.get())
  );

  private static final Schema RANDOM_SCHEMA = new Schema(SUPPORTED_PRIMITIVES.fields());

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testRowWrapper() {
    RowType flinkType = FlinkSchemaUtil.convert(RANDOM_SCHEMA);
    RowWrapper rowWrapper = new RowWrapper(flinkType, SUPPORTED_PRIMITIVES);
    Iterable<Row> rows = RandomData.generate(RANDOM_SCHEMA, 1, 1);
    rowWrapper = rowWrapper.wrap(rows.iterator().next());
    Object integerValue = rowWrapper.get(7, Type.TypeID.DATE.javaClass());
    Assert.assertTrue(integerValue instanceof Integer);

    Object longValue = rowWrapper.get(8, Type.TypeID.TIMESTAMP.javaClass());
    Assert.assertTrue(longValue instanceof Long);

    Object longValue2 = rowWrapper.get(9, Type.TypeID.TIMESTAMP.javaClass());
    Assert.assertTrue(longValue2 instanceof Long);

    Object byteValue = rowWrapper.get(11, Type.TypeID.FIXED.javaClass());
    Assert.assertTrue(byteValue instanceof ByteBuffer);
  }
}
