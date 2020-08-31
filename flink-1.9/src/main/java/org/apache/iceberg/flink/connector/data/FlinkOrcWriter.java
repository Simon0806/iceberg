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

import java.io.IOException;
import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class FlinkOrcWriter implements OrcRowWriter<Row> {

  private final FlinkOrcWriters.StructWriter writer;

  private FlinkOrcWriter(RowType rowType, Schema iSchema) {
    this.writer = (FlinkOrcWriters.StructWriter) FlinkOrcSchemaVisitor.visit(rowType, iSchema, new WriteBuilder());
  }

  public static OrcRowWriter<Row> buildWriter(RowType rowType, Schema iSchema) {
    return new FlinkOrcWriter(rowType, iSchema);
  }

  @Override
  public void write(Row row, VectorizedRowBatch output) throws IOException {
    int rowId = output.size;
    output.size += 1;

    List<OrcValueWriter<?>> writers = writer.writers();
    for (int c = 0; c < writers.size(); ++c) {
      OrcValueWriter child = writers.get(c);
      child.write(rowId, row.getField(c), output.cols[c]);
    }
  }

  private static class WriteBuilder extends FlinkOrcSchemaVisitor<OrcValueWriter<?>> {
    private WriteBuilder() {
    }

    @Override
    public OrcValueWriter<Row> record(Types.StructType iStruct,
                                      List<OrcValueWriter<?>> results,
                                      List<LogicalType> fieldType) {
      return FlinkOrcWriters.struct(results, fieldType);
    }

    @Override
    public OrcValueWriter<?> map(Types.MapType iMap, OrcValueWriter<?> key, OrcValueWriter<?> value,
                                 LogicalType keyType, LogicalType valueType) {
      return FlinkOrcWriters.map(key, value, keyType, valueType);
    }

    @Override
    public OrcValueWriter<?> list(Types.ListType iList, OrcValueWriter<?> element, LogicalType elementType) {
      return FlinkOrcWriters.list(element, elementType);
    }

    @Override
    public OrcValueWriter<?> primitive(Type.PrimitiveType iPrimitive, LogicalType flinkPrimitive) {
      switch (iPrimitive.typeId()) {
        case BOOLEAN:
          return GenericOrcWriters.booleans();
        case INTEGER:
          return GenericOrcWriters.ints();
        case LONG:
          return GenericOrcWriters.longs();
        case FLOAT:
          return GenericOrcWriters.floats();
        case DOUBLE:
          return GenericOrcWriters.doubles();
        case DATE:
          return FlinkOrcWriters.dates();
        case TIME:
          return FlinkOrcWriters.times();
        case TIMESTAMP:
          Types.TimestampType timestampType = (Types.TimestampType) iPrimitive;
          if (timestampType.shouldAdjustToUTC()) {
            return FlinkOrcWriters.timestampTzs();
          } else {
            return FlinkOrcWriters.timestamps();
          }
        case STRING:
          return FlinkOrcWriters.strings();
        case UUID:
          return GenericOrcWriters.uuids();
        case FIXED:
        case BINARY:
          return GenericOrcWriters.byteArrays();
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) iPrimitive;
          return FlinkOrcWriters.decimals(decimalType.scale(), decimalType.precision());
        default:
          throw new IllegalArgumentException(String.format(
              "Invalid iceberg type %s corresponding to Flink logical type %s", iPrimitive, flinkPrimitive));
      }
    }
  }
}
