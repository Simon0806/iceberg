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

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

class RecordPartitioner extends AbstractPartitioner<Row> {

  private final Accessor<Row>[] accessors;

  @SuppressWarnings("unchecked")
  RecordPartitioner(PartitionSpec spec) {
    super(spec);

    final Schema schema = spec.schema();
    final List<PartitionField> fields = spec.fields();
    final int numFields = fields.size();
    this.accessors = (Accessor<Row>[]) Array.newInstance(Accessor.class, numFields);
    final Map<Integer, Accessor<Row>> idToAccessorMap = buildAccessors(schema);
    for (int i = 0; i < numFields; i += 1) {
      PartitionField field = fields.get(i);
      Accessor<Row> accessor = idToAccessorMap.get(field.sourceId());
      if (accessor == null) {
        throw new RuntimeException(
            "Cannot build accessor for field: " + schema.findField(field.sourceId()));
      }
      this.accessors[i] = accessor;
    }
  }

  RecordPartitioner(RecordPartitioner toCopy) {
    super(toCopy);
    accessors = toCopy.accessors;
  }

  @Override
  public RecordPartitioner copy() {
    return new RecordPartitioner(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(RecordPartitioner.class.toString());
    sb.append("[");
    sb.append(super.toString());
    sb.append("]");
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void partition(Row record) {
    for (int i = 0; i < partitionTuple.length; i += 1) {
      Transform<Object, Object> transform = transforms[i];
      partitionTuple[i] = transform.apply(accessors[i].get(record));
    }
  }

  private interface Accessor<T> {
    Object get(T container);
  }

  private static Map<Integer, Accessor<Row>> buildAccessors(Schema schema) {
    return TypeUtil.visit(schema, new BuildPositionAccessors());
  }

  private static Accessor<Row> newAccessor(int position, Type type) {
    switch (type.typeId()) {
      case TIMESTAMP:
        return new TimeStampAccessor(position, ((Types.TimestampType) type).shouldAdjustToUTC());
      case TIME:
        return new TimeAccessor(position);
      case DATE:
        return new DateAccessor(position);
      default:
        return new PositionAccessor(position);
    }
  }

  private static Accessor<Row> newAccessor(int position, boolean isOptional,
                                              Accessor<Row> accessor) {
    if (isOptional) {
      // the wrapped position handles null layers
      return new WrappedPositionAccessor(position, accessor);
    } else if (accessor != null && accessor.getClass() == PositionAccessor.class) {
      return new Position2Accessor(position, (PositionAccessor) accessor);
    } else if (accessor instanceof Position2Accessor) {
      return new Position3Accessor(position, (Position2Accessor) accessor);
    } else {
      return new WrappedPositionAccessor(position, accessor);
    }
  }

  private static class BuildPositionAccessors
      extends TypeUtil.SchemaVisitor<Map<Integer, Accessor<Row>>> {
    @Override
    public Map<Integer, Accessor<Row>> schema(
        Schema schema, Map<Integer, Accessor<Row>> structResult) {
      return structResult;
    }

    @Override
    public Map<Integer, Accessor<Row>> struct(
        Types.StructType struct, List<Map<Integer, Accessor<Row>>> fieldResults) {
      Map<Integer, Accessor<Row>> accessors = Maps.newHashMap();
      List<Types.NestedField> fields = struct.fields();
      for (int i = 0; i < fieldResults.size(); i += 1) {
        Types.NestedField field = fields.get(i);
        Map<Integer, Accessor<Row>> result = fieldResults.get(i);
        if (result != null) {
          for (Map.Entry<Integer, Accessor<Row>> entry : result.entrySet()) {
            accessors.put(entry.getKey(), newAccessor(i, field.isOptional(), entry.getValue()));
          }
        } else {
          accessors.put(field.fieldId(), newAccessor(i, field.type()));
        }
      }

      if (accessors.isEmpty()) {
        return null;
      }

      return accessors;
    }

    @Override
    public Map<Integer, Accessor<Row>> field(
        Types.NestedField field, Map<Integer, Accessor<Row>> fieldResult) {
      return fieldResult;
    }
  }

  private static class PositionAccessor implements Accessor<Row> {
    private final int position;

    private PositionAccessor(int position) {
      this.position = position;
    }

    @Override
    public Object get(Row record) {
      return record.getField(position);
    }

    int position() {
      return position;
    }
  }

  private static class Position2Accessor implements Accessor<Row> {
    private final int p0;
    private final int p1;

    private Position2Accessor(int position, PositionAccessor wrapped) {
      this.p0 = position;
      this.p1 = wrapped.position;
    }

    @Override
    public Object get(Row record) {
      Row inner = (Row) record.getField(p0);
      return inner.getField(p1);
    }
  }

  private static class Position3Accessor implements Accessor<Row> {
    private final int p0;
    private final int p1;
    private final int p2;

    private Position3Accessor(int position, Position2Accessor wrapped) {
      this.p0 = position;
      this.p1 = wrapped.p0;
      this.p2 = wrapped.p1;
    }

    @Override
    public Object get(Row record) {
      Row inner = (Row) record.getField(p0);
      Row inner2 = (Row) inner.getField(p1);
      return inner2.getField(p2);
    }
  }

  private static class WrappedPositionAccessor implements Accessor<Row> {
    private final int position;
    private final Accessor<Row> accessor;

    private WrappedPositionAccessor(int position, Accessor<Row> accessor) {
      this.position = position;
      this.accessor = accessor;
    }

    @Override
    public Object get(Row record) {
      Row inner = (Row) record.getField(position);
      if (inner != null) {
        return accessor.get(inner);
      }
      return null;
    }
  }

  private static class TimeStampAccessor extends PositionAccessor {
    private final boolean withZone;

    private TimeStampAccessor(int position, boolean withZone) {
      super(position);
      this.withZone = withZone;
    }

    @Override
    public Object get(Row record) {
      return withZone ?
        DateTimeUtil.microsFromTimestamptz((java.time.OffsetDateTime) record.getField(position())) :
        DateTimeUtil.microsFromTimestamp((java.time.LocalDateTime) record.getField(position()));
    }
  }

  private static class TimeAccessor extends PositionAccessor {
    private TimeAccessor(int position) {
      super(position);
    }

    @Override
    public Object get(Row record) {
      return DateTimeUtil.microsFromTime((java.time.LocalTime) record.getField(position()));
    }
  }

  private static class DateAccessor extends PositionAccessor {
    private DateAccessor(int position) {
      super(position);
    }

    @Override
    public Object get(Row record) {
      return DateTimeUtil.daysFromDate((java.time.LocalDate) record.getField(position()));
    }
  }
}
