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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class FlinkParquetReaders {
  private FlinkParquetReaders() {
  }

  public static ParquetValueReader<Row> buildReader(Schema expectedSchema, MessageType fileSchema) {
    return buildReader(expectedSchema, fileSchema, ImmutableMap.of());
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<Row> buildReader(Schema expectedSchema,
                                                        MessageType fileSchema,
                                                        Map<Integer, ?> idToConstant) {
    ReadBuilder builder = new ReadBuilder(fileSchema, idToConstant);
    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      return (ParquetValueReader<Row>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema, builder);
    } else {
      return (ParquetValueReader<Row>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new FallbackReadBuilder(builder));
    }
  }

  private static class FallbackReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    private MessageType type;
    private final TypeWithSchemaVisitor<ParquetValueReader<?>> builder;

    FallbackReadBuilder(TypeWithSchemaVisitor<ParquetValueReader<?>> builder) {
      this.builder = builder;
    }

    @Override
    public ParquetValueReader<?> message(Types.StructType expected, MessageType message,
                                         List<ParquetValueReader<?>> fieldReaders) {
      // the top level matches by ID, but the remaining IDs are missing
      this.type = message;
      return builder.struct(expected, message, fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(Types.StructType ignored, GroupType struct,
                                        List<ParquetValueReader<?>> fieldReaders) {
      // the expected struct is ignored because nested fields are never found when the
      List<ParquetValueReader<?>> newFields = Lists.newArrayListWithExpectedSize(
          fieldReaders.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(fieldReaders.size());
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
        newFields.add(ParquetValueReaders.option(fieldType, fieldD, fieldReaders.get(i)));
        types.add(fieldType);
      }

      return new RowDataReader(types, newFields);
    }
  }

  private static class ReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    private final MessageType type;
    private final Map<Integer, ?> idToConstant;

    ReadBuilder(MessageType type, Map<Integer, ?> idToConstant) {
      this.type = type;
      this.idToConstant = idToConstant;
    }

    @Override
    public ParquetValueReader<?> message(Types.StructType expected, MessageType message,
                                         List<ParquetValueReader<?>> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(Types.StructType expected, GroupType struct,
                                        List<ParquetValueReader<?>> fieldReaders) {
      // match the expected struct's order
      Map<Integer, ParquetValueReader<?>> readersById = Maps.newHashMap();
      Map<Integer, Type> typesById = Maps.newHashMap();
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
        if (fieldType.getId() != null) {
          int id = fieldType.getId().intValue();
          readersById.put(id, ParquetValueReaders.option(fieldType, fieldD, fieldReaders.get(i)));
          typesById.put(id, fieldType);
        }
      }

      List<Types.NestedField> expectedFields = expected != null ?
          expected.fields() : ImmutableList.of();
      List<ParquetValueReader<?>> reorderedFields = Lists.newArrayListWithExpectedSize(
          expectedFields.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(expectedFields.size());
      for (Types.NestedField field : expectedFields) {
        int id = field.fieldId();
        if (idToConstant.containsKey(id)) {
          // containsKey is used because the constant may be null
          reorderedFields.add(ParquetValueReaders.constant(idToConstant.get(id)));
          types.add(null);
        } else {
          ParquetValueReader<?> reader = readersById.get(id);
          if (reader != null) {
            reorderedFields.add(reader);
            types.add(typesById.get(id));
          } else {
            reorderedFields.add(ParquetValueReaders.nulls());
            types.add(null);
          }
        }
      }

      return new RowDataReader(types, reorderedFields);
    }

    @Override
    public ParquetValueReader<?> list(Types.ListType expectedList, GroupType array,
                                      ParquetValueReader<?> elementReader) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName())) - 1;

      return new ArrayReader<>(repeatedD, repeatedR, ParquetValueReaders.option(elementType, elementD, elementReader));
    }

    @Override
    public ParquetValueReader<?> map(Types.MapType expectedMap, GroupType map,
                                     ParquetValueReader<?> keyReader,
                                     ParquetValueReader<?> valueReader) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName())) - 1;
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName())) - 1;

      return new MapReader<>(repeatedD, repeatedR,
          ParquetValueReaders.option(keyType, keyD, keyReader),
          ParquetValueReaders.option(valueType, valueD, valueReader));
    }

    @Override
    public ParquetValueReader<?> primitive(org.apache.iceberg.types.Type.PrimitiveType expected,
                                           PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return new StringReader(desc);
          case INT_8:
          case INT_16:
          case INT_32:
            if (expected != null && expected.typeId() == Types.LongType.get().typeId()) {
              return new ParquetValueReaders.IntAsLongReader(desc);
            } else {
              return new ParquetValueReaders.UnboxedReader<>(desc);
            }
          case DATE:
            return new DateReader(desc);
          case TIME_MICROS:
            return new TimeMillisReader(desc);
          case INT_64:
            return new ParquetValueReaders.UnboxedReader<>(desc);
          case TIMESTAMP_MICROS:
            if (((Types.TimestampType) expected.asPrimitiveType()).shouldAdjustToUTC()) {
              return new TimestampTzReader(desc);
            } else {
              return new TimestampReader(desc);
            }
          case DECIMAL:
            DecimalLogicalTypeAnnotation decimal = (DecimalLogicalTypeAnnotation) primitive.getLogicalTypeAnnotation();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new BinaryDecimalReader(desc, decimal.getScale());
              case INT64:
                return new LongDecimalReader(desc, decimal.getScale());
              case INT32:
                return new IntegerDecimalReader(desc, decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return new ParquetValueReaders.ByteArrayReader(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return new ParquetValueReaders.ByteArrayReader(desc);
        case INT32:
          if (expected != null && expected.typeId() == org.apache.iceberg.types.Type.TypeID.LONG) {
            return new ParquetValueReaders.IntAsLongReader(desc);
          } else {
            return new ParquetValueReaders.UnboxedReader<>(desc);
          }
        case FLOAT:
          if (expected != null && expected.typeId() == org.apache.iceberg.types.Type.TypeID.DOUBLE) {
            return new ParquetValueReaders.FloatAsDoubleReader(desc);
          } else {
            return new ParquetValueReaders.UnboxedReader<>(desc);
          }
        case BOOLEAN:
        case INT64:
        case DOUBLE:
          return new ParquetValueReaders.UnboxedReader<>(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }

    protected MessageType type() {
      return type;
    }
  }

  private static class BinaryDecimalReader extends ParquetValueReaders.PrimitiveReader<BigDecimal> {
    private final int scale;

    BinaryDecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public BigDecimal read(BigDecimal ignored) {
      Binary binary = column.nextBinary();
      return new BigDecimal(new BigInteger(binary.getBytes()), scale);
    }
  }

  private static class IntegerDecimalReader extends ParquetValueReaders.PrimitiveReader<BigDecimal> {
    private final int scale;

    IntegerDecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public BigDecimal read(BigDecimal ignored) {
      return BigDecimal.valueOf(column.nextInteger(), scale);
    }
  }

  private static class LongDecimalReader extends ParquetValueReaders.PrimitiveReader<BigDecimal> {
    private final int scale;

    LongDecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public BigDecimal read(BigDecimal ignored) {
      return BigDecimal.valueOf(column.nextLong(), scale);
    }
  }

  private static class DateReader extends ParquetValueReaders.PrimitiveReader<LocalDate> {
    private DateReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDate read(LocalDate reuse) {
      return DateTimeUtil.dateFromDays(column.nextInteger());
    }
  }

  private static class TimestampTzReader extends ParquetValueReaders.UnboxedReader<Instant> {
    TimestampTzReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Instant read(Instant ignored) {
      long value = readLong();
      return Instant.ofEpochSecond(value / 1000_000, (value % 1000_000) * 1000);
    }

    @Override
    public long readLong() {
      return column.nextLong();
    }
  }

  private static class TimestampReader extends ParquetValueReaders.UnboxedReader<LocalDateTime> {
    TimestampReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDateTime read(LocalDateTime ignored) {
      long value = readLong();
      return DateTimeUtil.timestampFromMicros(value);
    }

    @Override
    public long readLong() {
      return column.nextLong();
    }
  }

  private static class StringReader extends ParquetValueReaders.PrimitiveReader<String> {
    StringReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public String read(String ignored) {
      Binary binary = column.nextBinary();
      ByteBuffer buffer = binary.toByteBuffer();
      if (buffer.hasArray()) {
        return new String(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      } else {
        return new String(binary.getBytes());
      }
    }
  }

  private static class TimeMillisReader extends ParquetValueReaders.PrimitiveReader<LocalTime> {
    TimeMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalTime read(LocalTime reuse) {
      // Flink only supports millisecond, so we discard microseconds in millisecond
      return LocalTime.ofNanoOfDay(column.nextLong() * 1000);
    }
  }

  // target type, intermediate type, element type.
  private static class ArrayReader<E> extends ParquetValueReaders.RepeatedReader<E[], ReusableArray, E> {
    private int readPos = 0;
    private int writePos = 0;

    ArrayReader(int definitionLevel, int repetitionLevel, ParquetValueReader<E> reader) {
      super(definitionLevel, repetitionLevel, reader);
    }

    @Override
    protected ReusableArray newListData(E[] reuse) {
      this.readPos = 0;
      this.writePos = 0;
      return new ReusableArray(reuse);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected E getElement(ReusableArray list) {
      E value = null;
      if (readPos < list.capacity()) {
        value = (E) list.values[readPos];
      }

      readPos += 1;

      return value;
    }

    @Override
    protected void addElement(ReusableArray reused, E element) {
      if (writePos >= reused.capacity()) {
        reused.grow();
      }

      reused.values[writePos] = element;

      writePos += 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected E[] buildList(ReusableArray array) {
      array.setNumElements(writePos);
      return (E[]) array.toObjectArray();
    }
  }

  private static class MapReader<K, V> extends ParquetValueReaders.MapReader<K, V> {
    MapReader(int definitionLevel, int repetitionLevel, ParquetValueReader<K> keyReader,
              ParquetValueReader<V> valueReader) {
      super(definitionLevel, repetitionLevel, keyReader, valueReader);
    }
  }

  private static class RowDataReader extends ParquetValueReaders.StructReader<Row, Row> {
    private final int numFields;

    RowDataReader(List<Type> types, List<ParquetValueReader<?>> readers) {
      super(types, readers);
      this.numFields = readers.size();
    }

    @Override
    protected Row newStructData(Row reuse) {
      if (reuse != null) {
        return reuse;
      } else {
        return new Row(numFields);
      }
    }

    @Override
    protected Object getField(Row intermediate, int pos) {
      return intermediate.getField(pos);
    }

    @Override
    protected Row buildStruct(Row struct) {
      return struct;
    }

    @Override
    protected void set(Row row, int pos, Object value) {
      row.setField(pos, value);
    }

    @Override
    protected void setNull(Row row, int pos) {
      row.setField(pos, null);
    }

    @Override
    protected void setBoolean(Row row, int pos, boolean value) {
      row.setField(pos, value);
    }

    @Override
    protected void setInteger(Row row, int pos, int value) {
      row.setField(pos, value);
    }

    @Override
    protected void setLong(Row row, int pos, long value) {
      row.setField(pos, value);
    }

    @Override
    protected void setFloat(Row row, int pos, float value) {
      row.setField(pos, value);
    }

    @Override
    protected void setDouble(Row row, int pos, double value) {
      row.setField(pos, value);
    }
  }

  private static class ReusableArray {
    private static final Object[] EMPTY = new Object[0];

    private Object[] values = EMPTY;
    private int numElements = 0;

    ReusableArray(){
    }

    ReusableArray(Object[] reuse) {
      if (reuse != null) {
        this.values = reuse;
        this.numElements = reuse.length;
      }
    }

    private void grow() {
      if (values.length == 0) {
        this.values = new Object[20];
      } else {
        Object[] old = values;
        this.values = new Object[old.length << 1];
        // copy the old array in case it has values that can be reused
        System.arraycopy(old, 0, values, 0, old.length);
      }
    }

    private int capacity() {
      return values.length;
    }

    public void setNumElements(int numElements) {
      this.numElements = numElements;
    }

    public int size() {
      return numElements;
    }

    public Object get(int ordinal) {
      return values[ordinal];
    }

    public Object[] toObjectArray() {
      return Arrays.copyOfRange(values, 0, numElements);
    }
  }
}
