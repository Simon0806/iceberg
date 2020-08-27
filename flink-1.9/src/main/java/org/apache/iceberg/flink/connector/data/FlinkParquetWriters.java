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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.Row;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class FlinkParquetWriters {
  private FlinkParquetWriters() {
  }

  @SuppressWarnings("unchecked")
  public static <T> ParquetValueWriter<T> buildWriter(LogicalType schema, MessageType type) {
    return (ParquetValueWriter<T>) ParquetWithFlinkSchemaVisitor.visit(schema, type, new WriteBuilder(type));
  }

  private static class WriteBuilder extends ParquetWithFlinkSchemaVisitor<ParquetValueWriter<?>> {
    private final MessageType type;

    WriteBuilder(MessageType type) {
      this.type = type;
    }

    @Override
    public ParquetValueWriter<?> message(RowType sStruct, MessageType message, List<ParquetValueWriter<?>> fields) {
      return struct(sStruct, message.asGroupType(), fields);
    }

    @Override
    public ParquetValueWriter<?> struct(RowType sStruct, GroupType struct,
                                        List<ParquetValueWriter<?>> fieldWriters) {
      List<Type> fields = struct.getFields();
      List<RowField> flinkFields = sStruct.getFields();
      List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
      List<LogicalType> flinkTypes = Lists.newArrayList();
      for (int i = 0; i < fields.size(); i += 1) {
        writers.add(newOption(struct.getType(i), fieldWriters.get(i)));
        flinkTypes.add(flinkFields.get(i).getType());
      }

      return new RowWriter(writers);
    }

    @Override
    public ParquetValueWriter<?> list(ArrayType sArray, GroupType array, ParquetValueWriter<?> elementWriter) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      return new ArrayDataWriter<>(repeatedD, repeatedR,
          newOption(repeated.getType(0), elementWriter));
    }

    @Override
    public ParquetValueWriter<?> map(MapType sMap, GroupType map,
                                     ParquetValueWriter<?> keyWriter, ParquetValueWriter<?> valueWriter) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      org.apache.parquet.schema.Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName()));
      org.apache.parquet.schema.Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName()));

      return ParquetValueWriters.maps(repeatedD, repeatedR,
          ParquetValueWriters.option(keyType, keyD, keyWriter),
          ParquetValueWriters.option(valueType, valueD, valueWriter));
    }

    private ParquetValueWriter<?> newOption(org.apache.parquet.schema.Type fieldType, ParquetValueWriter<?> writer) {
      int maxD = type.getMaxDefinitionLevel(path(fieldType.getName()));
      return ParquetValueWriters.option(fieldType, maxD, writer);
    }

    @Override
    public ParquetValueWriter<?> primitive(LogicalType sType, PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return strings(desc);
          case INT_8:
          case INT_16:
          case INT_32:
            return ints(sType, desc);
          case DATE:
            return new DateWriter(desc);
          case INT_64:
            return ParquetValueWriters.longs(desc);
          case TIME_MICROS:
            return timeMicros(desc);
          case TIMESTAMP_MICROS:
            if (((TimestampLogicalTypeAnnotation) primitive.getLogicalTypeAnnotation()).isAdjustedToUTC()) {
              return timestampTzs(desc);
            } else {
              return timestamps(desc);
            }
          case DECIMAL:
            DecimalLogicalTypeAnnotation decimal = (DecimalLogicalTypeAnnotation) primitive.getLogicalTypeAnnotation();
            switch (primitive.getPrimitiveTypeName()) {
              case INT32:
                return decimalAsInteger(desc, decimal.getPrecision(), decimal.getScale());
              case INT64:
                return decimalAsLong(desc, decimal.getPrecision(), decimal.getScale());
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return decimalAsFixed(desc, decimal.getPrecision(), decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return byteArrays(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return byteArrays(desc);
        case BOOLEAN:
          return ParquetValueWriters.booleans(desc);
        case INT32:
          return ints(sType, desc);
        case INT64:
          return ParquetValueWriters.longs(desc);
        case FLOAT:
          return ParquetValueWriters.floats(desc);
        case DOUBLE:
          return ParquetValueWriters.doubles(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  private static ParquetValueWriters.PrimitiveWriter<?> ints(LogicalType type, ColumnDescriptor desc) {
    if (type instanceof TinyIntType) {
      return ParquetValueWriters.tinyints(desc);
    } else if (type instanceof SmallIntType) {
      return ParquetValueWriters.shorts(desc);
    }
    return ParquetValueWriters.ints(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<String> strings(ColumnDescriptor desc) {
    return new StringDataWriter(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<LocalTime> timeMicros(ColumnDescriptor desc) {
    return new TimeMicrosWriter(desc);
  }

  private static class DateWriter extends ParquetValueWriters.PrimitiveWriter<LocalDate> {
    private DateWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDate value) {
      column.writeInteger(repetitionLevel, (int) ChronoUnit.DAYS.between(EPOCH_DAY, value));
    }
  }

  private static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsInteger(ColumnDescriptor desc,
                                                                                   int precision, int scale) {
    return new IntegerDecimalWriter(desc, precision, scale);
  }
  private static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsLong(ColumnDescriptor desc,
                                                                                int precision, int scale) {
    return new LongDecimalWriter(desc, precision, scale);
  }

  private static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsFixed(ColumnDescriptor desc,
                                                                                 int precision, int scale) {
    return new FixedDecimalWriter(desc, precision, scale);
  }

  private static ParquetValueWriters.PrimitiveWriter<LocalDateTime> timestamps(ColumnDescriptor desc) {
    return new TimestampWriter(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<Instant> timestampTzs(ColumnDescriptor desc) {
    return new TimestampTzWriter(desc);
  }

  private static ParquetValueWriters.PrimitiveWriter<byte[]> byteArrays(ColumnDescriptor desc) {
    return new ByteArrayWriter(desc);
  }

  private static class StringDataWriter extends ParquetValueWriters.PrimitiveWriter<String> {
    private StringDataWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, String value) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(value.getBytes()));
    }
  }

  private static class TimeMicrosWriter extends ParquetValueWriters.PrimitiveWriter<LocalTime> {
    private TimeMicrosWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalTime value) {
      long micros = value.toNanoOfDay() / 1000;
      column.writeLong(repetitionLevel, micros);
    }
  }

  private static class IntegerDecimalWriter extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    private IntegerDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      Preconditions.checkArgument(decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, decimal);
      Preconditions.checkArgument(decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, decimal);

      column.writeInteger(repetitionLevel, decimal.unscaledValue().intValue());
    }
  }

  private static class LongDecimalWriter extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    private LongDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      Preconditions.checkArgument(decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, decimal);
      Preconditions.checkArgument(decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, decimal);

      column.writeLong(repetitionLevel, decimal.unscaledValue().longValue());
    }
  }

  private static class FixedDecimalWriter extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;
    private final int length;
    private final ThreadLocal<byte[]> bytes;

    private FixedDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
      this.length = TypeUtil.decimalRequiredBytes(precision);
      this.bytes = ThreadLocal.withInitial(() -> new byte[length]);
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      Preconditions.checkArgument(decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, decimal);
      Preconditions.checkArgument(decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, decimal);

      byte fillByte = (byte) (decimal.signum() < 0 ? 0xFF : 0x00);
      byte[] unscaled = decimal.unscaledValue().toByteArray();
      byte[] buf = bytes.get();
      int offset = length - unscaled.length;

      for (int i = 0; i < length; i += 1) {
        if (i < offset) {
          buf[i] = fillByte;
        } else {
          buf[i] = unscaled[i - offset];
        }
      }

      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(buf));
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class TimestampWriter extends ParquetValueWriters.PrimitiveWriter<LocalDateTime> {
    private TimestampWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDateTime value) {
      column.writeLong(repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class TimestampTzWriter extends ParquetValueWriters.PrimitiveWriter<Instant> {
    private TimestampTzWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, Instant value) {
      column.writeLong(repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class ByteArrayWriter extends ParquetValueWriters.PrimitiveWriter<byte[]> {
    private ByteArrayWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, byte[] bytes) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(bytes));
    }
  }

  private static class ArrayDataWriter<E> extends ParquetValueWriters.RepeatedWriter<E[], E> {

    private ArrayDataWriter(int definitionLevel, int repetitionLevel,
                            ParquetValueWriter<E> writer) {
      super(definitionLevel, repetitionLevel, writer);
    }

    @Override
    protected Iterator<E> elements(E[] array) {
      return Iterators.forArray(array);
    }
  }

  private static class RowWriter extends ParquetValueWriters.StructWriter<Row> {

    RowWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Row struct, int index) {
      return struct.getField(index);
    }
  }
}
