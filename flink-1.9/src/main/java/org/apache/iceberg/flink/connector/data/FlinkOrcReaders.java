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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;

public class FlinkOrcReaders {
  private FlinkOrcReaders() {
  }

  static OrcValueReader<String> strings() {
    return StringReader.INSTANCE;
  }

  static OrcValueReader<Integer> dates() {
    return DateReader.INSTANCE;
  }

  static OrcValueReader<BigDecimal> decimals(int precision, int scale) {
    if (precision <= 18) {
      return new Decimal18Reader(precision, scale);
    } else {
      return new Decimal38Reader(precision, scale);
    }
  }

  static OrcValueReader<Integer> times() {
    return TimeReader.INSTANCE;
  }

  static OrcValueReader<Timestamp> timestamps() {
    return TimestampReader.INSTANCE;
  }

  static OrcValueReader<Timestamp> timestampTzs() {
    return TimestampTzReader.INSTANCE;
  }

  static <T> OrcValueReader<Object[]> array(OrcValueReader<T> elementReader) {
    return new ArrayReader<>(elementReader);
  }

  public static OrcValueReader<Map<?, ?>> map(OrcValueReader<?> keyReader, OrcValueReader<?> valueReader) {
    return new MapReader<>(keyReader, valueReader);
  }

  public static OrcValueReader<Row> struct(List<OrcValueReader<?>> readers,
                                           Types.StructType struct,
                                           Map<Integer, ?> idToConstant) {
    return new StructReader(readers, struct, idToConstant);
  }

  private static class StringReader implements OrcValueReader<String> {
    private static final StringReader INSTANCE = new StringReader();

    @Override
    public String nonNullRead(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      return new String(bytesVector.vector[row]);
    }
  }

  private static class DateReader implements OrcValueReader<Integer> {
    private static final DateReader INSTANCE = new DateReader();

    @Override
    public Integer nonNullRead(ColumnVector vector, int row) {
      return (int) ((LongColumnVector) vector).vector[row];
    }
  }

  private static class Decimal18Reader implements OrcValueReader<BigDecimal> {
    private final int precision;
    private final int scale;

    Decimal18Reader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public BigDecimal nonNullRead(ColumnVector vector, int row) {
      HiveDecimalWritable value = ((DecimalColumnVector) vector).vector[row];
      return BigDecimal.valueOf(value.serialize64(value.scale()), value.scale());
    }
  }

  private static class Decimal38Reader implements OrcValueReader<BigDecimal> {
    private final int precision;
    private final int scale;

    Decimal38Reader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public BigDecimal nonNullRead(ColumnVector vector, int row) {
      return ((DecimalColumnVector) vector).vector[row].getHiveDecimal().bigDecimalValue();
    }
  }

  private static class TimeReader implements OrcValueReader<Integer> {
    private static final TimeReader INSTANCE = new TimeReader();

    @Override
    public Integer nonNullRead(ColumnVector vector, int row) {
      long micros = ((LongColumnVector) vector).vector[row];
      // Flink only support time mills, just erase micros.
      return (int) (micros / 1000);
    }
  }

  private static class TimestampReader implements OrcValueReader<Timestamp> {
    private static final TimestampReader INSTANCE = new TimestampReader();

    @Override
    public Timestamp nonNullRead(ColumnVector vector, int row) {
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      Instant localDate = Instant
          .ofEpochSecond(Math.floorDiv(tcv.time[row], 1_000), tcv.nanos[row])
          .atOffset(ZoneOffset.UTC)
          .toInstant();
      return Timestamp.from(localDate);
    }
  }

  private static class TimestampTzReader implements OrcValueReader<Timestamp> {
    private static final TimestampTzReader INSTANCE = new TimestampTzReader();

    @Override
    public Timestamp nonNullRead(ColumnVector vector, int row) {
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      Instant instant = Instant.ofEpochSecond(Math.floorDiv(tcv.time[row], 1_000), tcv.nanos[row])
          .atOffset(ZoneOffset.UTC)
          .toInstant();
      return Timestamp.from(instant);
    }
  }

  private static class ArrayReader<T> implements OrcValueReader<Object[]> {
    private final OrcValueReader<T> elementReader;

    private ArrayReader(OrcValueReader<T> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public Object[] nonNullRead(ColumnVector vector, int row) {
      ListColumnVector listVector = (ListColumnVector) vector;
      int offset = (int) listVector.offsets[row];
      int length = (int) listVector.lengths[row];
      List<T> elements = Lists.newArrayListWithExpectedSize(length);
      for (int c = 0; c < length; ++c) {
        elements.add(elementReader.read(listVector.child, offset + c));
      }
      return elements.toArray();
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      elementReader.setBatchContext(batchOffsetInFile);
    }
  }

  private static class MapReader<K, V> implements OrcValueReader<Map<?, ?>> {
    private final OrcValueReader<K> keyReader;
    private final OrcValueReader<V> valueReader;

    private MapReader(OrcValueReader<K> keyReader, OrcValueReader<V> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public Map<K, V> nonNullRead(ColumnVector vector, int row) {
      MapColumnVector mapVector = (MapColumnVector) vector;
      int offset = (int) mapVector.offsets[row];
      long length = mapVector.lengths[row];

      Map<K, V> map = Maps.newHashMap();
      for (int c = 0; c < length; c++) {
        K key = keyReader.read(mapVector.keys, offset + c);
        V value = valueReader.read(mapVector.values, offset + c);
        map.put(key, value);
      }
      return map;
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      keyReader.setBatchContext(batchOffsetInFile);
      valueReader.setBatchContext(batchOffsetInFile);
    }
  }

  private static class StructReader extends OrcValueReaders.StructReader<Row> {
    private final int numFields;

    StructReader(List<OrcValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
      super(readers, struct, idToConstant);
      this.numFields = readers.size();
    }

    @Override
    protected Row create() {
      return new Row(numFields);
    }

    @Override
    protected void set(Row struct, int pos, Object value) {
      struct.setField(pos, value);
    }
  }
}
