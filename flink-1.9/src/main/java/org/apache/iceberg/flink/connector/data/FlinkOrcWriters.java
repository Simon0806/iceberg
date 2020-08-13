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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;

public class FlinkOrcWriters {

  private FlinkOrcWriters(){
  }

  static OrcValueWriter<?> strings() {
    return StringWriter.INSTANCE;
  }

  static OrcValueWriter<?> dates() {
    return DateWriter.INSTANCE;
  }

  static OrcValueWriter<?> times() {
    return TimeWriter.INSTANCE;
  }

  static OrcValueWriter<?> timestamps() {
    return TimestampWriter.INSTANCE;
  }

  static OrcValueWriter<?> timestampTzs() {
    return TimestampTzWriter.INSTANCE;
  }

  static OrcValueWriter<?> decimals(int scale, int precision) {
    if (precision <= 18) {
      return new Decimal18Writer(scale, precision);
    } else {
      return new Decimal38Writer(scale, precision);
    }
  }

  static <T> OrcValueWriter<Object[]> list(OrcValueWriter<T> elementWriter, LogicalType elementType) {
    return new ListWriter<>(elementWriter, elementType);
  }

  static <K, V> OrcValueWriter<Map<?, ?>> map(OrcValueWriter<K> keyWriter, OrcValueWriter<V> valueWriter,
                                              LogicalType keyType, LogicalType valueType) {
    return new MapWriter<>(keyWriter, valueWriter, keyType, valueType);
  }

  static OrcValueWriter<Row> struct(List<OrcValueWriter<?>> writers, List<LogicalType> types) {
    return new StructWriter(writers, types);
  }

  static class MapWriter<K, V> implements OrcValueWriter<Map<?, ?>> {
    private final OrcValueWriter<K> keyWriter;
    private final OrcValueWriter<V> valueWriter;

    MapWriter(OrcValueWriter<K> keyWriter, OrcValueWriter<V> valueWriter,
              LogicalType keyType, LogicalType valueType) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    public Class<?> getJavaClass() {
      return Map.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void nonNullWrite(int rowId, Map<?, ?> data, ColumnVector output) {
      MapColumnVector cv = (MapColumnVector) output;
      Object[] keyArray = data.keySet().toArray();
      Object[] valueArray = data.values().toArray();

      // record the length and start of the list elements
      cv.lengths[rowId] = data.size();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount += cv.lengths[rowId];
      // make sure the child is big enough
      cv.keys.ensureSize(cv.childCount, true);
      cv.values.ensureSize(cv.childCount, true);
      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        int pos = (int) (e + cv.offsets[rowId]);
        keyWriter.write(pos, (K) keyArray[e], cv.keys);
        valueWriter.write(pos, (V) valueArray[e], cv.values);
      }
    }
  }

  static class ListWriter<T> implements OrcValueWriter<Object[]> {
    private final OrcValueWriter<T> elementWriter;

    ListWriter(OrcValueWriter<T> elementWriter, LogicalType elementType) {
      this.elementWriter = elementWriter;
    }

    @Override
    public Class<?> getJavaClass() {
      return Object[].class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void nonNullWrite(int rowId, Object[] data, ColumnVector output) {
      ListColumnVector cv = (ListColumnVector) output;
      cv.lengths[rowId] = data.length;
      cv.offsets[rowId] = cv.childCount;
      cv.childCount += cv.lengths[rowId];
      // make sure the child is big enough.
      cv.child.ensureSize(cv.childCount, true);

      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        elementWriter.write((int) (e + cv.offsets[rowId]), (T) data[e], cv.child);
      }
    }
  }

  static class StructWriter implements OrcValueWriter<Row> {
    private final List<OrcValueWriter<?>> writers;

    StructWriter(List<OrcValueWriter<?>> writers, List<LogicalType> types) {
      this.writers = writers;
    }

    List<OrcValueWriter<?>> writers() {
      return writers;
    }

    @Override
    public Class<?> getJavaClass() {
      return Row.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void nonNullWrite(int rowId, Row data, ColumnVector output) {
      StructColumnVector cv = (StructColumnVector) output;
      for (int c = 0; c < writers.size(); ++c) {
        OrcValueWriter writer = writers.get(c);
        writer.write(rowId, data.getField(c), cv.fields[c]);
      }
    }
  }

  private static class StringWriter implements OrcValueWriter<String> {
    private static final StringWriter INSTANCE = new StringWriter();

    @Override
    public Class<?> getJavaClass() {
      return String.class;
    }

    @Override
    public void nonNullWrite(int rowId, String data, ColumnVector output) {
      byte[] value = data.getBytes();
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  private static class DateWriter implements OrcValueWriter<Integer> {
    private static final DateWriter INSTANCE = new DateWriter();

    @Override
    public Class<?> getJavaClass() {
      return Integer.class;
    }

    @Override
    public void nonNullWrite(int rowId, Integer data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class TimeWriter implements OrcValueWriter<Integer> {
    private static final TimeWriter INSTANCE = new TimeWriter();

    @Override
    public Class<?> getJavaClass() {
      return Integer.class;
    }

    @Override
    public void nonNullWrite(int rowId, Integer millis, ColumnVector output) {
      // The time in flink is in millisecond, while the standard time in iceberg is microsecond.
      // So we need to transform it to microsecond.
      ((LongColumnVector) output).vector[rowId] = millis * 1000;
    }
  }

  private static class TimestampWriter implements OrcValueWriter<Timestamp> {
    private static final TimestampWriter INSTANCE = new TimestampWriter();

    @Override
    public Class<?> getJavaClass() {
      return Timestamp.class;
    }

    @Override
    public void nonNullWrite(int rowId, Timestamp data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      cv.setIsUTC(true);
      // millis
      OffsetDateTime offsetDateTime = data.toInstant().atOffset(ZoneOffset.UTC);
      cv.time[rowId] = offsetDateTime.toEpochSecond() * 1_000 + offsetDateTime.getNano() / 1_000_000;
      // truncate nanos to only keep microsecond precision.
      cv.nanos[rowId] = (offsetDateTime.getNano() / 1_000) * 1_000;
    }
  }

  private static class TimestampTzWriter implements OrcValueWriter<Timestamp> {
    private static final TimestampTzWriter INSTANCE = new TimestampTzWriter();

    @Override
    public Class<Timestamp> getJavaClass() {
      return Timestamp.class;
    }

    @Override
    public void nonNullWrite(int rowId, Timestamp data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      // millis
      Instant instant = data.toInstant();
      cv.time[rowId] = instant.toEpochMilli();
      // truncate nanos to only keep microsecond precision.
      cv.nanos[rowId] = (instant.getNano() / 1_000) * 1_000;
    }
  }

  private static class TimestampLocalTzWriter implements OrcValueWriter<Long> {
    private static final TimestampLocalTzWriter INSTANCE = new TimestampLocalTzWriter();

    @Override
    public Class<Long> getJavaClass() {
      return Long.class;
    }

    @Override
    public void nonNullWrite(int rowId, Long data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      // millis
      cv.time[rowId] = data;
      // truncate nanos to only keep microsecond precision.
      cv.nanos[rowId] = (int) ((data / 1_000) * 1_000);
    }
  }

  private static class Decimal18Writer implements OrcValueWriter<BigDecimal> {
    private final int scale;
    private final int precision;

    Decimal18Writer(int scale, int precision) {
      this.scale = scale;
      this.precision = precision;
    }

    @Override
    public Class<?> getJavaClass() {
      return BigDecimal.class;
    }

    @Override
    public void nonNullWrite(int rowId, BigDecimal data, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].setFromLongAndScale(data.unscaledValue().longValue(), data.scale());
    }
  }

  private static class Decimal38Writer implements OrcValueWriter<BigDecimal> {
    private final int scale;
    private final int precision;

    Decimal38Writer(int scale, int precision) {
      this.scale = scale;
      this.precision = precision;
    }

    @Override
    public Class<?> getJavaClass() {
      return BigDecimal.class;
    }

    @Override
    public void nonNullWrite(int rowId, BigDecimal data, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].set(HiveDecimal.create(data, false));
    }
  }
}
