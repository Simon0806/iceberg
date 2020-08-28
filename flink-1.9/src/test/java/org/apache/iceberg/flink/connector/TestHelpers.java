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

package org.apache.iceberg.flink.connector;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

public class TestHelpers {
  private TestHelpers() {}

  private static final OffsetDateTime EPOCH = Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  public static void assertRow(Type type, Record expectedRecord, Row actualRow) {
    if (expectedRecord == null && actualRow == null) {
      return;
    }

    Assert.assertTrue("expected and actual should be both null or not null",
        expectedRecord != null && actualRow != null);

    List<Type> types = new ArrayList<>();
    for (Types.NestedField field : type.asStructType().fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      if (expectedRecord.get(i) == null) {
        Assert.assertNull(actualRow.getField(i));
        continue;
      }

      Type.TypeID typeId = types.get(i).typeId();
      Object actualValue = actualRow.getField(i);
      Object expectedValue = expectedRecord.get(i);

      switch (typeId) {
        case BOOLEAN:
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
          Assert.assertEquals("primitive value should be equal", expectedValue, actualValue);
          break;
        case STRING:
          Assert.assertTrue("Should expect a String", actualValue instanceof String);
          Assert.assertEquals("string should be equal", String.valueOf(expectedValue), actualValue);
          break;
        case DATE:
          Assert.assertTrue("Should expect a LocalDate", actualValue instanceof LocalDate);
          Assert.assertEquals("date should be equal", expectedValue, actualValue);
          break;
        case TIME:
          Assert.assertTrue("Should expect a LocalTime", actualValue instanceof LocalTime);
          Assert.assertEquals("time should be equal", expectedValue, actualValue);
          break;
        case TIMESTAMP:
          if (((Types.TimestampType) types.get(i)).shouldAdjustToUTC()) {
            Assert.assertTrue("Should expect a OffsetDataTime", expectedValue instanceof OffsetDateTime);
            Assert.assertTrue("Should expect a Instant", actualValue instanceof Instant);
            OffsetDateTime ts = (OffsetDateTime) expectedValue;
            Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
                ((Instant) actualValue).atOffset(ZoneOffset.UTC).toLocalDateTime());
          } else {
            Assert.assertTrue("Should expect a LocalDataTime", expectedValue instanceof LocalDateTime);
            LocalDateTime ts = (LocalDateTime) expectedValue;
            Assert.assertEquals("LocalDataTime should be equal", ts, actualValue);
          }
          break;
        case FIXED:
          Assert.assertTrue("Should expect a ByteBuffer", actualValue instanceof byte[]);
          Assert.assertArrayEquals("binary should be equal", (byte[]) expectedValue, (byte[]) actualValue);
          break;
        case BINARY:
          Assert.assertTrue("Should expect a ByteBuffer", actualValue instanceof byte[]);
          Assert.assertEquals("binary should be equal", expectedValue, ByteBuffer.wrap((byte[]) actualValue));
          break;
        case DECIMAL:
          Assert.assertTrue("Should expect a BigDecimal", actualValue instanceof BigDecimal);
          Assert.assertEquals("decimal value should be equal", expectedValue, actualValue);
          break;
        case LIST:
          Assert.assertTrue("Should expect an Array", actualValue.getClass().isArray());
          List<?> actualArray = Lists.newArrayList((Object[]) actualValue);
          List<?> expectedArray = Lists.newArrayList((Collection<?>) expectedRecord.get(i));
          Assert.assertEquals("array length should be equal", expectedArray.size(), Array.getLength(actualValue));
          assertArrayValues(types.get(i).asListType().elementType(), expectedArray, actualArray);
          break;
        case MAP:
          Assert.assertTrue("Should expect a Map", actualValue instanceof Map);
          List<?> actualKeyArray = Lists.newArrayList(((Map<?, ?>) actualValue).keySet());
          List<?> actualValueArray = Lists.newArrayList(((Map<?, ?>) actualValue).values());
          List<?> expectedKeyArray = Lists.newArrayList(((Map<?, ?>) expectedValue).keySet());
          List<?> expectedValueArray = Lists.newArrayList(((Map<?, ?>) expectedValue).values());

          Type keyType = types.get(i).asMapType().keyType();
          Type valueType = types.get(i).asMapType().valueType();
          Assert.assertEquals("map size should be equal",
              ((Map<?, ?>) actualValue).size(), ((Map<?, ?>) expectedValue).size());
          assertArrayValues(keyType, expectedKeyArray, actualKeyArray);
          assertArrayValues(valueType, expectedValueArray, actualValueArray);
          break;
        case STRUCT:
          Assert.assertTrue("Should expect a Record", actualValue instanceof Row);
          assertRow(types.get(i).asStructType(), (Record) expectedValue, (Row) actualValue);
          break;
        default:
          throw new IllegalArgumentException("Not a supported type: " + type);
      }
    }
  }

  private static void assertArrayValues(Type type, List<?> expectedArray, List<?> actualArray) {
    for (int i = 0; i < expectedArray.size(); i += 1) {
      if (expectedArray.get(i) == null) {
        Assert.assertNull(actualArray.get(i));
        continue;
      }

      Object expectedValue = expectedArray.get(i);
      Object actualValue = actualArray.get(i);

      switch (type.typeId()) {
        case BOOLEAN:
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
          Assert.assertEquals("boolean value should be equal", expectedValue, actualValue);
          break;
        case STRING:
          Assert.assertTrue("Should expect a String", actualValue instanceof String);
          Assert.assertEquals("string should be equal", String.valueOf(expectedValue), actualValue);
          break;
        case DATE:
          Assert.assertTrue("Should expect a Date", actualValue instanceof Date);
          Date expectedDate = Date.valueOf(ChronoUnit.DAYS.addTo(EPOCH_DAY, (int) expectedValue));
          Assert.assertEquals("date should be equal", expectedDate, actualValue);
          break;
        case TIME:
          Assert.assertTrue("Should expect a Time", actualValue instanceof Time);
          Time expectedTime = Time.valueOf(LocalTime.ofNanoOfDay((long) expectedValue));
          Assert.assertEquals("time should be equal", expectedTime, actualValue);
          break;
        case TIMESTAMP:
          Assert.assertTrue("Should expect a Timestamp", actualValue instanceof Timestamp);
          Timestamp ts = (Timestamp) actualValue;
          // milliseconds from nanos has already been added by getTime
          long tsMicros = (ts.getTime() * 1000) + ((ts.getNanos() / 1000) % 1000);
          Assert.assertEquals("Timestamp micros should be equal", expectedValue, tsMicros);
          break;
        case BINARY:
          Assert.assertTrue("Should expect a ByteBuffer", actualValue instanceof byte[]);
          Assert.assertArrayEquals("binary should be equal",
              ((ByteBuffer) expectedValue).array(), (byte[]) actualValue);
          break;
        case DECIMAL:
          Assert.assertTrue("Should expect a BigDecimal", actualValue instanceof BigDecimal);
          Assert.assertEquals("decimal value should be equal", expectedValue, actualValue);
          break;
        case LIST:
          Assert.assertTrue("Should expect an Array", actualValue.getClass().isArray());
          Assert.assertEquals("array length should be equal", expectedArray.size(), actualArray.size());
          List<?> actualList = Lists.newArrayList((Object[]) actualValue);
          List<?> expectedList = Lists.newArrayList((Collection<?>) expectedValue);
          assertArrayValues(type.asListType().elementType(), expectedList, actualList);
          break;
        case MAP:
          Assert.assertTrue("Should expect a Map", actualValue instanceof Map);
          List<?> actualKeyArray = Lists.newArrayList(((Map<?, ?>) actualValue).keySet());
          List<?> actualValueArray = Lists.newArrayList(((Map<?, ?>) actualValue).values());

          List<?> expectedKeyArray = Lists.newArrayList(((Map<?, ?>) expectedValue).keySet());
          List<?> expectedValueArray = Lists.newArrayList(((Map<?, ?>) expectedValue).values());

          Type keyType = type.asMapType().keyType();
          Type valueType = type.asMapType().valueType();
          Assert.assertEquals("map size should be equal",
              ((Map<?, ?>) actualValue).size(), ((Map<?, ?>) expectedValue).size());
          assertArrayValues(keyType, expectedKeyArray, actualKeyArray);
          assertArrayValues(valueType, expectedValueArray, actualValueArray);
          break;
        case STRUCT:
          Assert.assertTrue("Should expect a Row", actualValue instanceof Row);
          assertRow(type.asStructType(), (Record) expectedValue, (Row) actualValue);
          break;

        default:
          throw new IllegalArgumentException("Not a supported type: " + type);
      }
    }
  }
}
