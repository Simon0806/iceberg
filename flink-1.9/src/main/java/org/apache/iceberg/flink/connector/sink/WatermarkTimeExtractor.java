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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatermarkTimeExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkTimeExtractor.class);

  private final TimeUnit timestampUnit;
  private final List<Integer> timestampFieldChain;

  public WatermarkTimeExtractor(Schema schema, String timestampFieldChainAsString, TimeUnit timestampUnit) {
    this.timestampUnit = timestampUnit;
    this.timestampFieldChain = getTimestampFieldChain(schema, timestampFieldChainAsString);
  }

  private List<Integer> getTimestampFieldChain(Schema schema, String timestampFieldChainAsString) {
    if (Strings.isNullOrEmpty(timestampFieldChainAsString)) {
      return Collections.emptyList();
    }

    List<String> fieldNames = Splitter.on(".").splitToList(timestampFieldChainAsString);
    final int size = fieldNames.size();  // size >= 1 due to the Strings.isNullOrEmpty() check ahead
    List<Integer> positionChain = new ArrayList<>(size);
    Type type = null;

    for (int i = 0; i <= size - 1; i++) {  // each field in the chain
      String fieldName = fieldNames.get(i).trim();

      if (i == 0) {  // first level, get from schema
        positionChain.add(getPosition(schema.columns(), fieldName));
        type = schema.findType(fieldName);
      } else {  // other levels including leaf, get from its parent
        positionChain.add(getPosition(type.asNestedType().fields(), fieldName));
        type = type.asNestedType().fieldType(fieldName);
      }

      if (type == null) {
        throw new IllegalArgumentException(
            String.format("Can't find field %s in schema", fieldName));
      } else {
        if (i == size - 1) {  // leaf node should be LONG type
          if (type.typeId() != Type.TypeID.LONG) {
            throw new IllegalArgumentException(
                String.format("leaf timestamp field %s is not a timestamp type, but %s", fieldName, type.typeId()));
          }
        } else {
          if (!type.isNestedType()) {
            throw new IllegalArgumentException(
                String.format("upstream field %s is not a nested type, but %s", fieldName, type.typeId()));
          }
        }
      }
    }  // each field in the chain

    LOG.info("Found matched timestamp field identified by {} in the schema", timestampFieldChainAsString);
    return positionChain;
  }

  private int getPosition(final List<Types.NestedField> fields, final String fieldName) {
    if (Strings.isNullOrEmpty(fieldName)) {
      throw new IllegalArgumentException("field name in the chain must not be null or empty");
    }

    int position = 0;
    for (Types.NestedField field : fields) {
      if (fieldName.equals(field.name())) {
        return position;
      } else {
        position++;
      }
    }

    return -1;  // error condition: target field name not found
  }

  /**
   * Extract the user provided watermark timestamp value (as long) from each input.
   *
   * @param row the input row to extract from.
   * @return return watermark timestamp value if the named field could be found.
   *         otherwise, return null.
   */
  public Long getWatermarkTimeMs(final Row row) {
    if (timestampFieldChain.isEmpty()) {
      return null;
    }

    Row rowAlongPath = row;

    final int size = timestampFieldChain.size();

    // from top to the parent of leaf
    for (int i = 0; i <= size - 2; i++) {
      rowAlongPath = (Row) rowAlongPath.getField(timestampFieldChain.get(i));
    }

    // leaf
    Long ts = (Long) rowAlongPath.getField(timestampFieldChain.get(size - 1));
    if (ts == null) {
      return null;
    } else {
      if (timestampUnit != TimeUnit.MILLISECONDS) {
        return timestampUnit.toMillis(ts);
      } else {
        return ts;
      }
    }
  }
}
