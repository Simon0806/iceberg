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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

public class RowWrapper implements StructLike {

  private final LogicalType[] types;
  private Row row = null;

  @SuppressWarnings("unchecked")
  RowWrapper(RowType rowType, Types.StructType struct) {
    int size = rowType.getFieldCount();
    types = (LogicalType[]) Array.newInstance(LogicalType.class, size);
  }

  RowWrapper wrap(Row data) {
    this.row = data;
    return this;
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (row.getField(pos) == null) {
      return null;
    } else {
      return javaClass.cast(row.getField(pos));
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Could not set a field in the RowDataWrapper because rowData is read-only");
  }
}
