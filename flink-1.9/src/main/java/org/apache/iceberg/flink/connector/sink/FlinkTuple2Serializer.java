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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;

/**
 * Pass through the second field (f1, as {@link Row}) of {@link Tuple2},
 * only when the first field (f0, so called "message type" or "flag") is true
 */
public class FlinkTuple2Serializer implements RecordSerializer<Tuple2<Boolean, Row>> {
  private static final long serialVersionUID = 1L;
  private static final FlinkTuple2Serializer INSTANCE = new FlinkTuple2Serializer();

  public static FlinkTuple2Serializer getInstance() {
    return INSTANCE;
  }

  @Override
  public Row serialize(Tuple2<Boolean, Row> tuple, Schema schema) {
    if (tuple.f0) {
      return tuple.f1;
    } else {  // tuple.f0 = false
      throw new UnsupportedOperationException(
          "Message with false flag (f0) means to delete, but delete NOT supported yet");
    }
  }
}
