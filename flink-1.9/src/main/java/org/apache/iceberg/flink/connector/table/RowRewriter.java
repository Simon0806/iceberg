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

package org.apache.iceberg.flink.connector.table;

import java.io.Serializable;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.connector.sink.TaskWriterFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowRewriter implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RowRewriter.class);
  private FileIO io;
  private Schema schema;
  private EncryptionManager encryptionManager;
  private boolean caseSensitive;
  private final TaskWriterFactory<Row> writerFactory;

  public RowRewriter(FileIO io,
                     Schema schema,
                     EncryptionManager encryptionManager,
                     boolean caseSensitive,
                     TaskWriterFactory<Row> writerFactory) {
    this.writerFactory = writerFactory;
    this.io = io;
    this.schema = schema;
    this.encryptionManager = encryptionManager;
    this.caseSensitive = caseSensitive;
  }

  public List<DataFile> rewriteDataForTask(CombinedScanTask task) throws Exception {

    TaskWriter<Row> writer = writerFactory.create();
    RowReader dataReader = new RowReader(task, io, schema, encryptionManager, caseSensitive);
    try {
      while (dataReader.hasNext()) {
        Row row = dataReader.next();
        writer.write(row);
      }
      dataReader.close();
      dataReader = null;
      writer.close();
      return Lists.newArrayList(writer.complete());
    } catch (Throwable originalThrowable) {
      try {
        LOG.error("Aborting task", originalThrowable);
        if (dataReader != null) {
          dataReader.close();
        }
        writer.abort();
      } catch (Throwable inner) {
        if (originalThrowable != inner) {
          originalThrowable.addSuppressed(inner);
          LOG.warn("Suppressing exception in catch: {}", inner.getMessage(), inner);
        }
      }

      if (originalThrowable instanceof Exception) {
        throw originalThrowable;
      } else {
        throw new RuntimeException(originalThrowable);
      }
    }
  }

}
