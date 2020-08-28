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

import org.apache.flink.types.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.connector.data.FlinkOrcReader;
import org.apache.iceberg.flink.connector.data.FlinkParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

public class RowReader extends BaseRowReader<Row>  {
  private final boolean caseSensitive;
  private final Schema readSchema;

  public RowReader(CombinedScanTask task,
                   FileIO fileIo,
                   Schema readSchema,
                   EncryptionManager encryptionManager,
                   boolean caseSensitive) {
    super(task, fileIo, encryptionManager);
    this.caseSensitive = caseSensitive;
    this.readSchema = readSchema;
  }

  @Override
  protected CloseableIterator<Row> open(FileScanTask currentTask) {
    InputFile inputFile = getInputFile(currentTask);
    return newIterable(inputFile, currentTask).iterator();
  }

  private CloseableIterable<Row> newIterable(InputFile location, FileScanTask task) {
    DataFile file = task.file();
    switch (file.format()) {
      case PARQUET:
        return Parquet.read(location)
            .project(readSchema)
            .split(task.start(), task.length())
            .createReaderFunc(fileSchema -> FlinkParquetReaders.buildReader(readSchema, fileSchema))
            .filter(task.residual())
            .caseSensitive(caseSensitive)
            .build();
      case ORC:
        return ORC.read(location)
            .project(readSchema)
            .split(task.start(), task.length())
            .createReaderFunc(fileSchema -> FlinkOrcReader.buildReader(readSchema, fileSchema))
            .filter(task.residual())
            .caseSensitive(caseSensitive)
            .build();
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot read %s file: %s", file.format().name(), file.path()));
    }
  }
}
