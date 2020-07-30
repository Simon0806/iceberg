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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public abstract class BaseRowReader<T> implements CloseableIterator<T> {
  private final Iterator<FileScanTask> tasks;
  private final FileIO fileIo;
  private final Map<String, InputFile> inputFiles;

  private CloseableIterator<T> closeableIter;
  private T current = null;

  BaseRowReader(CombinedScanTask task, FileIO fileIo, EncryptionManager encryptionManager) {
    this.fileIo = fileIo;
    this.tasks = task.files().iterator();
    Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(task.files().stream().map(fileScanTask ->
        EncryptedFiles.encryptedInput(
            this.fileIo.newInputFile(fileScanTask.file().path().toString()),
            fileScanTask.file().keyMetadata()))
        .collect(Collectors.toList()));
    ImmutableMap.Builder<String, InputFile> inputFileBuilder = ImmutableMap.builder();
    decryptedFiles.forEach(decrypted -> inputFileBuilder.put(decrypted.location(), decrypted));
    this.inputFiles = inputFileBuilder.build();
    this.closeableIter = CloseableIterator.empty();
  }

  protected abstract CloseableIterator<T> open(FileScanTask task);

  @Override
  public boolean hasNext() {
    if (current != null) {
      return true;
    } else {
      while (true) {
        if (closeableIter.hasNext()) {
          this.current = closeableIter.next();
          return true;
        } else if (tasks.hasNext()) {
          try {
            closeableIter.close();
          } catch (IOException e) {
            throw new RuntimeIOException(e);
          }
          closeableIter = open(tasks.next());
        } else {
          return false;
        }
      }
    }
  }

  @Override
  public T next() {
    T result = current;
    current = null;
    return result;
  }

  @Override
  public void close() throws IOException {
    // close the current iterator
    this.closeableIter.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
    return inputFiles.get(task.file().path().toString());
  }
}
