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

package org.apache.iceberg.flink.connector.sink.merge;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.connector.model.CombinedScanTaskWrapper;
import org.apache.iceberg.flink.connector.sink.TaskWriterFactory;
import org.apache.iceberg.flink.connector.table.RowRewriter;
import org.apache.iceberg.io.FileIO;

public class DataFileRewriteOperator extends
    AbstractRewriteOperator<Tuple2<CombinedScanTaskWrapper, Integer>, DataFileRewriteOperatorOut> {
  private TaskWriterFactory<Row> writerFactory;
  private FileIO io;
  private Schema schema;
  private EncryptionManager encryptionManager;
  private boolean caseSensitive;

  private transient RowRewriter rowRewriter;

  public DataFileRewriteOperator(Schema schema,
                                 FileIO io,
                                 EncryptionManager encryptionManager,
                                 boolean caseSensitive,
                                 Configuration config,
                                 TaskWriterFactory<Row> writerFactory) {
    super(config);
    this.io = io;
    this.schema = schema;
    this.encryptionManager = encryptionManager;
    this.writerFactory = writerFactory;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public void open() throws Exception {
    this.rowRewriter = new RowRewriter(io, schema, encryptionManager, caseSensitive, writerFactory);
    writerFactory.initialize(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getAttemptNumber());
  }

  @Override
  public void processElement(StreamRecord<Tuple2<CombinedScanTaskWrapper, Integer>> element) throws Exception {
    if (element != null) {
      CombinedScanTaskWrapper taskInfo = element.getValue().f0;
      int totalCombinedScanTaskNums = element.getValue().f1;
      CombinedScanTask task = taskInfo.getTask();
      List<DataFile> currentDataFiles = task.files().stream()
          .map(FileScanTask::file)
          .collect(Collectors.toList());
      List<DataFile> rewriteDataFiles = rowRewriter.rewriteDataForTask(task);
      DataFileRewriteOperatorOut result = new DataFileRewriteOperatorOut(
          rewriteDataFiles, currentDataFiles, taskInfo.getCurrentTaskMillis(),
          totalCombinedScanTaskNums, getRuntimeContext().getIndexOfThisSubtask());
      output.collect(new StreamRecord<>(result));
    } else {
      LOG.warn("Get empty element.");
    }
  }

  @Override
  public void endInput() throws Exception {
  }
}
