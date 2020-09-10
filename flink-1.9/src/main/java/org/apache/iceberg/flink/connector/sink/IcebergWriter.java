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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:ClassTypeParameterName")
public class IcebergWriter<IN> extends AbstractStreamOperator<DataFile>
    implements OneInputStreamOperator<IN, DataFile>, BoundedOneInput,
    ProcessingTimeCallback {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

  private final RecordSerializer<IN> serializer;
  private final TaskWriterFactory<Row> taskWriterFactory;
  private final String identifier;
  private final boolean skipIncompatibleRecord;
  private final long flushCommitInterval;
  private final Schema schema;
  private final TimeUnit timestampUnit;
  private final long maxFileSize;

  private transient int subtaskId;
  private transient ProcessingTimeService processingTimeService;
  private transient TaskWriter<Row> writer;

  public IcebergWriter(Table table, RecordSerializer<IN> serializer, Configuration config,
                       TaskWriterFactory<Row> taskWriterFactory) {
    this.serializer = serializer;
    this.taskWriterFactory = taskWriterFactory;

    // When timestampField inputted is
    // (1) A field name (i.e. neither null nor an empty string), the logic extracts the named field.
    // (2) Null or an empty string, the related logic does not get executed (i.e. disabled).
    // See WatermarkTimeExtractor for details.
    // Watermark timestamp function is not fully tested, so disable it for now.
    // When timestampField is a field name (i.e. neither null nor an empty string), throw an exception to exit.
    String timestampField = config
        .getString(IcebergConnectorConstant.WATERMARK_TIMESTAMP_FIELD, "");
    Preconditions.checkArgument(Strings.isNullOrEmpty(timestampField),
        "Watermark timestamp function is disabled");

    this.timestampUnit = TimeUnit.valueOf(config.getString(IcebergConnectorConstant.WATERMARK_TIMESTAMP_UNIT,
        IcebergConnectorConstant.DEFAULT_WATERMARK_TIMESTAMP_UNIT));

    this.maxFileSize = config.getLong(IcebergConnectorConstant.MAX_FILE_SIZE_BYTES,
        IcebergConnectorConstant.DEFAULT_MAX_FILE_SIZE_BYTES);
    Preconditions.checkArgument(this.maxFileSize > 0,
        String.format("%s must be positive, but is %d",
            IcebergConnectorConstant.MAX_FILE_SIZE_BYTES, this.maxFileSize));
    LOG.info("{} is set to {} bytes", IcebergConnectorConstant.MAX_FILE_SIZE_BYTES, this.maxFileSize);

    this.skipIncompatibleRecord = config.getBoolean(IcebergConnectorConstant.SKIP_INCOMPATIBLE_RECORD,
        IcebergConnectorConstant.DEFAULT_SKIP_INCOMPATIBLE_RECORD);
    LOG.info("{} is set to {}", IcebergConnectorConstant.SKIP_INCOMPATIBLE_RECORD, this.skipIncompatibleRecord);

    this.identifier = config.getString(IcebergConnectorConstant.IDENTIFIER, "");
    this.schema = table.schema();
    this.flushCommitInterval = config.getLong(IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL,
        IcebergConnectorConstant.DEFAULT_FLUSH_COMMIT_INTERVAL);
    Preconditions.checkArgument(flushCommitInterval > 0,
        String.format("%s must be positive, but is %d",
            IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval));
    LOG.info("{} is set to {} ms", IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval);
    LOG.info("Iceberg writer {} data file location: {}", identifier, table.locationProvider().newDataLocation(""));
    LOG.info("Iceberg writer {} created with sink config", identifier);
    LOG.info("Iceberg writer {} loaded table: schema = {}\npartition spec = {}", identifier, schema, table.spec());
    // default ChainingStrategy is set to HEAD
    // we prefer chaining to avoid the huge serialization and deserialization overhead.
    super.setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.subtaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.processingTimeService = getRuntimeContext().getProcessingTimeService();
    boolean isCheckpointEnabled = getRuntimeContext().isCheckpointingEnabled();
    final long currentTimestamp = processingTimeService.getCurrentProcessingTime();
    // If we don't enable checkpoint, we will use timeservice to do commit,
    if (!isCheckpointEnabled) {
      processingTimeService.registerTimer(currentTimestamp + flushCommitInterval, this);
      LOG.info("Checkpointing is disabled, but timer is registered with {} as {} ms",
          IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval);
    }

    this.taskWriterFactory.initialize(subtaskId, getRuntimeContext().getAttemptNumber());
    this.writer = taskWriterFactory.create();

    LOG.info("{} is set to {}",
        IcebergConnectorConstant.WRITER_PARALLELISM, getRuntimeContext().getNumberOfParallelSubtasks());
    LOG.info("{} is set to {} bytes", IcebergConnectorConstant.MAX_FILE_SIZE_BYTES, maxFileSize);
    LOG.info("{} is set to {}", IcebergConnectorConstant.SKIP_INCOMPATIBLE_RECORD, skipIncompatibleRecord);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    LOG.info("Iceberg writer {} subtask {} begin preparing for checkpoint {}", identifier, subtaskId, checkpointId);
    // close all open files and emit files to downstream committer operator
    flush(true);
    LOG.info("Iceberg writer {} subtask {} completed preparing for checkpoint {}", identifier, subtaskId, checkpointId);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
  }

  @VisibleForTesting
  void flush(boolean emit) throws IOException {
    List<DataFile> dataFiles = Arrays.asList(writer.complete());
    for (DataFile dataFile : dataFiles) {
      if (emit) {
        emit(dataFile);
      }
    }
    // TODO can optimize this to avoid create in every flush.
    this.writer = taskWriterFactory.create();
    LOG.info("Iceberg writer {} subtask {} flushed {} open files", identifier, subtaskId, dataFiles.size());
  }

  void emit(DataFile dataFile) {
    output.collect(new StreamRecord<>(dataFile));
    LOG.debug("Iceberg writer {} subtask {} emitted data file to committer" +
        " with {} records and {} bytes on this path: {}",
        identifier, subtaskId, dataFile.recordCount(), dataFile.fileSizeInBytes(), dataFile.path());
  }

  @Override
  public void close() throws Exception {
    super.close();

    LOG.info("Iceberg writer {} subtask {} begin close", identifier, subtaskId);
    // close all open files without emitting to downstream committer
    flush(false);
    processingTimeService.shutdownAndAwaitPending(flushCommitInterval, TimeUnit.MILLISECONDS);
    LOG.info("Iceberg writer {} subtask {} completed close", identifier, subtaskId);
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();

    LOG.info("Iceberg writer {} subtask {} begin dispose", identifier, subtaskId);
    if (writer != null) {
      writer.close();
      writer = null;
    }
    LOG.info("Iceberg writer {} subtask {} completed dispose", identifier, subtaskId);
  }

  @Override
  public void processElement(StreamRecord<IN> element) throws Exception {
    IN value = element.getValue();
    try {
      writer.write(serializer.serialize(value, schema));
    } catch (Exception t) {
      if (!skipIncompatibleRecord) {  // not skip but throw the exception out
        throw t;
      } else {  // swallow and log the exception
        LOG.warn("Incompatible record [{}] processed with exception: ", value, t);
      }
    }
  }

  @Override
  public void onProcessingTime(long timestamp) throws Exception {
    flush(true);
    final long currentTimestamp = processingTimeService.getCurrentProcessingTime();
    processingTimeService.registerTimer(currentTimestamp + flushCommitInterval, this);
  }

  @Override
  public void endInput() throws Exception {
    // For bounded source. when there is no data , will call this
    // method to flush the final data.
    flush(true);
  }
}
