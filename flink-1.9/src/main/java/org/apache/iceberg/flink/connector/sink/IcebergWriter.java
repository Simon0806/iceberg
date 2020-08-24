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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
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
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.flink.connector.FlinkSchemaUtil;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.flink.connector.data.FlinkParquetWriters;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

@SuppressWarnings("checkstyle:ClassTypeParameterName")
public class IcebergWriter<IN> extends AbstractStreamOperator<FlinkDataFile>
    implements OneInputStreamOperator<IN, FlinkDataFile>, BoundedOneInput,
    ProcessingTimeCallback {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

  private final RecordSerializer<IN> serializer;
  private final String identifier;
  private final FileFormat format;
  private final boolean skipIncompatibleRecord;
  private final long flushCommitInterval;
  private final Schema schema;
  private final PartitionSpec spec;
  private final LocationProvider locations;
  private final FileIO io;
  private final Map<String, String> tableProps;
  private final String timestampField;
  private final TimeUnit timestampUnit;
  private final long maxFileSize;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;
  private transient Map<String, FileWriter> openPartitionFiles;
  private transient int subtaskId;
  private transient ProcessingTimeService timerService;
  private transient Partitioner<Row> partitioner;

  private transient ProcessingTimeService processingTimeService;
  private transient boolean isCheckpointEnabled;

  public IcebergWriter(Table table, RecordSerializer<IN> serializer, Configuration config) {
    this.serializer = serializer;

    // When timestampField inputted is
    // (1) A field name (i.e. neither null nor an empty string), the logic extracts the named field.
    // (2) Null or an empty string, the related logic does not get executed (i.e. disabled).
    // See WatermarkTimeExtractor for details.
    this.timestampField = config.getString(IcebergConnectorConstant.WATERMARK_TIMESTAMP_FIELD, "");
    // Watermark timestamp function is not fully tested, so disable it for now.
    // When timestampField is a field name (i.e. neither null nor an empty string), throw an exception to exit.
    if (!(this.timestampField == null || "".equals(this.timestampField))) {
      throw new IllegalArgumentException("Watermark timestamp function is disabled");
    }
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
    this.spec = table.spec();
    this.locations = table.locationProvider();
    this.io = table.io();
    this.tableProps = table.properties();
    this.format = FileFormat.valueOf(
        tableProps.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT).toUpperCase(Locale.ENGLISH));

    this.flushCommitInterval = config.getLong(IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL,
        IcebergConnectorConstant.DEFAULT_FLUSH_COMMIT_INTERVAL);
    Preconditions.checkArgument(flushCommitInterval > 0,
        String.format("%s must be positive, but is %d",
            IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval));
    LOG.info("{} is set to {} ms", IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval);

    LOG.info("Iceberg writer {} data file location: {}", identifier, locations.newDataLocation(""));
    LOG.info("Iceberg writer {} created with sink config", identifier);
    LOG.info("Iceberg writer {} loaded table: schema = {}\npartition spec = {}", identifier, schema, spec);

    // default ChainingStrategy is set to HEAD
    // we prefer chaining to avoid the huge serialization and deserialization overhead.
    super.setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void open() throws Exception {
    super.open();

    this.openPartitionFiles = new HashMap<>();
    this.hadoopConf = new org.apache.hadoop.conf.Configuration();
    this.subtaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.timerService = getProcessingTimeService();
    this.processingTimeService = getRuntimeContext().getProcessingTimeService();
    this.isCheckpointEnabled = getRuntimeContext().isCheckpointingEnabled();
    final long currentTimestamp = processingTimeService.getCurrentProcessingTime();
    // If we don't enable checkpoint, we will use timeservice to do commit,
    if (!isCheckpointEnabled) {
      processingTimeService.registerTimer(currentTimestamp + flushCommitInterval, this);
      LOG.info("Checkpointing is disabled, but timer is registered with {} as {} ms",
          IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval);
    }

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
  List<FlinkDataFile> flush(boolean emit) throws IOException {
    List<FlinkDataFile> dataFiles = new ArrayList<>(openPartitionFiles.size());
    for (Map.Entry<String, FileWriter> entry : openPartitionFiles.entrySet()) {
      FileWriter writer = entry.getValue();
      FlinkDataFile flinkDataFile = closeWriter(writer);
      dataFiles.add(flinkDataFile);
      if (emit) {
        emit(flinkDataFile);
      }
    }
    LOG.info("Iceberg writer {} subtask {} flushed {} open files", identifier, subtaskId, openPartitionFiles.size());
    openPartitionFiles.clear();
    return dataFiles;
  }

  FlinkDataFile closeWriter(FileWriter writer) throws IOException {
    FlinkDataFile flinkDataFile = writer.close();
    LOG.info(
        "Iceberg writer {} subtask {} data file closed with {} records and {} bytes on this path: {}",
        identifier, subtaskId, flinkDataFile.getIcebergDataFile().recordCount(),
        flinkDataFile.getIcebergDataFile().fileSizeInBytes(), flinkDataFile.getIcebergDataFile().path());
    return flinkDataFile;
  }

  void emit(FlinkDataFile flinkDataFile) {
    output.collect(new StreamRecord<>(flinkDataFile));
    LOG.debug("Iceberg writer {} subtask {} emitted data file to committer" +
        " with {} records and {} bytes on this path: {}",
        identifier, subtaskId, flinkDataFile.getIcebergDataFile().recordCount(),
        flinkDataFile.getIcebergDataFile().fileSizeInBytes(), flinkDataFile.getIcebergDataFile().path());
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
    abort();
    LOG.info("Iceberg writer {} subtask {} completed dispose", identifier, subtaskId);
  }

  private void abort() {
    if (openPartitionFiles != null) {
      LOG.info("Iceberg writer {} subtask {} has {} open files to abort",
          identifier, subtaskId, openPartitionFiles.size());
      // close all open files without sending DataFile list to downstream committer operator.
      // because there are not checkpointed,
      // we don't want to commit these files.
      for (Map.Entry<String, FileWriter> entry : openPartitionFiles.entrySet()) {
        final FileWriter writer = entry.getValue();
        final Path path = writer.getPath();
        try {
          LOG.debug("Iceberg writer {} subtask {} start to abort file: {}", identifier, subtaskId, path);
          writer.abort();
          LOG.info("Iceberg writer {} subtask {} completed aborting file: {}", identifier, subtaskId, path);
        } catch (Throwable t) {
          LOG.error(
              "Iceberg writer {} subtask {} failed to abort open file: {}. Throwable = {}",
              identifier, subtaskId, path.toString(), t);
          continue;
        }

        try {
          LOG.debug("Iceberg writer {} subtask {} deleting aborted file: {}", identifier, subtaskId, path);
          io.deleteFile(path.toString());
          LOG.info("Iceberg writer {} subtask {} deleted aborted file: {}", identifier, subtaskId, path);
        } catch (Throwable t) {
          LOG.error("Iceberg writer {} subtask {} failed to delete aborted file: {}. Throwable = {}",
              identifier, subtaskId, path.toString(), t);
        }
      }
      LOG.info("Iceberg writer {} subtask {} aborted {} open files", identifier, subtaskId, openPartitionFiles.size());
      openPartitionFiles.clear();
    }
  }

  @Override
  public void processElement(StreamRecord<IN> element) throws Exception {
    IN value = element.getValue();
    try {
      processInternal(value);
    } catch (Exception t) {
      if (!skipIncompatibleRecord) {  // not skip but throw the exception out
        throw t;
      } else {  // swallow and log the exception
        LOG.warn("Incompatible record [{}] processed with exception: ", value, t);
      }
    }
  }

  @VisibleForTesting
  void processInternal(IN value) throws Exception {
    Row record = serializer.serialize(value, schema);
    processRecord(record);
  }

  /**
   * process a single {@link Row} of Flink and write it into data file by {@link FileWriter}
   */
  private void processRecord(Row record) throws Exception {
    if (partitioner == null) {
      partitioner = new RecordPartitioner(spec);
    }
    partitioner.partition(record);
    final String partitionPath = locations.newDataLocation(spec, partitioner, "");
    if (!openPartitionFiles.containsKey(partitionPath)) {
      final Path path = new Path(partitionPath, generateFileName());
      FileWriter writer = newWriter(path, partitioner);
      openPartitionFiles.put(partitionPath, writer);  // TODO: 1 writer for 1 partition path?
      LOG.info("Iceberg writer {} subtask {} opened a new file: {}", identifier, subtaskId, path.toString());
    }
    final FileWriter writer = openPartitionFiles.get(partitionPath);
    final long fileSize = writer.write(record);
    // Rotate the file if over size limit.
    // This is mainly to avoid the 5 GB size limit of copying object in S3
    // that auto-lift otherwise can run into.
    // We still rely on presto-s3fs for progressive upload
    // that uploads a ~100 MB part whenever filled,
    // which achieves smoother outbound/upload network traffic.
    if (fileSize >= maxFileSize) {
      FlinkDataFile flinkDataFile = closeWriter(writer);
      emit(flinkDataFile);
      openPartitionFiles.remove(partitionPath);
    }
  }

  private String generateFileName() {
    return format.addExtension(
        String.format("%d_%d_%s", subtaskId, System.currentTimeMillis(), UUID.randomUUID().toString()));
  }

  private FileWriter newWriter(final Path path, final Partitioner<Row> part) throws Exception {
    FileAppender<Row> appender = newAppender(io.newOutputFile(path.toString()));
    FileWriter writer = FileWriter.builder()
        .withFileFormat(format)
        .withPath(path)
        .withProcessingTimeService(timerService)
        .withPartitioner(part.copy())
        .withAppender(appender)
        .withHadooopConfig(hadoopConf)
        .withSpec(spec)
        .withSchema(schema)
        .withTimestampField(timestampField)
        .withTimestampUnit(timestampUnit)
        .build();
    return writer;
  }


  private FileAppender<Row> newAppender(OutputFile file) throws Exception {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProps);
    try {
      switch (format) {
        case PARQUET:
          return Parquet.write(file)
              .setAll(tableProps)
              .metricsConfig(metricsConfig)
              .schema(schema)
              .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(
                  FlinkSchemaUtil.convert(schema.asStruct()), msgType))
              .overwrite()
              .build();

        case AVRO:
          return Avro.write(file)
              .createWriterFunc(DataWriter::create)
              .setAll(tableProps)
              .schema(schema)
              .overwrite()
              .build();

        default:
          throw new UnsupportedOperationException("Cannot write unknown format: " + format);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
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
