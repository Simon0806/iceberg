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

import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.connector.FlinkSchemaUtil;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.flink.connector.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

/**
 * Append Iceberg sink to DataStream
 *
 * @param <IN> input data type
 */
@SuppressWarnings({"checkstyle:ClassTypeParameterName", "checkstyle:HiddenField"})
public class IcebergSinkAppender<IN> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkAppender.class);

  private final Configuration config;
  private final String sinkName;
  private RecordSerializer<IN> serializer;

  public IcebergSinkAppender(Configuration config, RecordSerializer<IN> serializer, String sinkName) {
    this.config = config;
    this.sinkName = sinkName;
    this.serializer = serializer;
  }

  /**
   * @param dataStream append sink to this DataStream
   */
  @SuppressWarnings("unchecked")
  public DataStreamSink<DataFile> append(DataStream<IN> dataStream) {
    Preconditions.checkNotNull(serializer, "serializer can not be null");

    Table table = IcebergTableUtil.findTable(config);
    IcebergWriter<IN> writer = createIcebergWriter(table);
    IcebergCommitter committer = new IcebergCommitter(table, config);

    // Generate write stream by append operators.
    int writerParallelism = config.getInteger(
        IcebergConnectorConstant.WRITER_PARALLELISM, IcebergConnectorConstant.DEFAULT_WRITER_PARALLELISM);
    Preconditions.checkArgument(writerParallelism >= 0,
        String.format("%s must be positive, or %d as default, but is %d",
            IcebergConnectorConstant.WRITER_PARALLELISM, IcebergConnectorConstant.DEFAULT_WRITER_PARALLELISM,
            writerParallelism));
    if (writerParallelism == IcebergConnectorConstant.DEFAULT_WRITER_PARALLELISM) {  // default
      writerParallelism = dataStream.getParallelism();
      LOG.info("{} not set, or explicitly set to the default value as {}, " +
              "so make it to be the parallelism of the upstream operator as {}",
          IcebergConnectorConstant.WRITER_PARALLELISM, IcebergConnectorConstant.DEFAULT_WRITER_PARALLELISM,
          writerParallelism);
    }
    final String writerId = sinkName + "-writer";
    final String committerId = sinkName + "-committer";

    SingleOutputStreamOperator<DataFile> writerStream = dataStream
        .transform(writerId, TypeInformation.of(DataFile.class), writer)  // IcebergWriter as stream operator
        .setParallelism(writerParallelism)
        .uid(writerId)
        .transform(committerId, TypeInformation.of(DataFile.class), committer)
        .setParallelism(1)
        .uid(committerId);

    LOG.info("{} is finally set to {}", IcebergConnectorConstant.WRITER_PARALLELISM, writerParallelism);

    // we don't need a real sink because all data are handled in each operators
    // to make a complete jobgraph , we need to put a empty sink here.
    return writerStream.addSink(new EmptySink());
  }

  private IcebergWriter<IN> createIcebergWriter(Table table) {
    Preconditions.checkArgument(table != null, "Iceberg table should't be null");
    // TODO use flink table schema to read data.
    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
    Map<String, String> props = table.properties();
    long targetFileSize = config.getLong(IcebergConnectorConstant.MAX_FILE_SIZE_BYTES,
        IcebergConnectorConstant.DEFAULT_MAX_FILE_SIZE_BYTES);
    FileFormat fileFormat = FileFormat.valueOf(props.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
        .toUpperCase(Locale.ENGLISH));

    TaskWriterFactory<Row> taskWriterFactory = new Flink19RowWriterFactory(table.schema(), flinkSchema,
        table.spec(), table.locationProvider(), table.io(), table.encryption(), targetFileSize, fileFormat, props);

    return new IcebergWriter<>(table, serializer, config, taskWriterFactory);
  }

  private static class EmptySink implements SinkFunction {
  }
}
