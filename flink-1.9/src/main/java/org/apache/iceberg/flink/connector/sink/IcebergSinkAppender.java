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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.flink.connector.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public DataStreamSink<FlinkDataFile> append(DataStream<IN> dataStream) {
    Preconditions.checkNotNull(serializer, "serializer can not be null");

    Table table = IcebergTableUtil.findTable(config);
    IcebergWriter<IN> writer = new IcebergWriter<>(table, serializer, config);
    IcebergCommitter committer = new IcebergCommitter(table, config);

    // append writer
    final String writerId = sinkName + "-writer";
    SingleOutputStreamOperator<FlinkDataFile> writerStream = dataStream
        .transform(writerId, TypeInformation.of(FlinkDataFile.class), writer)  // IcebergWriter as stream operator
        .uid(writerId);

    // set writer's parallelism
    int writerParallelism = config.getInteger(
        IcebergConnectorConstant.WRITER_PARALLELISM, IcebergConnectorConstant.DEFAULT_WRITER_PARALLELISM);
    if (writerParallelism == IcebergConnectorConstant.DEFAULT_WRITER_PARALLELISM ||
        writerParallelism < 0 /* invalid input */) {
      LOG.info("Iceberg writer parallelism not set explicitly or given an invalid input, " +
          "so default it to the parallelism of the upstream operator");
      writerParallelism = dataStream.getParallelism();
    }
    LOG.info("Set Iceberg writer parallelism to {}", writerParallelism);
    writerStream.setParallelism(writerParallelism);

    // append committer
    final String committerId = sinkName + "-committer";
    return writerStream
        .addSink(committer)  // IcebergCommitter as sink
        .name(committerId)
        .uid(committerId)
        .setParallelism(1);  // force parallelism of committer to 1
  }
}
