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
 *
 */

package org.apache.iceberg.flink.connector.sink;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.flink.connector.data.FlinkOrcWriter;
import org.apache.iceberg.flink.connector.data.FlinkParquetWriters;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class Flink19RowWriterFactory implements TaskWriterFactory<Row> {
  private final Schema schema;
  private final RowType flinkSchema;
  private final PartitionSpec spec;
  private final LocationProvider locations;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final long targetFileSizeBytes;
  private final FileFormat format;
  private final FileAppenderFactory<Row> appenderFactory;

  private transient OutputFileFactory outputFileFactory;

  public Flink19RowWriterFactory(Schema schema,
                                 RowType flinkSchema,
                                 PartitionSpec spec,
                                 LocationProvider locations,
                                 FileIO io,
                                 EncryptionManager encryptionManager,
                                 long targetFileSizeBytes,
                                 FileFormat format,
                                 Map<String, String> tableProperties) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.spec = spec;
    this.locations = locations;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.format = format;
    this.appenderFactory = new Flink19AppendFactory(schema, flinkSchema, tableProperties);
  }

  @Override
  public void initialize(int taskId, int attemptId) {
    this.outputFileFactory = new OutputFileFactory(spec, format, locations, io, encryptionManager, taskId, attemptId);
  }

  @Override
  public TaskWriter<Row> create() {
    Preconditions.checkNotNull(outputFileFactory,
        "The outputFileFactory shouldn't be null if we have invoked the initialize().");

    if (spec.fields().isEmpty()) {
      return new UnpartitionedWriter<>(spec, format, appenderFactory, outputFileFactory, io, targetFileSizeBytes);
    } else {
      return new RowPartitionedFanoutWriter(spec, format, appenderFactory, outputFileFactory,
          io, targetFileSizeBytes, schema, flinkSchema);
    }
  }

  private static class RowPartitionedFanoutWriter extends PartitionedFanoutWriter<Row> {
    private final PartitionKey partitionKey;
    private final RowWrapper rowDataWrapper;

    RowPartitionedFanoutWriter(PartitionSpec spec,
                               FileFormat format,
                               FileAppenderFactory<Row> appenderFactory,
                               OutputFileFactory fileFactory,
                               FileIO io, long targetFileSize,
                               Schema schema, RowType flinkSchema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.partitionKey = new PartitionKey(spec, schema);
      this.rowDataWrapper = new RowWrapper(flinkSchema, schema.asStruct());
    }

    @Override
    protected PartitionKey partition(Row row) {
      partitionKey.partition(rowDataWrapper.wrap(row));
      return partitionKey;
    }
  }

  public class Flink19AppendFactory implements FileAppenderFactory<Row>, Serializable {

    private final Schema schema;
    private final RowType flinkSchema;
    private final Map<String, String> props;

    public Flink19AppendFactory(Schema schema,
                                RowType flinkSchema,
                                Map<String, String> props) {
      this.schema = schema;
      this.flinkSchema = flinkSchema;
      this.props = props;
    }

    @Override
    public FileAppender<Row> newAppender(OutputFile outputFile,
                                         FileFormat fileFormat) {
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(props);
      try {
        switch (fileFormat) {
          case PARQUET:
            return Parquet.write(outputFile)
                .setAll(props)
                .metricsConfig(metricsConfig)
                .schema(schema)
                .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(flinkSchema, msgType))
                .overwrite()
                .build();

          case AVRO:
            return Avro.write(outputFile)
                .createWriterFunc(DataWriter::create)
                .setAll(props)
                .schema(schema)
                .overwrite()
                .build();
          case ORC:
            return ORC.write(outputFile)
                .setAll(props)
                .schema(schema)
                .createWriterFunc((iSchema, typeDesc) -> FlinkOrcWriter.buildWriter(flinkSchema, iSchema))
                .overwrite()
                .build();

          default:
            throw new UnsupportedOperationException("Cannot write unknown format: " + format);
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }
}
