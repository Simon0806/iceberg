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

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.flink.connector.sink.FlinkTuple2Serializer;
import org.apache.iceberg.flink.connector.sink.IcebergSinkAppender;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalogs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableSink implements UpsertStreamTableSink<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableSink.class);

  private final boolean isAppendOnly;
  private final TableSchema tableSchema;
  private final Configuration config;

  public IcebergTableSink(boolean isAppendOnly, TableSchema tableSchema, Configuration config) {
    this.isAppendOnly = isAppendOnly;
    this.tableSchema = tableSchema;
    this.config = config;
  }

  @Override
  public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
    // empty implementation, due to being deprecated
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
    // catalog
    Object catalogOrHadoopTables = buildCatalog();
    Table table = null;

    if (catalogOrHadoopTables instanceof Catalog) {  // Hive or Hadoop catalog
      Catalog catalog = (Catalog) catalogOrHadoopTables;

      // table identifier
      String identifier = config.getString(IcebergConnectorConstant.IDENTIFIER, "");
      TableIdentifier tableIdentifier = TableIdentifier.parse(identifier);

      // load or create table from catalog
      if (catalog.tableExists(tableIdentifier)) {
        table = catalog.loadTable(tableIdentifier);

        LOG.info("Table of {} exists, so loaded from catalog", table);
      } else {  // table not exist
        // Disable table creation in Flink sink for now
        throw new IllegalArgumentException(String.format("Table %s does NOT exist", identifier));

        /*
        Schema icebergSchema = FlinkSchemaUtil.convert(tableSchema);
        PartitionSpec partitionSpec = buildPartitionSpec(icebergSchema);
        Map<String, String> properties = new HashMap<>();

        table = catalog.createTable(tableIdentifier, icebergSchema, partitionSpec, properties);

        LOG.info("Table of {} created with schema = {}\npartition spec {}", table, icebergSchema, partitionSpec);
        */
      }
    } else {  // HadoopTables
      HadoopTables hadoopTables = (HadoopTables) catalogOrHadoopTables;

      String location = config.getString(IcebergConnectorConstant.IDENTIFIER, "");
      table = hadoopTables.load(location);

      LOG.info("Table loaded by HadoopTables with location as {}", location);
    }

    // writer parallelism
    Integer writerParallelism = config.getInteger(
        IcebergConnectorConstant.WRITER_PARALLELISM, IcebergConnectorConstant.DEFAULT_WRITER_PARALLELISM);

    // append Iceberg sink to data stream
    IcebergSinkAppender<Tuple2<Boolean, Row>> appender =
        new IcebergSinkAppender<Tuple2<Boolean, Row>>(
            table, config, FlinkTuple2Serializer.getInstance(), "Iceberg_DataStream_sink_generated_by_TableSink_api")
        .withWriterParallelism(writerParallelism);

    return appender.append(dataStream);
  }

  @Override
  public TableSchema getTableSchema() {
    return this.tableSchema;
  }

  @Override
  public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    if (!Arrays.equals(tableSchema.getFieldNames(), fieldNames)) {
      String expectedFieldNames = Arrays.toString(tableSchema.getFieldNames());
      String actualFieldNames = Arrays.toString(fieldNames);
      throw new ValidationException("The field names are mismatched. Expected: " +
          expectedFieldNames + "But was: " + actualFieldNames);
    }
    if (!Arrays.equals(tableSchema.getFieldTypes(), fieldTypes)) {
      String expectedFieldTypes = Arrays.toString(tableSchema.getFieldTypes());
      String actualFieldTypes = Arrays.toString(fieldNames);
      throw new ValidationException("Field types are mismatched. Expected: " +
          expectedFieldTypes + " But was: " + actualFieldTypes);
    }
    return this;
  }

  @Override
  public void setKeyFields(String[] keys) {
    // do nothing here.
  }

  @Override
  public void setIsAppendOnly(Boolean isAppendOnly) {
    if (this.isAppendOnly && !isAppendOnly) {
      throw new ValidationException("The given query is not supported by this sink because the sink is configured " +
          "to operate in append mode only. Thus, it only support insertions (no queries with updating results).");
    }
  }

  @Override
  public TypeInformation<Row> getRecordType() {
    return this.tableSchema.toRowType();
  }

  @Override
  public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
    return org.apache.flink.api.common.typeinfo.Types.TUPLE(
        org.apache.flink.api.common.typeinfo.Types.BOOLEAN,
        getRecordType());
  }

  private Object buildCatalog() {
    String catalogType = config.getString(IcebergConnectorConstant.CATALOG_TYPE, IcebergConnectorConstant.HIVE_CATALOG);

    switch (catalogType.toUpperCase()) {
      case IcebergConnectorConstant.HIVE_CATALOG:
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set(HiveConf.ConfVars.METASTOREURIS.varname,
            config.getString(IcebergConnectorConstant.HIVE_METASTORE_URIS, ""));
        /* hive metastore warehouse location is not a must when table creation by Hive catalog is not supported
        hadoopConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
            config.getString(IcebergConnectorConstant.WAREHOUSE_LOCATION, ""));
        */
        return HiveCatalogs.loadCatalog(hadoopConf);

      case IcebergConnectorConstant.HADOOP_CATALOG:
        return new HadoopCatalog(
            new org.apache.hadoop.conf.Configuration(),
            config.getString(IcebergConnectorConstant.WAREHOUSE_LOCATION, ""));

      case IcebergConnectorConstant.HADOOP_TABLES:
        return new HadoopTables();

      default:
        throw new IllegalArgumentException("Unknown catalog type: " + catalogType);
    }
  }

  private PartitionSpec buildPartitionSpec(Schema schema) {
    String columnsAsString = config.getString(IcebergConnectorConstant.PARTITION_COLUMN,
        IcebergConnectorConstant.PARTITION_COLUMN_UNPARTITIONED /* fake default */);
    if (IcebergConnectorConstant.PARTITION_COLUMN_UNPARTITIONED.equals(columnsAsString)) {  // un-partitioned
      return PartitionSpec.unpartitioned();
    } else {  // partitioned
      final Pattern hasNumInTransform = Pattern.compile("(\\w+)\\[(\\d+)\\]");

      String[] columns = columnsAsString.split(",");
      String[] transforms =
          config.getString(IcebergConnectorConstant.PARTITION_TRANSFORM,
              IcebergConnectorConstant.PARTITION_COLUMN_UNPARTITIONED /* fake default */)
          .split(",");

      PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

      for (int i = 0; i <= columns.length - 1; i++) {  // transforms should be of the same length
        String column = columns[i];
        String transform = transforms[i].toLowerCase();

        Matcher numMatcher = hasNumInTransform.matcher(transform);
        if (numMatcher.matches()) {  // bucket[n] or truncate[n]
          String parsedTransform = numMatcher.group(1);
          int parsedNum = Integer.parseInt(numMatcher.group(2));

          switch (parsedTransform) {
            case IcebergConnectorConstant.PARTITION_TRANSFORM_BUCKET:
              builder.bucket(column, parsedNum /* as numBuckets */);
              break;

            case IcebergConnectorConstant.PARTITION_TRANSFORM_TRUNCATE:
              builder.truncate(column, parsedNum /* as width */);
              break;

            default:
              throw new UnsupportedOperationException("Unsupported partition transform: " + transform);
          }
        } else {  // other transforms
          switch (transform) {
            case IcebergConnectorConstant.PARTITION_TRANSFORM_IDENTITY:
              builder.identity(column);
              break;

            case IcebergConnectorConstant.PARTITION_TRANSFORM_YEAR:
              builder.year(column);
              break;

            case IcebergConnectorConstant.PARTITION_TRANSFORM_MONTH:
              builder.month(column);
              break;

            case IcebergConnectorConstant.PARTITION_TRANSFORM_DAY:
              builder.day(column);
              break;

            case IcebergConnectorConstant.PARTITION_TRANSFORM_HOUR:
              builder.hour(column);
              break;

            default:
              throw new UnsupportedOperationException("Unsupported partition transform: " + transform);
          }
        }
      }  // every column/transform

      return builder.build();
    }  // partitioned
  }
}
