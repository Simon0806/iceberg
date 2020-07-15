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

import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating configured instances of {@link IcebergTableSink}.
 */
public class IcebergTableFactory implements
    StreamTableSinkFactory<Tuple2<Boolean, Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableFactory.class);

  @Override
  public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
    DescriptorProperties descProperties = getValidatedProperties(properties);

    // Create the IcebergTableSink instance.
    boolean isAppendOnly = descProperties
        .isValue(StreamTableDescriptorValidator.UPDATE_MODE, StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND);
    TableSchema tableSchema = descProperties.getTableSchema(Schema.SCHEMA);
    Configuration config = IcebergValidator.getInstance().getConfiguration(descProperties);

    return new IcebergTableSink(isAppendOnly, tableSchema, config);
  }

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = Maps.newHashMap();
    context.put(ConnectorDescriptorValidator.CONNECTOR_TYPE, IcebergValidator.CONNECTOR_TYPE_VALUE);
    context.put(ConnectorDescriptorValidator.CONNECTOR_VERSION, IcebergValidator.CONNECTOR_VERSION_VALUE);
    context.put(ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION, "1");
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> properties = Lists.newArrayList();

    // update mode
    properties.add(StreamTableDescriptorValidator.UPDATE_MODE);

    // Flink schema properties
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_TYPE);
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_NAME);
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_FROM);

    // Iceberg specific
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_HIVE_METASTORE_URIS);

    properties.add(IcebergValidator.CONNECTOR_ICEBERG_IDENTIFIER);

    // Disable table creation in Flink sink for now
    /*
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_PARTITION + ".#." +
        IcebergValidator.CONNECTOR_ICEBERG_PARTITION_COLUMN);
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_PARTITION + ".#." +
        IcebergValidator.CONNECTOR_ICEBERG_PARTITION_TRANSFORM);
    */

    properties.add(IcebergValidator.CONNECTOR_ICEBERG_WRITER_PARALLELISM);
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_MAX_FILE_SIZE_BYTES);
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_SKIP_INCOMPATIBLE_RECORD);

    properties.add(IcebergValidator.CONNECTOR_ICEBERG_TEMP_MANIFEST_LOCATION);
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_SNAPSHOT_RETENTION_TIME);

    properties.add(IcebergValidator.CONNECTOR_ICEBERG_FLUSH_COMMIT_INTERVAL);

    return properties;
  }

  private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
    DescriptorProperties descProperties = new DescriptorProperties(true);
    descProperties.putProperties(properties);
    // Validate the properties values.
    IcebergValidator.getInstance().validate(descProperties);
    return descProperties;
  }
}
