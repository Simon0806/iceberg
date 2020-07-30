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

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;

class IcebergValidator extends ConnectorDescriptorValidator {

  // values for connector. keys already defined in ConnectorDescriptorValidator
  public static final String CONNECTOR_TYPE_VALUE = "iceberg";
  public static final String CONNECTOR_VERSION_VALUE = "0.8.0";
  public static final int CONNECTOR_PROPERTY_VERSION_VALUE = 1;

  // Iceberg specific
  public static final String CONNECTOR_ICEBERG_PREFIX = CONNECTOR + "." + "iceberg";

  // for Hive catalog only
  public static final String CONNECTOR_ICEBERG_HIVE_METASTORE_URIS = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.HIVE_METASTORE_URIS;

  // table identifier, could be namespace.table for catalog or a path for HadoopTables
  public static final String CONNECTOR_ICEBERG_IDENTIFIER = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.IDENTIFIER;

  // partition, only used when creating a table
  public static final String CONNECTOR_ICEBERG_PARTITION = CONNECTOR_ICEBERG_PREFIX + "." + "partition";
  public static final String CONNECTOR_ICEBERG_PARTITION_COLUMN = "column";
  public static final String CONNECTOR_ICEBERG_PARTITION_TRANSFORM = "transform";

  // others
  // for IcebergWriter
  public static final String CONNECTOR_ICEBERG_WRITER_PARALLELISM = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.WRITER_PARALLELISM;
  public static final String CONNECTOR_ICEBERG_MAX_FILE_SIZE_BYTES = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.MAX_FILE_SIZE_BYTES;
  public static final String CONNECTOR_ICEBERG_SKIP_INCOMPATIBLE_RECORD = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.SKIP_INCOMPATIBLE_RECORD;

  // for IcebergCommitter
  public static final String CONNECTOR_ICEBERG_TEMP_MANIFEST_LOCATION = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.TEMP_MANIFEST_LOCATION;
  public static final String CONNECTOR_ICEBERG_SNAPSHOT_RETENTION_TIME = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.SNAPSHOT_RETENTION_TIME;

  // for both IcebergWriter and IcebergCommitter
  public static final String CONNECTOR_ICEBERG_FLUSH_COMMIT_INTERVAL = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL;

  // for source
  public static final String CONNECTOR_ICEBERG_FROM_SNAPSHOT_ID = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.FROM_SNAPSHOT_ID;
  public static final String CONNECTOR_ICEBERG_MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS;
  public static final String CONNECTOR_ICEBERG_AS_OF_TIME = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.AS_OF_TIME;
  public static final String CONNECTOR_ICEBERG_CASE_SENSITIVE = CONNECTOR_ICEBERG_PREFIX + "." +
      IcebergConnectorConstant.CASE_SENSITIVE;

  private static final IcebergValidator INSTANCE = new IcebergValidator();

  @Override
  public void validate(DescriptorProperties properties) {
    super.validate(properties);

    properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE, false);

    // Iceberg specific
    properties.validateString(CONNECTOR_ICEBERG_HIVE_METASTORE_URIS, true, 1);

    properties.validateString(CONNECTOR_ICEBERG_IDENTIFIER, false, 1);

    properties.validateInt(CONNECTOR_ICEBERG_WRITER_PARALLELISM, false, 0);
    properties.validateLong(CONNECTOR_ICEBERG_MAX_FILE_SIZE_BYTES, true, 1);
    properties.validateBoolean(CONNECTOR_ICEBERG_SKIP_INCOMPATIBLE_RECORD, true);

    properties.validateString(CONNECTOR_ICEBERG_TEMP_MANIFEST_LOCATION, true, 1);
    properties.validateLong(CONNECTOR_ICEBERG_SNAPSHOT_RETENTION_TIME, true, 0);

    properties.validateLong(CONNECTOR_ICEBERG_FLUSH_COMMIT_INTERVAL, true, 1);

    // table source properties
    properties.validateString(CONNECTOR_ICEBERG_FROM_SNAPSHOT_ID, true, 1);
    properties.validateLong(CONNECTOR_ICEBERG_MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS, true, 1000);
    properties.validateLong(CONNECTOR_ICEBERG_AS_OF_TIME, true, 0);
    properties.validateBoolean(CONNECTOR_ICEBERG_CASE_SENSITIVE, true);
  }

  public static IcebergValidator getInstance() {
    return INSTANCE;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Configuration getConfiguration(DescriptorProperties properties) {
    Configuration config = new Configuration();

    // hive metastore uris for HiveCatalog
    if (properties.containsKey(CONNECTOR_ICEBERG_HIVE_METASTORE_URIS)) {
      config.setString(IcebergConnectorConstant.HIVE_METASTORE_URIS,
          properties.getString(CONNECTOR_ICEBERG_HIVE_METASTORE_URIS));
    }

    // table identifier
    config.setString(IcebergConnectorConstant.IDENTIFIER, properties.getString(CONNECTOR_ICEBERG_IDENTIFIER));

    // partition spec, of use only when creating a table
    Map<String, String> columnMap = properties.getIndexedProperty(
        CONNECTOR_ICEBERG_PARTITION, CONNECTOR_ICEBERG_PARTITION_COLUMN);
    Map<String, String> transformMap = properties.getIndexedProperty(
        CONNECTOR_ICEBERG_PARTITION, CONNECTOR_ICEBERG_PARTITION_TRANSFORM);

    int size = columnMap.size();
    if (size != transformMap.size()) {
      throw new IllegalArgumentException(String.format("Partition column and transform must not have different sizes:" +
          " %s vs. %s", size, transformMap.size()));
    }

    if (size == 0) {  // un-partitioned
      config.setString(IcebergConnectorConstant.PARTITION_COLUMN,
          IcebergConnectorConstant.PARTITION_COLUMN_UNPARTITIONED);
      config.setString(IcebergConnectorConstant.PARTITION_TRANSFORM,
          IcebergConnectorConstant.PARTITION_COLUMN_UNPARTITIONED);
    } else {  // partitioned
      String[] columns = new String[size];
      String[] transforms = new String[size];

      // check if the index starting from 0 and increased by 1
      final Joiner dot = Joiner.on(".");
      for (int i = 0; i <= size - 1; i++) {
        // column
        columns[i] = columnMap.get(
            dot.join(CONNECTOR_ICEBERG_PARTITION, i, CONNECTOR_ICEBERG_PARTITION_COLUMN));
        if (columns[i] == null) {  // not found in the map
          throw new IllegalArgumentException("Index of partition column should start from 0 and be increased by 1");
        }

        // transform
        transforms[i] = transformMap.get(
            dot.join(CONNECTOR_ICEBERG_PARTITION, i, CONNECTOR_ICEBERG_PARTITION_TRANSFORM));
        if (transforms[i] == null) {  // not found in the map
          throw new IllegalArgumentException("Index of partition transform should start from 0 and be increased by 1");
        }
      }

      final Joiner comma = Joiner.on(",");
      config.setString(IcebergConnectorConstant.PARTITION_COLUMN, comma.join(columns));
      config.setString(IcebergConnectorConstant.PARTITION_TRANSFORM, comma.join(transforms));
    }

    config.setInteger(IcebergConnectorConstant.WRITER_PARALLELISM,
        properties.getInt(CONNECTOR_ICEBERG_WRITER_PARALLELISM));
    if (properties.containsKey(CONNECTOR_ICEBERG_MAX_FILE_SIZE_BYTES)) {
      config.setLong(IcebergConnectorConstant.MAX_FILE_SIZE_BYTES,
          properties.getLong(CONNECTOR_ICEBERG_MAX_FILE_SIZE_BYTES));
    }
    if (properties.containsKey(CONNECTOR_ICEBERG_SKIP_INCOMPATIBLE_RECORD)) {
      config.setBoolean(IcebergConnectorConstant.SKIP_INCOMPATIBLE_RECORD,
          properties.getBoolean(CONNECTOR_ICEBERG_SKIP_INCOMPATIBLE_RECORD));
    }

    if (properties.containsKey(CONNECTOR_ICEBERG_TEMP_MANIFEST_LOCATION)) {
      config.setString(IcebergConnectorConstant.TEMP_MANIFEST_LOCATION,
          properties.getString(CONNECTOR_ICEBERG_TEMP_MANIFEST_LOCATION));
    }
    if (properties.containsKey(CONNECTOR_ICEBERG_SNAPSHOT_RETENTION_TIME)) {
      config.setLong(IcebergConnectorConstant.SNAPSHOT_RETENTION_TIME,
          properties.getLong(CONNECTOR_ICEBERG_SNAPSHOT_RETENTION_TIME));
    }

    if (properties.containsKey(CONNECTOR_ICEBERG_FLUSH_COMMIT_INTERVAL)) {
      config.setLong(IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL,
          properties.getLong(CONNECTOR_ICEBERG_FLUSH_COMMIT_INTERVAL));
    }

    // table source property settings
    if (properties.containsKey(CONNECTOR_ICEBERG_FROM_SNAPSHOT_ID)) {
      config.setLong(IcebergConnectorConstant.FROM_SNAPSHOT_ID,
          properties.getLong(CONNECTOR_ICEBERG_FROM_SNAPSHOT_ID));
    }

    if (properties.containsKey(CONNECTOR_ICEBERG_MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS)) {
      config.setLong(IcebergConnectorConstant.MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS,
          properties.getLong(CONNECTOR_ICEBERG_MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS));
    }

    if (properties.containsKey(CONNECTOR_ICEBERG_AS_OF_TIME)) {
      config.setLong(IcebergConnectorConstant.AS_OF_TIME,
          properties.getLong(CONNECTOR_ICEBERG_AS_OF_TIME));
    }

    if (properties.containsKey(CONNECTOR_ICEBERG_CASE_SENSITIVE)) {
      config.setBoolean(IcebergConnectorConstant.CASE_SENSITIVE,
          properties.getBoolean(CONNECTOR_ICEBERG_CASE_SENSITIVE));
    }

    return config;
  }
}
