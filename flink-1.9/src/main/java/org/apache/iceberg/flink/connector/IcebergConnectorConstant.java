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

package org.apache.iceberg.flink.connector;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class IcebergConnectorConstant {
  private IcebergConnectorConstant() {}

  public static final String CATALOG_TYPE = "catalog-type";
  public static final String HIVE_CATALOG = "HIVE";
  public static final String HADOOP_CATALOG = "HADOOP";
  public static final String HADOOP_TABLES = "HADOOP_TABLES";
  public static final List<String> VALID_CATALOG_TYPE_OPTIONS = ImmutableList.of(
      HIVE_CATALOG, HADOOP_CATALOG, HADOOP_TABLES);
  public static final String CATALOG_TYPE_DEFAULT = HIVE_CATALOG;

  public static final String HIVE_METASTORE_URIS = "hive-metastore-uris";
  public static final String WAREHOUSE_LOCATION = "warehouse-location";

  public static final String IDENTIFIER = "identifier";

  // for table creation in table sink only
  public static final String PARTITION_COLUMN = "partition-column";
  public static final String PARTITION_COLUMN_UNPARTITIONED = "";

  public static final String PARTITION_TRANSFORM = "partition-transform";

  // transform options
  public static final String PARTITION_TRANSFORM_IDENTITY = "identity";
  public static final String PARTITION_TRANSFORM_BUCKET = "bucket";
  public static final String PARTITION_TRANSFORM_TRUNCATE = "truncate";
  public static final String PARTITION_TRANSFORM_YEAR = "year";
  public static final String PARTITION_TRANSFORM_MONTH = "month";
  public static final String PARTITION_TRANSFORM_DAY = "day";
  public static final String PARTITION_TRANSFORM_HOUR = "hour";
  public static final List<String> VALID_TRANSFORM_OPTIONS = ImmutableList.of(
      PARTITION_TRANSFORM_IDENTITY,
      PARTITION_TRANSFORM_BUCKET,
      PARTITION_TRANSFORM_TRUNCATE,
      PARTITION_TRANSFORM_YEAR,
      PARTITION_TRANSFORM_MONTH,
      PARTITION_TRANSFORM_DAY,
      PARTITION_TRANSFORM_HOUR);

  // Temporary location to store manifest files before doing the commit operation in Iceberg.
  // This is an INTERNAL configuration used in IcebergCommitter when taking snapshot.
  // The default is the "temp_manifest" folder under table's base location, at the same level as "data" and "metadata".
  // Users does NOT intend to set it explicitly but to use the default location.
  public static final String TEMP_MANIFEST_LOCATION = "temp-manifest-location";
  public static final String DEFAULT_TEMP_MANIFEST_FOLDER_NAME = "temp_manifest";

  // writer parallelism and its default value are for table sink only.
  // for data stream sink, use IcebergSinkAppender#withWriterParallelism() instead.
  public static final String WRITER_PARALLELISM = "writer-parallelism";
  public static final int DEFAULT_WRITER_PARALLELISM = 0;  // 0 is only a trigger to set the parallelism of writer
                                                           // to the parallelism of the upstream operator
                                                           // to which this sink is chained

  // watermark timestamp function, used for statistics
  // Disabled for now
  public static final String WATERMARK_TIMESTAMP_FIELD = "watermark-timestamp-field";
  public static final String WATERMARK_TIMESTAMP_UNIT = "watermark-timestamp-unit";
  public static final String DEFAULT_WATERMARK_TIMESTAMP_UNIT = TimeUnit.MILLISECONDS.name();

  // rotate the data file if its size is over the limit
  public static final String MAX_FILE_SIZE = "max-file-size";
  public static final long DEFAULT_MAX_FILE_SIZE = 1024L * 1024L * 1024L * 4;  // 4GB

  // when meeting incompatible record (have any exception when processing record), skip it or throw the exception
  public static final String SKIP_INCOMPATIBLE_RECORD = "skip-incompatible-record";
  public static final boolean DEFAULT_SKIP_INCOMPATIBLE_RECORD = false;

  // when restoring from snapshot, the un-committed manifest file (and all data files included) recovered from snapshot
  // will be dropped if its checkpoint timestamp exceeds the retention (in hour)
  public static final String SNAPSHOT_RETENTION_HOURS = "snapshot-retention-hours";
  public static final long DEFAULT_SNAPSHOT_RETENTION_HOURS = 0;  // infinite retention, never drop recovered manifest

  // when restoring from snapshot, for those un-committed manifest file (and all data files included)
  // recovered from snapshot and passes retention check, commit them to Iceberg or not
  public static final String COMMIT_RESTORED_MANIFEST_FILES = "commit-restored-manifest-files";
  public static final boolean DEFAULT_COMMIT_RESTORED_MANIFEST_FILES = true;

  // when checkpoint is not enabled, the interval in which data file flush and Iceberg commit are performed
  public static final String FLUSH_COMMIT_INTERVAL = "flush-commit-interval";
  public static final long DEFAULT_FLUSH_COMMIT_INTERVAL = 60 * 1000L;
}
