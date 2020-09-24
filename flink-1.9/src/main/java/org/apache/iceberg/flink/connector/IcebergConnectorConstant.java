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
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class IcebergConnectorConstant {
  private IcebergConnectorConstant() {
  }

  // for HiveCatalog
  public static final String HIVE_METASTORE_URIS = "hive-metastore-uris";

  // table identifier
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

  // Window end time align time 2020-01-01 00:00:00. This is use to calculate a
  // window end time for a timestamp, to avoid different result because of timezone,
  // we should use a align time.
  public static final long ALIGN_TIME = 1577808000000L;

  // writer parallelism
  // the default value 0 is only a trigger to set the parallelism of writer to the parallelism of the upstream operator
  // to which this sink is chained
  public static final String WRITER_PARALLELISM = "writer-parallelism";
  public static final int DEFAULT_WRITER_PARALLELISM = 0;

  // rotate the data file if its size is over this limit (in byte)
  public static final String MAX_FILE_SIZE_BYTES = "max-file-size-bytes";
  public static final long DEFAULT_MAX_FILE_SIZE_BYTES = TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

  // when meeting incompatible record (have any exception when processing record), skip it or throw the exception
  public static final String SKIP_INCOMPATIBLE_RECORD = "skip-incompatible-record";
  public static final boolean DEFAULT_SKIP_INCOMPATIBLE_RECORD = false;

  // Temporary location to store manifest files before doing the commit operation in Iceberg.
  // This is an INTERNAL configuration used in IcebergCommitter when taking snapshot.
  // The default is the "temp_manifest" folder under table's base location, at the same level as "data" and "metadata".
  // Users does NOT intend to set it explicitly but to use the default location.
  public static final String TEMP_MANIFEST_LOCATION = "temp-manifest-location";
  public static final String DEFAULT_TEMP_MANIFEST_FOLDER_NAME = "temp_manifest";

  /**
   * When restoring from snapshot, the un-committed manifest file (and all data files included) recovered from snapshot
   * will be dropped if its checkpoint timestamp exceeds this retention time (in milli-second).
   */
  public static final String SNAPSHOT_RETENTION_TIME = "snapshot-retention-time";
  public static final long INFINITE_SNAPSHOT_RETENTION_TIME = 0;  // infinite retention, never drop recovered manifest

  // when checkpoint is not enabled, the interval in which data file flush and Iceberg commit are performed
  public static final String FLUSH_COMMIT_INTERVAL = "flush-commit-interval";
  public static final long DEFAULT_FLUSH_COMMIT_INTERVAL = 60 * 1000L;

  // In streaming job, the commit operation is always continuously, each commit will generate
  // a new snapshot, when the amount of snapshot reaches to this parameter, then trigger merge operation
  public static final String NEW_SNAPSHOT_NUMS_TO_MERGE = "new-snapshot-nums-to-merge";
  public static final int DEFAULT_NEW_SNAPSHOT_NUMS_TO_MERGE = 10;

  public static final String NUMS_OF_DATAFILE_TO_MERGE = "nums-of-datafile-to-merge";
  public static final int DEFAULT_NUMS_OF_DATAFILE_TO_MERGE = 100;

  // Datafile should auto flush or not, this just use for test.
  public static final String WRITER_AUTO_ROLLING = "writer-auto-rolling";
  public static final boolean DEFAULT_WRITER_AUTO_ROLLING = false;

  // watermark timestamp function, used for statistics
  // disabled for now
  public static final String WATERMARK_TIMESTAMP_FIELD = "watermark-timestamp-field";
  public static final String WATERMARK_TIMESTAMP_UNIT = "watermark-timestamp-unit";
  public static final String DEFAULT_WATERMARK_TIMESTAMP_UNIT = TimeUnit.MILLISECONDS.name();

  // for Flink table source
  public static final String FROM_SNAPSHOT_ID = "from-snapshot-id";
  public static final long DEFAULT_FROM_SNAPSHOT_ID = -1;
  public static final String MIN_SNAPSHOT_POLLING_INTERVAL_MILLIS = "min-snapshot-polling-milliseconds";
  public static final long DEFAULT_MIN_SNAPSHOT_POLLING_INTERVAL_MILLIS = 100;
  public static final String MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS = "max-snapshot-polling-milliseconds";
  public static final long DEFAULT_MAX_SNAPSHOT_POLLING_INTERVAL_MILLIS = 1000;
  public static final String CASE_SENSITIVE = "case-sensitive";
  public static final boolean DEFAULT_CASE_SENSITIVE = true;
  public static final String AS_OF_TIME = "as-of-time";
  public static final long DEFAULT_AS_OF_TIME = -1;
}
