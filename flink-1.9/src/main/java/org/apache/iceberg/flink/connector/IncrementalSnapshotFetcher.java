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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;

class IncrementalSnapshotFetcher {
  private final Table table;
  private long lastConsumedSnapshotId;
  private final long asOfTime;
  private final boolean caseSensitive;

  IncrementalSnapshotFetcher(Table table, long lastConsumedSnapshotId, long asOfTime, boolean caseSensitive) {
    this.table = table;
    this.lastConsumedSnapshotId = lastConsumedSnapshotId;
    this.asOfTime = asOfTime;
    this.caseSensitive = caseSensitive;
  }

  CloseableIterable<CombinedScanTask> consumeNextSnap() {
    table.refresh();

    // Read the un-consumed snapshot id list, the list is from newest to oldest.
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    int index = snapshotIds.lastIndexOf(lastConsumedSnapshotId);
    if (index >= 0) {
      snapshotIds = snapshotIds.subList(0, index);
    }

    if (!snapshotIds.isEmpty()) {
      CloseableIterable<CombinedScanTask> scanTasks;
      long snapshotId = snapshotIds.get(snapshotIds.size() - 1);

      Snapshot snapshot = table.snapshot(snapshotId);
      if (DataOperations.OVERWRITE.equals(snapshot.operation())) {
        throw new UnsupportedOperationException("Don't support consuming OVERWRITE snapshot now.");
      }

      if (!DataOperations.APPEND.equals(snapshot.operation())) {
        return CloseableIterable.empty();
      }

      if (lastConsumedSnapshotId == IcebergConnectorConstant.DEFAULT_FROM_SNAPSHOT_ID) {
        // The IncrementalScan could only read the second snapshot's incremental data, so the first snapshot MUST use
        // the normal table scan.
        TableScan tableScan = table.newScan()
            .caseSensitive(caseSensitive)
            .useSnapshot(snapshotId);
        if (asOfTime != IcebergConnectorConstant.DEFAULT_AS_OF_TIME) {
          tableScan.asOfTime(asOfTime);
        }

        scanTasks = tableScan.planTasks();
      } else {
        TableScan tableScan = table.newScan()
            .caseSensitive(caseSensitive)
            .appendsBetween(lastConsumedSnapshotId, snapshotId);
        if (asOfTime != IcebergConnectorConstant.DEFAULT_AS_OF_TIME) {
          tableScan.asOfTime(asOfTime);
        }

        scanTasks = tableScan.planTasks();
      }

      // Update the last consumed snapshot id to the latest snapshot id.
      this.lastConsumedSnapshotId = snapshotId;
      return scanTasks;
    } else {
      return CloseableIterable.empty();
    }
  }

  long getLastConsumedSnapshotId() {
    return lastConsumedSnapshotId;
  }
}
