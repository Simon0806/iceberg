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

package org.apache.iceberg.spark.source;


import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mirco-batch based Spark Structured Streaming reader for Iceberg table. It will track the added
 * files and generate tasks per batch to process newly added files. By default it will process
 * all the newly added files to the current snapshot in each batch, user could also set this
 * configuration "max-files-per-trigger" to control the number of files processed per batch.
 */
class StreamingReader extends Reader implements MicroBatchReader {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingReader.class);
  private static final int DEFAULT_MAX_FILES_PER_BATCH = 1000;

  private StreamingOffset startOffset;
  private StreamingOffset endOffset;

  private final Table table;
  private final int maxFilesPerBatch;
  private final Long startSnapshotId;

  // Used to cache the pending tasks of this batch
  private CloseableIterable<IndexedTask> pendingTasks;

  /**
   * Utility class to track the snapshotId, task and index of task within this snapshot.
   */
  @VisibleForTesting
  static class IndexedTask {
    private final long snapshotId;
    private final FileScanTask task;
    private final int index;

    IndexedTask(long snapshotId, FileScanTask task, int index) {
      this.snapshotId = snapshotId;
      this.task = task;
      this.index = index;
    }

    long snapshotId() {
      return snapshotId;
    }

    FileScanTask task() {
      return task;
    }

    int index() {
      return index;
    }
  }

  StreamingReader(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
                  boolean caseSensitive, DataSourceOptions options) {
    super(table, io, encryptionManager, caseSensitive, options);

    this.table = table;
    this.maxFilesPerBatch =
       options.get("max-files-per-batch").map(Integer::parseInt).orElse(DEFAULT_MAX_FILES_PER_BATCH);
    Preconditions.checkArgument(maxFilesPerBatch > 0,
        "Option max-files-per-batch '%d' should > 0", maxFilesPerBatch);

    this.startSnapshotId = options.get("starting-snapshot-id").map(Long::parseLong).orElse(null);
    if (startSnapshotId != null) {
      if (!SnapshotUtil.ancestorOf(table, table.currentSnapshot().snapshotId(), startSnapshotId)) {
        throw new IllegalStateException("The option starting-snapshot-id " + startSnapshotId +
            " is not an ancestor of the current snapshot");
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
    table.refresh();

    if (start.isPresent() && !StreamingOffset.START_OFFSET.equals(start.get())) {
      this.startOffset = (StreamingOffset) start.get();
      this.endOffset = (StreamingOffset) end.orElse(calculateEndOffset(startOffset));
    } else {
      // If starting offset is "START_OFFSET" (there's no snapshot in the last batch), or starting
      // offset is not set, then we need to calculate the starting offset again.
      this.startOffset = calculateStartingOffset();
      this.endOffset = calculateEndOffset(startOffset);
    }
  }

  @Override
  public Offset getStartOffset() {
    if (startOffset == null) {
      throw new IllegalStateException("Start offset is not set");
    }

    return startOffset;
  }

  @Override
  public Offset getEndOffset() {
    if (endOffset == null) {
      throw new IllegalStateException("End offset is not set");
    }

    return endOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    return StreamingOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {
    // Since all the data and metadata of Iceberg is as it is, nothing needs to commit when
    // offset is processed, so no need to implement this method.
  }

  @Override
  public void stop() {
  }

  @Override
  @SuppressWarnings("unchecked")
  protected List<CombinedScanTask> tasks() {
    if (startOffset.equals(endOffset)) {
      LOG.info("Start offset {} equals to end offset {}, no data to process", startOffset, endOffset);
      return Collections.emptyList();
    }

    Preconditions.checkState(pendingTasks != null,
        "pendingTasks is null, which is unexpected as it will be set when calculating end offset");

    if (Iterables.isEmpty(pendingTasks)) {
      LOG.info("There's no task to process in this batch");
      return Collections.emptyList();
    }

    IndexedTask last = Iterables.getLast(pendingTasks);
    if (last.snapshotId() != endOffset.snapshotId() || last.index() != endOffset.index()) {
      throw new IllegalStateException("The cached pendingTasks doesn't match the current end offset " + endOffset);
    }

    LOG.info("Processing data from {} exclusive to {} inclusive", startOffset, endOffset);
    CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
        CloseableIterable.transform(pendingTasks, IndexedTask::task), splitSize());
    return Lists.newArrayList(
        TableScanUtil.planTasks(splitTasks, splitSize(), splitLookback(), splitOpenFileCost()));
  }

  private StreamingOffset calculateStartingOffset() {
    StreamingOffset startingOffset;
    if (startSnapshotId != null) {
      startingOffset = new StreamingOffset(startSnapshotId, -1, true);
    } else {
      List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
      if (snapshotIds.isEmpty()) {
        // there's no snapshot currently.
        startingOffset = StreamingOffset.START_OFFSET;
      } else {
        startingOffset = new StreamingOffset(snapshotIds.get(snapshotIds.size() - 1), -1, true);
      }
    }

    return startingOffset;
  }

  private StreamingOffset calculateEndOffset(StreamingOffset start) {
    if (start.equals(StreamingOffset.START_OFFSET)) {
      return StreamingOffset.START_OFFSET;
    }

    this.pendingTasks = getChangesWithRateLimit(start.snapshotId(), start.index(),
        start.isStartingSnapshotId(), maxFilesPerBatch);
    IndexedTask last = Iterables.isEmpty(pendingTasks) ? null : Iterables.getLast(pendingTasks);

    if (last == null) {
      return start;
    } else {
      boolean isStarting = last.snapshotId() == start.snapshotId() && start.isStartingSnapshotId();
      return new StreamingOffset(last.snapshotId(), last.index(), isStarting);
    }
  }

  @VisibleForTesting
  CloseableIterable<IndexedTask> getChangesWithRateLimit(long snapshotId, int index, boolean isStarting, int maxFiles) {
    List<CloseableIterable<IndexedTask>> snapshotTasks = Lists.newArrayList();
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    int currentFiles = 0;

    if (isStarting) {
      CloseableIterable<FileScanTask> iter = buildTableScan()
          .option("use-worker-pool", "false")
          .useSnapshot(snapshotId)
          .planFiles();
      CloseableIterable<IndexedTask> tasks = filterTasks(indexTasks(iter, snapshotId), snapshotId, index);
      Pair<Integer, CloseableIterable<IndexedTask>> limitedTasks = limitTasks(tasks, currentFiles, maxFiles);
      currentFiles = limitedTasks.getLeft();
      snapshotTasks.add(limitedTasks.getRight());

      if (currentFiles >= maxFiles) {
        // If the files to be processed already reach to max-files-batch, will not continue to plan files.
        return CloseableIterable.concat(snapshotTasks);
      }
    }

    Long start = isStarting ? Long.valueOf(snapshotId) : table.snapshot(snapshotId).parentId();
    Preconditions.checkState(start != null, "Start snapshot id should exist");
    ImmutableList<Long> snapshotIds = ImmutableList.<Long>builder()
        .addAll(SnapshotUtil.snapshotIdsBetween(table, start, currentSnapshotId))
        .add(start)
        .build()
        .reverse();

    for (int i = 0; i < snapshotIds.size() - 1; i++) {
      long fromSnapshotId = snapshotIds.get(i);
      long toSnapshotId = snapshotIds.get(i + 1);

      CloseableIterable<FileScanTask> iter = buildTableScan()
          .option("use-worker-pool", "false")
          .appendsBetween(fromSnapshotId, toSnapshotId)
          .planFiles();
      CloseableIterable<IndexedTask> tasks = filterTasks(indexTasks(iter, toSnapshotId), snapshotId, index);
      Pair<Integer, CloseableIterable<IndexedTask>> limitedTasks = limitTasks(tasks, currentFiles, maxFiles);
      currentFiles = limitedTasks.getLeft();
      snapshotTasks.add(limitedTasks.getRight());

      if (currentFiles >= maxFiles) {
        break;
      }
    }

    return CloseableIterable.concat(snapshotTasks);
  }

  private static CloseableIterable<IndexedTask> indexTasks(CloseableIterable<FileScanTask> iter, long snapshotId) {
    return CloseableIterable.transformWithIndex(iter, (idx, task) -> new IndexedTask(snapshotId, task, idx.intValue()));
  }

  private static CloseableIterable<IndexedTask> filterTasks(CloseableIterable<IndexedTask> iter,
                                                            long startSnapshotId, int index) {
    return CloseableIterable.filter(iter, t -> t.snapshotId() != startSnapshotId || t.index() > index);
  }

  private static Pair<Integer, CloseableIterable<IndexedTask>> limitTasks(CloseableIterable<IndexedTask> iter,
                                                                          int currentFiles, int maxFiles) {
    if (Iterables.isEmpty(iter)) {
      return Pair.of(currentFiles, CloseableIterable.empty());
    }

    IndexedTask last = Iterables.getLast(iter);
    int addedFiles = currentFiles + last.index() + 1;
    CloseableIterable<IndexedTask> limitedTasks;
    if (addedFiles >= maxFiles) {
      int leftFiles = maxFiles - currentFiles;
      limitedTasks = CloseableIterable.combine(Iterables.limit(iter, leftFiles), iter);
    } else {
      limitedTasks = iter;
    }

    return Pair.of(addedFiles, limitedTasks);
  }
}
