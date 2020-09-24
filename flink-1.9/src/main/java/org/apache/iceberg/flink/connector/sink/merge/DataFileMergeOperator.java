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

package org.apache.iceberg.flink.connector.sink.merge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ArrayListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.HashBasedTable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Table;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFileMergeOperator extends
    AbstractRewriteOperator<DataFileRewriteOperatorOut, DataFileMergeOperatorOut> {

  private static final Logger LOG = LoggerFactory.getLogger(DataFileMergeOperator.class);
  private Table<Long, List<DataFile>, Integer> addedFileCache;
  private Multimap<Long, DataFile> currentDataFileCache;

  private transient long tableSnapshotRetainMills;
  private transient int tableSnapshotRetainNums;

  public DataFileMergeOperator(Configuration config) {
    super(config);
    this.addedFileCache = HashBasedTable.create();
    this.currentDataFileCache = ArrayListMultimap.create();
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.tableSnapshotRetainMills = PropertyUtil.propertyAsInt(getTable().properties(),
        TableProperties.SNAPSHOT_RETAIN_LAST_MINUTES,
        TableProperties.SNAPSHOT_RETAIN_LAST_MINUTES_DEFAULT) * 60 * 1000L;
    this.tableSnapshotRetainNums = PropertyUtil.propertyAsInt(getTable().properties(),
        TableProperties.SNAPSHOT_RETAIN_LAST_NUMS, TableProperties.SNAPSHOT_RETAIN_LAST_NUMS_DEFAULT);
  }

  @Override
  public void processElement(StreamRecord<DataFileRewriteOperatorOut> element) throws Exception {
    if (element != null) {
      DataFileRewriteOperatorOut lastOpOut = element.getValue();
      long lastBatchMills = lastOpOut.getTaskTriggerMillis();
      if (!addedFileCache.rowKeySet().isEmpty()) {
        long maxMills = Collections.max(addedFileCache.rowKeySet());
        if (lastBatchMills > maxMills) {
          // skip the old merge file list and do the new one. it means the pre batch of merge
          // does not success because one task of current batch of merge has arrived. we only
          // need to do the latest batch.
          addedFileCache.clear();
          LOG.warn("Current merge window time is: {}", lastBatchMills);
        }
      }
      addedFileCache.put(lastBatchMills, lastOpOut.getAddedDataFiles(), lastOpOut.getSubTaskId());
      lastOpOut.getCurrentDataFiles().forEach(elem -> {
        currentDataFileCache.put(lastBatchMills, elem);
      });
      if (addedFileCache.row(lastBatchMills).size() == lastOpOut.getTaskNums()) {
        // all datafile list has arrived for current batch, so we can do merge.
        List<DataFile> addedDataFiles = addedFileCache.columnKeySet().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        List<DataFile> currentDataFiles = new ArrayList<>(currentDataFileCache.get(lastBatchMills));

        replaceDataFiles(currentDataFiles, addedDataFiles);
        addedFileCache.clear();
        currentDataFiles.clear();
        doExpireSnapshot();
      }
    }
  }

  private void replaceDataFiles(Iterable<DataFile> deletedDataFiles, Iterable<DataFile> addedDataFiles) {
    try {
      getTable().refresh();
      RewriteFiles rewriteFiles = getTable().newRewrite();
      rewriteFiles.rewriteFiles(Sets.newHashSet(deletedDataFiles), Sets.newHashSet(addedDataFiles));
      rewriteFiles.commit();
    } catch (Exception e) {
      FileIO fileIO = getTable().io();
      Tasks.foreach(Iterables.transform(addedDataFiles, f -> {
        assert f != null;
        return f.path().toString();
      }))
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(fileIO::deleteFile);
      throw e;
    }
  }

  private void doExpireSnapshot() {
    if (tableSnapshotRetainMills > 0) {
      getTable().refresh();
      getTable().expireSnapshots()
          .expireOlderThan(System.currentTimeMillis() - tableSnapshotRetainMills)
          .retainLast(tableSnapshotRetainNums)
          .commit();
    }
  }

}
