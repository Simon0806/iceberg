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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SlidingWindowReservoir;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.flink.connector.IcebergConnectorConstant;
import org.apache.iceberg.flink.connector.IcebergTableUtil;
import org.apache.iceberg.flink.connector.model.CommitMetadata;
import org.apache.iceberg.flink.connector.model.CommitMetadataUtil;
import org.apache.iceberg.flink.connector.model.FlinkManifestFile;
import org.apache.iceberg.flink.connector.model.FlinkManifestFileUtil;
import org.apache.iceberg.flink.connector.model.GenericFlinkManifestFile;
import org.apache.iceberg.flink.connector.model.ManifestFileState;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator commit data files to Iceberg table.
 * <p>
 * This operator should always run with parallelism of 1.
 * Because Iceberg lib perform optimistic concurrency control,
 * this can help reduce contention and retries
 * when committing files to Iceberg table.
 * <p>
 */
@SuppressWarnings("checkstyle:HiddenField")
public class IcebergCommitter extends RichSinkFunction<FlinkDataFile>
    implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitter.class);

  private static final String COMMIT_MANIFEST_HASHES_KEY = "flink.commit.manifest.hashes";
  private static final String WATERMARK_PROP_KEY_PREFIX = "flink.watermark";

  // For metrics
  private static final String METRICS_ICEBERG_COMMITTER_TPS = "tps";
  private static final String METRICS_ICEBERG_COMMITTER_COMMIT_LATENCY = "CommitLatency";
  private static final String METRICS_GROUP_ICEBERG_COMMITTER = "IcebergCommitter";

  private Configuration config;
  private boolean isCheckpointEnabled;
  private final String identifier;

  private final boolean watermarkEnabled;
  private final String watermarkPropKey;
  private final long snapshotRetention;
  private final String tempManifestLocation;
  private final PartitionSpec spec;
  private final FileIO io;
  private final String flinkJobId;
  private final long flushCommitInterval;

  private transient Table table;
  private transient List<FlinkDataFile> pendingDataFiles;
  private transient List<FlinkManifestFile> flinkManifestFiles;
  private transient ListState<ManifestFileState> manifestFileState;
  private transient CommitMetadata metadata;
  private transient ListState<CommitMetadata> commitMetadataState;

  private transient ProcessingTimeService processingTimeService;

  private transient DropwizardMeterWrapper tpsMeter;
  private transient DropwizardHistogramWrapper latencyHisto;

  public IcebergCommitter(Table table, Configuration config) {
    this.config = config;

    // current Iceberg sink implementation can't work with concurrent checkpoints.
    // We disable concurrent checkpoints by default as min pause is set to 60s by default.
    // Add an assertion to fail explicit in case job enables concurrent checkpoints.
    CheckpointConfig checkpointConfig = StreamExecutionEnvironment.getExecutionEnvironment().getCheckpointConfig();
    if (checkpointConfig.getMaxConcurrentCheckpoints() > 1) {
      throw new IllegalArgumentException("Iceberg sink doesn't support concurrent checkpoints");
    }

    identifier = config.getString(IcebergConnectorConstant.IDENTIFIER, "");

    // When watermark timestamp field inputted is
    // (1) A field name (i.e. neither null nor an empty string), watermarkEnabled = true.
    //     The logic extracts the named field.
    // (2) Null or an empty string, watermarkEnabled = false (i.e. disabled).
    //     The related logic does not get executed.
    // See WatermarkTimeExtractor for details.
    watermarkEnabled = !Strings.isNullOrEmpty(
        config.getString(IcebergConnectorConstant.WATERMARK_TIMESTAMP_FIELD, ""));
    // Watermark timestamp function is not fully tested, so disable it for now.
    // When watermarkEnabled is true (i.e. a field name is specified), throw an exception to exit.
    if (watermarkEnabled) {
      throw new IllegalArgumentException("Watermark timestamp function is disabled");
    }
    watermarkPropKey = WATERMARK_PROP_KEY_PREFIX;

    snapshotRetention = config.getLong(IcebergConnectorConstant.SNAPSHOT_RETENTION_TIME,
        IcebergConnectorConstant.INFINITE_SNAPSHOT_RETENTION_TIME /* default to infinite */);
    Preconditions.checkArgument(
        snapshotRetention >= 0 /* valid input */ ||
            snapshotRetention == IcebergConnectorConstant.INFINITE_SNAPSHOT_RETENTION_TIME /* default */,
        String.format("%s must be positive or 0, or %d as default, but is %d",
            IcebergConnectorConstant.SNAPSHOT_RETENTION_TIME, IcebergConnectorConstant.INFINITE_SNAPSHOT_RETENTION_TIME,
            snapshotRetention));
    if (snapshotRetention == IcebergConnectorConstant.INFINITE_SNAPSHOT_RETENTION_TIME) {  // default
      LOG.info("{} is not set, or explicitly set to the default value as {}, " +
              "so make it to be infinite (never drop any recovered manifest files)",
          IcebergConnectorConstant.SNAPSHOT_RETENTION_TIME, IcebergConnectorConstant.INFINITE_SNAPSHOT_RETENTION_TIME);
    }
    LOG.info("{} is set to {} ms", IcebergConnectorConstant.SNAPSHOT_RETENTION_TIME, snapshotRetention);

    tempManifestLocation = getTempManifestLocation(config, table.location());
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tempManifestLocation),
        String.format("%s must not be null or empty", IcebergConnectorConstant.TEMP_MANIFEST_LOCATION));
    LOG.info("{} is set to {}", IcebergConnectorConstant.TEMP_MANIFEST_LOCATION, tempManifestLocation);

    flushCommitInterval = config.getLong(IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL,
        IcebergConnectorConstant.DEFAULT_FLUSH_COMMIT_INTERVAL);
    Preconditions.checkArgument(flushCommitInterval > 0,
        String.format("%s must be positive, but is %d",
            IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval));
    LOG.info("{} is set to {} ms", IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval);

    // The only final fields yielded by table inputted
    spec = table.spec();
    io = table.io();

    final JobExecutionResult jobExecutionResult
        = ExecutionEnvironment.getExecutionEnvironment().getLastJobExecutionResult();
    if (jobExecutionResult != null) {
      flinkJobId = jobExecutionResult.getJobID().toString();
      LOG.info("Get Flink job ID from execution environment: {}", flinkJobId);
    } else {
      flinkJobId = new JobID().toString();
      LOG.info("Execution environment doesn't have executed job. Generate a random job ID : {}", flinkJobId);
    }
    LOG.info("Iceberg committer {} created with sink config", identifier);
    LOG.info("Iceberg committer {} loaded table partition spec: {}", identifier, spec);
  }

  @VisibleForTesting
  List<FlinkDataFile> getPendingDataFiles() {
    return pendingDataFiles;
  }

  @VisibleForTesting
  List<FlinkManifestFile> getFlinkManifestFiles() {
    return flinkManifestFiles;
  }

  @VisibleForTesting
  CommitMetadata getMetadata() {
    return metadata;
  }

  private String getTempManifestLocation(Configuration config, String baseLocation) {
    String defaultLocation =
        new Path(baseLocation, IcebergConnectorConstant.DEFAULT_TEMP_MANIFEST_FOLDER_NAME).toString();
    return config.getString(IcebergConnectorConstant.TEMP_MANIFEST_LOCATION, defaultLocation);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.isCheckpointEnabled = ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();
    this.processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
    final long currentTimestamp = processingTimeService.getCurrentProcessingTime();
    // If we don't enable checkpoint, we will use timeservice to do commit,
    if (!isCheckpointEnabled) {
      processingTimeService.registerTimer(currentTimestamp +
          flushCommitInterval, this);
      LOG.info("Checkpointing is disabled, but timer is registered with {} as {} ms",
          IcebergConnectorConstant.FLUSH_COMMIT_INTERVAL, flushCommitInterval);
    }

    LOG.info("{} is set to {} ms", IcebergConnectorConstant.SNAPSHOT_RETENTION_TIME, snapshotRetention);
    LOG.info("{} is set to {}", IcebergConnectorConstant.TEMP_MANIFEST_LOCATION, tempManifestLocation);

    this.tpsMeter = getRuntimeContext().getMetricGroup().addGroup(METRICS_GROUP_ICEBERG_COMMITTER)
        .meter(METRICS_ICEBERG_COMMITTER_TPS, new DropwizardMeterWrapper(new Meter()));
    // Set a histogram size to 50.
    Histogram dropwizardHistogram = new Histogram(new SlidingWindowReservoir(50));
    this.latencyHisto = getRuntimeContext().getMetricGroup().addGroup(METRICS_GROUP_ICEBERG_COMMITTER)
        .histogram(METRICS_ICEBERG_COMMITTER_COMMIT_LATENCY, new DropwizardHistogramWrapper(dropwizardHistogram));
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (processingTimeService != null && processingTimeService.isTerminated()) {
      processingTimeService.shutdownAndAwaitPending(flushCommitInterval, TimeUnit.MICROSECONDS);
    }
    LOG.info("Iceberg committer {} successfully close processing time service in close()", identifier);
    // When checkpoint enable, let checkpoint to do commit.
    if (!pendingDataFiles.isEmpty()) {
      flinkManifestFiles.add(createManifestFile(pendingDataFiles));
      pendingDataFiles.clear();
      commit();
    } else {
      LOG.info("Iceberg committer {} has nothing to commit in close()", identifier);
    }
  }

  void init() {
    table = IcebergTableUtil.findTable(config);

    pendingDataFiles = new ArrayList<>();
    flinkManifestFiles = new ArrayList<>();
    metadata = CommitMetadata.newBuilder()
        .setLastCheckpointId(0)
        .setLastCheckpointTimestamp(0)
        .setLastCommitTimestamp(System.currentTimeMillis())
        .build();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    init();

    Preconditions.checkState(manifestFileState == null,
        "checkpointedFilesState has already been initialized.");
    Preconditions.checkState(commitMetadataState == null,
        "commitMetadataState has already been initialized.");
    manifestFileState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
        "iceberg-committer-manifest-files-state", ManifestFileState.class));
    commitMetadataState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
        "iceberg-committer-metadata-state", CommitMetadata.class));

    if (context.isRestored()) {
      final Iterable<CommitMetadata> restoredMetadata = commitMetadataState.get();
      if (null != restoredMetadata) {
        LOG.info("Iceberg committer {} restoring metadata", identifier);
        List<CommitMetadata> metadataList = new ArrayList<>();
        for (CommitMetadata entry : restoredMetadata) {
          metadataList.add(entry);
        }
        Preconditions.checkState(1 == metadataList.size(),
            "metadata list size should be 1. got " + metadataList.size());
        metadata = metadataList.get(0);
        LOG.info("Iceberg committer {} restored metadata: {}", identifier, CommitMetadataUtil.encodeAsJson(metadata));
      } else {
        LOG.info("Iceberg committer {} has nothing to restore for metadata", identifier);
      }

      Iterable<ManifestFileState> restoredManifestFileStates = manifestFileState.get();
      if (null != restoredManifestFileStates) {
        LOG.info("Iceberg committer {} restoring manifest files", identifier);
        for (ManifestFileState manifestFileState : restoredManifestFileStates) {
          flinkManifestFiles.add(GenericFlinkManifestFile.fromState(manifestFileState));
        }
        LOG.info("Iceberg committer {} restored {} manifest files: {}",
            identifier, flinkManifestFiles.size(), flinkManifestFiles);
        final long now = System.currentTimeMillis();
        if (snapshotRetention != IcebergConnectorConstant.INFINITE_SNAPSHOT_RETENTION_TIME &&
            now - metadata.getLastCheckpointTimestamp() > snapshotRetention /* in milli-second */) {
          // manifest recovered is expired, so dropped
          flinkManifestFiles.clear();
          LOG.info("Iceberg committer {} cleared restored manifest files as checkpoint timestamp is too old: " +
                  "checkpointTimestamp = {}, now = {}, snapshotRetention = {}",
              identifier, metadata.getLastCheckpointTimestamp(), now, snapshotRetention);
        } else {  // manifest recovered is in retention
          flinkManifestFiles = removeCommittedManifests(flinkManifestFiles);
          if (flinkManifestFiles.isEmpty()) {
            LOG.info("Iceberg committer {} has zero uncommitted manifest files from restored state", identifier);
          } else {
            commitRestoredManifestFiles();
          }
        }
      } else {
        LOG.info("Iceberg committer {} has nothing to restore for manifest files", identifier);
      }
    }
  }

  @VisibleForTesting
  void commitRestoredManifestFiles() throws Exception {
    LOG.info("Iceberg committer {} committing last uncompleted transaction upon recovery: " +
            "metadata = {}, flink manifest files ({}) = {}", identifier,
        CommitMetadataUtil.encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
    commit();
    LOG.info("Iceberg committer {} committed last uncompleted transaction upon recovery: " +
            "metadata = {}, flink manifest files ({}) = {}", identifier,
        CommitMetadataUtil.encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
  }

  private List<FlinkManifestFile> removeCommittedManifests(List<FlinkManifestFile> flinkManifestFiles) {
    int snapshotCount = 0;
    String result = "succeeded";
    final long start = System.currentTimeMillis();
    try {
      final Set<String> manifestHashes = flinkManifestFiles.stream()
          .map(f -> f.hash())
          .collect(Collectors.toSet());
      final Set<String> committedHashes = new HashSet<>(flinkManifestFiles.size());
      final Iterable<Snapshot> snapshots = table.snapshots();
      for (Snapshot snapshot : snapshots) {
        ++snapshotCount;
        final Map<String, String> summary = snapshot.summary();
        final List<String> hashes = FlinkManifestFileUtil.hashesStringToList(summary.get(COMMIT_MANIFEST_HASHES_KEY));
        for (String hash : hashes) {
          if (manifestHashes.contains(hash)) {
            committedHashes.add(hash);
          }
        }
      }
      final List<FlinkManifestFile> uncommittedManifestFiles = flinkManifestFiles.stream()
          .filter(f -> !committedHashes.contains(f.hash()))
          .collect(Collectors.toList());
      return uncommittedManifestFiles;
    } catch (Throwable t) {
      result = "failed";
      LOG.error("Iceberg committer {} failed to check transaction completed. Throwable = {}", identifier, t);
      throw t;
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.info("Iceberg committer {} {} to check transaction completed" +
               " after iterating {} snapshots and {} milli-seconds",
          identifier, result, snapshotCount, duration);
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    LOG.info("Iceberg committer {} snapshot state: checkpointId = {}, triggerTime = {}",
        identifier, context.getCheckpointId(), context.getCheckpointTimestamp());
    Preconditions.checkState(manifestFileState != null,
        "manifest files state has not been properly initialized.");
    Preconditions.checkState(commitMetadataState != null,
        "metadata state has not been properly initialized.");

    // set transaction to null to indicate a start of a new checkpoint/commit/transaction
    synchronized (this) {
      snapshot(context, pendingDataFiles);
      checkpointState(flinkManifestFiles, metadata);
      postSnapshotSuccess();
    }
  }

  @VisibleForTesting
  void snapshot(FunctionSnapshotContext context, List<FlinkDataFile> pendingDataFiles) throws Exception {
    FlinkManifestFile flinkManifestFile = null;
    if (!pendingDataFiles.isEmpty()) {
      flinkManifestFile = createManifestFile(context, pendingDataFiles);
      flinkManifestFiles.add(flinkManifestFile);
    }
    metadata = updateMetadata(metadata, context, flinkManifestFile);
  }

  // For non-checkpoint case.
  private FlinkManifestFile createManifestFile(List<FlinkDataFile> pendingDataFiles)
      throws Exception {
    // Generate a default context in non-checkpoint mode.
    return createManifestFile(new StateSnapshotContextSynchronousImpl(0L,
            processingTimeService.getCurrentProcessingTime()), pendingDataFiles);
  }

  /* TODO we should use a unify datafile to track pending files.
  *   we should optimize logical to make commit out of checkpoint logica.
  *  */
  // For checkpoint case.
  private FlinkManifestFile createManifestFile(
      FunctionSnapshotContext context, List<FlinkDataFile> pendingDataFiles) throws Exception {
    Preconditions.checkArgument(pendingDataFiles != null && !pendingDataFiles.isEmpty(),
        "Need at least 1 pending data file (pendingDataFiles size >= 1) when calling createManifestFile()");
    LOG.info("Iceberg committer {} checkpointing {} pending data files", identifier, pendingDataFiles.size());
    String result = "succeeded";
    final long start = System.currentTimeMillis();
    long id = context.getCheckpointId();
    long timestamp = context.getCheckpointTimestamp();
    try {
      final String manifestFileName = Joiner.on("_")
          .join(flinkJobId, id, timestamp);
      // Iceberg requires file format suffix right now
      final String manifestFileNameWithSuffix = manifestFileName + ".avro";
      OutputFile outputFile = io.newOutputFile(new Path(tempManifestLocation, manifestFileNameWithSuffix).toString());
      ManifestWriter manifestWriter = ManifestFiles.write(spec, outputFile);

      // stats
      long recordCount = 0;
      long byteCount = 0;
      long lowWatermark = Long.MAX_VALUE;
      long highWatermark = Long.MIN_VALUE;
      for (FlinkDataFile flinkDataFile : pendingDataFiles) {
        DataFile dataFile = flinkDataFile.getIcebergDataFile();
        manifestWriter.add(dataFile);
        // update stats
        recordCount += dataFile.recordCount();
        byteCount += dataFile.fileSizeInBytes();
        if (flinkDataFile.getLowWatermark() < lowWatermark) {
          lowWatermark = flinkDataFile.getLowWatermark();
        }
        if (flinkDataFile.getHighWatermark() > highWatermark) {
          highWatermark = flinkDataFile.getHighWatermark();
        }
        LOG.debug("Data file with size of {} bytes added to manifest", dataFile.fileSizeInBytes());
      }
      manifestWriter.close();
      ManifestFile manifestFile = manifestWriter.toManifestFile();

      FlinkManifestFile flinkManifestFile = GenericFlinkManifestFile.builder()
          .setPath(manifestFile.path())
          .setLength(manifestFile.length())
          .setSpecId(manifestFile.partitionSpecId())
          .setCheckpointId(id)
          .setCheckpointTimestamp(timestamp)
          .setDataFileCount(pendingDataFiles.size())
          .setRecordCount(recordCount)
          .setByteCount(byteCount)
          .setLowWatermark(lowWatermark)
          .setHighWatermark(highWatermark)
          .build();

      // don't want to log a giant list at one line.
      // split the complete list into smaller chunks with 50 files.
      final AtomicInteger counter = new AtomicInteger(0);
      Collection<List<String>> listOfFileList = pendingDataFiles.stream()
          .map(flinkDataFile -> flinkDataFile.toCompactDump())
          .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / 50))
          .values();
      for (List<String> fileList : listOfFileList) {
        LOG.info("Iceberg committer {} created manifest file {} for {}/{} data files: {}",
            identifier, manifestFile.path(), fileList.size(), pendingDataFiles.size(), fileList);
      }
      return flinkManifestFile;
    } catch (Throwable t) {
      result = "failed";
      //LOG.error(String.format("Iceberg committer %s.%s failed to create manifest file for %d pending data files",
      //          database, tableName, pendingDataFiles.size()), t);
      LOG.error("Iceberg committer {} failed to create manifest file for {} pending data files. Throwable={}",
          identifier, pendingDataFiles.size(), t);
      throw t;
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.info("Iceberg committer {} {} to create manifest file with {} data files after {} milli-seconds",
          identifier, result, pendingDataFiles.size(), duration);
    }
  }

  /**
   * Extract watermark from old {@link CommitMetadata} and {@link FlinkManifestFile},
   * to build a new {@link CommitMetadata}.
   */
  private CommitMetadata updateMetadata(
      CommitMetadata oldMetadata,
      FunctionSnapshotContext context,
      @Nullable FlinkManifestFile flinkManifestFile) {
    LOG.info("Iceberg committer {} updating metadata {} with manifest file {}",
        identifier, CommitMetadataUtil.encodeAsJson(oldMetadata), flinkManifestFile);
    CommitMetadata.Builder metadataBuilder = CommitMetadata.newBuilder(oldMetadata)
        .setLastCheckpointId(context.getCheckpointId())
        .setLastCheckpointTimestamp(context.getCheckpointTimestamp());
    if (watermarkEnabled) {
      Long watermark = oldMetadata.getWatermark();
      if (flinkManifestFile == null) {
        // when there is no data to be committed
        // use elapsed wall clock time to move watermark forward.
        if (watermark != null) {
          final long elapsedTimeMs = System.currentTimeMillis() - oldMetadata.getLastCommitTimestamp();
          watermark += elapsedTimeMs;
        } else {
          watermark = System.currentTimeMillis();
        }
      } else {
        // use lowWatermark.
        if (flinkManifestFile.lowWatermark() == null) {
          throw new IllegalArgumentException("Watermark is enabled but lowWatermark is null");
        }
        // in case one container/slot is lagging behind,
        // we want to move watermark forward based on the slowest.
        final long newWatermark = flinkManifestFile.lowWatermark();
        // make sure watermark doesn't go back in time
        if (watermark == null || newWatermark > watermark) {
          watermark = newWatermark;
        }
      }
      metadataBuilder.setWatermark(watermark);
    }
    CommitMetadata newMetadata = metadataBuilder.build();
    LOG.info("Iceberg committer {} updated metadata {} with manifest file {}",
        identifier, CommitMetadataUtil.encodeAsJson(newMetadata), flinkManifestFile);
    return newMetadata;
  }

  private void checkpointState(List<FlinkManifestFile> flinkManifestFiles, CommitMetadata metadata) throws Exception {
    LOG.info("Iceberg committer {} checkpointing state", identifier);
    List<ManifestFileState> manifestFileStates = flinkManifestFiles.stream()
        .map(f -> f.toState())
        .collect(Collectors.toList());
    manifestFileState.clear();
    manifestFileState.addAll(manifestFileStates);
    commitMetadataState.clear();
    commitMetadataState.add(metadata);
    LOG.info("Iceberg committer {} checkpointed state: metadata = {}, flinkManifestFiles({}) = {}",
        identifier, CommitMetadataUtil.encodeAsJson(metadata),
        flinkManifestFiles.size(), flinkManifestFiles);
  }

  private void postSnapshotSuccess() {
    pendingDataFiles.clear();
    LOG.debug("Un-committed manifest file count {}, containing data file count {}, record count {} and byte count {}",
        flinkManifestFiles.size(),
        FlinkManifestFileUtil.getDataFileCount(flinkManifestFiles),
        FlinkManifestFileUtil.getRecordCount(flinkManifestFiles),
        FlinkManifestFileUtil.getByteCount(flinkManifestFiles)
    );
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    LOG.info("Iceberg committer {} checkpoint {} completed", identifier, checkpointId);
    synchronized (this) {
      if (checkpointId == metadata.getLastCheckpointId()) {
        try {
          commit();
        } catch (Exception t) {
          LOG.error("Iceberg committer {} failed to do post checkpoint commit. Throwable = ", identifier, t);
          throw t;  // Do NOT swallow exception but force job re-start in case of commit failure
        }
      } else {
        // TODO: it would be nice to fix this and allow concurrent checkpoint
        LOG.info("Iceberg committer {} skip committing transaction: " +
                "notify complete checkpoint id = {}, last manifest checkpoint id = {}",
            identifier, checkpointId, metadata.getLastCheckpointId());
      }
    }
  }

  @VisibleForTesting
  void commit() {
    if (!flinkManifestFiles.isEmpty() || watermarkEnabled) {
      final long start = System.currentTimeMillis();
      try {
        // prepare and commit transactions in two separate methods
        // so that we can measure latency separately.
        Transaction transaction = prepareTransaction(flinkManifestFiles, metadata);
        commitTransaction(transaction);
        final long duration = System.currentTimeMillis() - start;
        LOG.info("Iceberg committer {} succeeded to commit {} manifest files after {} milli-seconds",
            identifier, flinkManifestFiles.size(), TimeUnit.NANOSECONDS.toMillis(duration));
        latencyHisto.update(duration);

        LOG.info("Iceberg committer {} update metrics and metadata post commit success", identifier);
        LOG.debug("Committed manifest file count {}, containing data file count {}, record count {} and byte count {}",
            flinkManifestFiles.size(),
            FlinkManifestFileUtil.getDataFileCount(flinkManifestFiles),
            FlinkManifestFileUtil.getRecordCount(flinkManifestFiles),
            FlinkManifestFileUtil.getByteCount(flinkManifestFiles));
        final Long lowWatermark = FlinkManifestFileUtil.getLowWatermark(flinkManifestFiles);
        if (null != lowWatermark) {
          LOG.debug("Low watermark as {}", lowWatermark);
        }
        final Long highWatermark = FlinkManifestFileUtil.getHighWatermark(flinkManifestFiles);
        if (null != highWatermark) {
          LOG.debug("High watermark as {}", highWatermark);
        }
        if (metadata.getWatermark() != null) {
          LOG.debug("Watermark as {}", metadata.getWatermark());
        }
        metadata.setLastCommitTimestamp(System.currentTimeMillis());
        flinkManifestFiles.clear();
      } catch (Throwable t) {
        final long duration = System.currentTimeMillis() - start;
        LOG.error("Iceberg committer {} failed to commit {} manifest files after {} milli-seconds. Throwable = {}",
            identifier, flinkManifestFiles.size(), duration, t);
        throw t;
      }
    } else {
      LOG.info("Iceberg committer {} skip commit, as there are no uncommitted data files and watermark is disabled",
          identifier);
    }
  }

  private Transaction prepareTransaction(List<FlinkManifestFile> flinkManifestFiles, CommitMetadata metadata) {
    LOG.info("Iceberg committer {} start to prepare transaction: {}",
        identifier, CommitMetadataUtil.encodeAsJson(metadata));
    final long start = System.currentTimeMillis();
    try {
      Transaction transaction = table.newTransaction();
      if (!flinkManifestFiles.isEmpty()) {
        List<String> hashes = new ArrayList<>(flinkManifestFiles.size());
        AppendFiles appendFiles = transaction.newAppend();
        for (FlinkManifestFile flinkManifestFile : flinkManifestFiles) {
          appendFiles.appendManifest(flinkManifestFile);
          hashes.add(flinkManifestFile.hash());
        }

        appendFiles.set(COMMIT_MANIFEST_HASHES_KEY, FlinkManifestFileUtil.hashesListToString(hashes));
        appendFiles.commit();
        LOG.info("Iceberg committer {} appended {} manifest files to transaction",
            identifier, flinkManifestFiles.size());
      }
      if (watermarkEnabled) {
        UpdateProperties updateProperties = transaction.updateProperties();
        updateProperties.set(watermarkPropKey, Long.toString(metadata.getWatermark()));
        updateProperties.commit();
        LOG.info("Iceberg committer {} set watermark to {}", identifier, metadata.getWatermark());
      }
      return transaction;
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.debug("Transaction prepared in {} milli-seconds", duration);
    }
  }

  private void commitTransaction(Transaction transaction) {
    LOG.info("Iceberg committer {} start to commit transaction: {}",
        identifier, CommitMetadataUtil.encodeAsJson(metadata));
    final long start = System.currentTimeMillis();
    try {
      transaction.commitTransaction();
    } finally {
      final long duration = System.currentTimeMillis() - start;
      LOG.debug("Transaction committed in {} milli-seconds", duration);
    }
  }

  @Override
  public void invoke(FlinkDataFile value, Context context) throws Exception {
    pendingDataFiles.add(value);
    // TODO
    LOG.debug("Receive file count ?, containing record count {} and file size in byte {}",
        value.getIcebergDataFile().recordCount(),
        value.getIcebergDataFile().fileSizeInBytes());
    LOG.debug("Iceberg committer {} has total pending files {} after receiving new data file: {}",
        identifier, pendingDataFiles.size(), value.toCompactDump());
    tpsMeter.markEvent();
  }

  @Override
  public void onProcessingTime(long timestamp) throws Exception {
    long startProcess = System.currentTimeMillis();
    if (!pendingDataFiles.isEmpty()) {
      FlinkManifestFile flinkManifestFile = createManifestFile(pendingDataFiles);
      flinkManifestFiles.add(flinkManifestFile);
      pendingDataFiles.clear();
    }
    try {
      commit();
      final long currentTimestamp = processingTimeService.getCurrentProcessingTime();
      processingTimeService.registerTimer(currentTimestamp +
          flushCommitInterval, this);
      latencyHisto.update(System.currentTimeMillis() - startProcess);
    } catch (Exception t) {
      LOG.error("Iceberg committer {}.{} failed to do post checkpoint commit. Throwable = ",
          identifier, t);
      throw t;
    }
  }

}
