/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.compaction.level.hitter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.heavyhitter.QueryHitterManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HitterLevelCompactionTsFileManagement extends LevelCompactionTsFileManagement {

  private static final Logger logger = LoggerFactory
      .getLogger(HitterLevelCompactionTsFileManagement.class);
  private static int merge_time = 0;
  private final int sizeRatio = IoTDBDescriptor.getInstance().getConfig().getSizeRatio();
  private final int firstLevelNum = Math
      .max(IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel(), 1);
  private final int fullMergeRate =
      IoTDBDescriptor.getInstance().getConfig().getMergeWriteThroughputMbPerSec() -
          IoTDBDescriptor.getInstance().getConfig().getHitterMergeWriteThroughputMbPerSec();
  private final String MERGE_SUFFIX = ".temp";
  private boolean isFullMerging = false;

  public class FullMergeTask implements Runnable {

    private List<TsFileResource> mergeFileLst;
    private long timePartitionId;

    public FullMergeTask(List<TsFileResource> mergeFileLst, long timePartitionId) {
      this.mergeFileLst = mergeFileLst;
      this.timePartitionId = timePartitionId;
    }

    @Override
    public void run() {
      mergeFull(mergeFileLst, timePartitionId);
    }
  }

  public HitterLevelCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
  }

  @Override
  protected void merge(long timePartition) {
    long startTimeMillis = System.currentTimeMillis();
    merge1(forkedSequenceTsFileResources, timePartition);
    long time = System.currentTimeMillis();
    merge_time += time - startTimeMillis;
    logger.info("merge 总时长: {}", merge_time);
    if (enableUnseqCompaction && forkedUnSequenceTsFileResources.size() > 0) {
      merge(isForceFullMerge, getTsFileList(true), forkedUnSequenceTsFileResources.get(0),
          Long.MAX_VALUE);
    }
    if (conMerge) {
      this.forkCurrentFileList(timePartition);
      if (forkedSequenceTsFileResources.get(0).size() >= firstLevelNum) {
        merge(timePartition);
      }
    }
  }

  protected void merge1(List<List<TsFileResource>> mergeResources, long timePartition) {
    // wait until unseq merge has finished
    while (isUnseqMerging) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return;
      }
    }
    long startTimeMillis = System.currentTimeMillis();
    try {
      logger.info("{} start to filter compaction condition", storageGroupName);
      for (int i = 0; i < seqLevelNum - 1; i++) {
        if (mergeResources.get(i).size() >= firstLevelNum * Math.pow(sizeRatio, i)) {
          List<TsFileResource> toMergeTsFiles = mergeResources.get(i);
          logger.info("{} [Hitter Compaction] merge level-{}'s {} TsFiles to next level",
              storageGroupName, i, toMergeTsFiles.size());
          for (TsFileResource toMergeTsFile : toMergeTsFiles) {
            logger.info("{} [Hitter Compaction] start to merge TsFile {}", storageGroupName,
                toMergeTsFile);
          }

          // tmp file which contains all the hitter series
          File newLevelFile = createTempTsFileName(mergeResources.get(i).get(0).getTsFile());
          TsFileResource newResource = new TsFileResource(newLevelFile);
          // merge, read  heavy hitters time series from source files and write to target file
          List<PartialPath> unmergedPaths = QueryHitterManager.getInstance().getQueryHitter()
              .getTopCompactionSeries(new PartialPath(storageGroupName));
          long st = System.nanoTime();
          CompactionUtils
              .hitterMerge(newResource, toMergeTsFiles, storageGroupName, new HashSet<>(),
                  unmergedPaths);
          logger.info(
              "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to clean up",
              storageGroupName, i, toMergeTsFiles.size());
          // do the clean Up
          writeLockAllFiles(toMergeTsFiles);
          try {
            Set<Path> mergedPaths = new HashSet<>(unmergedPaths);
            List<TsFileIOWriter> writers = new ArrayList<>();
            for (TsFileResource fileResource : toMergeTsFiles) {
              // remove cache
              ChunkMetadataCache.getInstance().remove(fileResource);
              FileReaderManager.getInstance()
                  .closeFileAndRemoveReader(fileResource.getTsFilePath());

              TsFileIOWriter oldFileWriter = getOldFileWriter(fileResource);
              // filter all the chunks that have been merged
              oldFileWriter.filterChunksHitter(mergedPaths);
              writers.add(oldFileWriter);
            }
            TsFileIOWriter newFileWriter = getOldFileWriter(newResource);
            try (TsFileSequenceReader tmpFileReader =
                new TsFileSequenceReader(newFileWriter.getFile().getPath())) {
              Map<String, List<ChunkMetadata>> chunkMetadataListInChunkGroups =
                  newFileWriter.getDeviceChunkMetadataMap();
              for (Map.Entry<String, List<ChunkMetadata>> entry : chunkMetadataListInChunkGroups
                  .entrySet()) {
                String deviceId = entry.getKey();
                List<ChunkMetadata> chunkMetadataList = entry.getValue();
                writeMergedChunkGroup(chunkMetadataList, deviceId, tmpFileReader, writers.get(0));

                if (Thread.interrupted()) {
                  Thread.currentThread().interrupt();
                  writers.get(0).close();
                  restoreOldFile(newResource);
                  return;
                }
              }
            }
            Set<Long> historicalVersions = new HashSet<>();
            for (TsFileResource tsFileResource : toMergeTsFiles) {
              historicalVersions.addAll(tsFileResource.getHistoricalVersions());
            }
            toMergeTsFiles.get(0).setHistoricalVersions(historicalVersions);
            // update start end time
            for (Map.Entry<String, Integer> deviceIndexEntry : newResource
                .getDeviceToIndexMap().entrySet()) {
              toMergeTsFiles.get(0).updateStartTime(deviceIndexEntry.getKey(),
                  newResource.getStartTime(deviceIndexEntry.getKey()));
              toMergeTsFiles.get(0).updateEndTime(deviceIndexEntry.getKey(),
                  newResource.getEndTime(deviceIndexEntry.getKey()));
            }
            for (TsFileResource tsFileResource : toMergeTsFiles) {
              // rename file
              File oldFile = tsFileResource.getTsFile();
              File newFile = createNewTsFileName(oldFile, i + 1);
              FSFactoryProducer.getFSFactory().moveFile(oldFile, newFile);
              FSFactoryProducer.getFSFactory().moveFile(
                  FSFactoryProducer.getFSFactory()
                      .getFile(oldFile + TsFileResource.RESOURCE_SUFFIX),
                  FSFactoryProducer.getFSFactory()
                      .getFile(newFile + TsFileResource.RESOURCE_SUFFIX));
              tsFileResource.setFile(newFile);
              //
              tsFileResource.serialize();
              tsFileResource.close();
            }
            for (TsFileIOWriter writer : writers) {
              writer.endFile();
            }
            newFileWriter.close();
            newFileWriter.getFile().delete();
          } finally {
            writeUnlockAllFiles(toMergeTsFiles);
          }
//          System.exit(0);
          long en = System.nanoTime();
          long interval = en - st;
          System.out.println("合并总时间: " + interval);
          writeLock();
          try {
            synchronized (sequenceTsFileResources) {
              for (TsFileResource tsFileResource : toMergeTsFiles) {
                // remove and add
                sequenceTsFileResources.get(timePartition).get(i).remove(tsFileResource);
                sequenceTsFileResources.get(timePartition).get(i + 1).add(tsFileResource);
                if (mergeResources.size() > i + 1) {
//                  mergeResources.get(i).remove(tsFileResource);
                  mergeResources.get(i + 1).add(tsFileResource);
                }
              }
            }
          } finally {
            writeUnlock();
          }
        }
      }
      if (fullMergeRate > 0) {
        List<TsFileResource> fullMergeRes = new ArrayList<>(mergeResources.get(seqLevelNum - 1));
        FullMergeTask fullMergeTask = new FullMergeTask(fullMergeRes, timePartition);
        new Thread(fullMergeTask).start();
      }
    } catch (Exception e) {
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      // reset the merge working state to false
      logger.info("{} [Compaction] merge end time consumption: {} ms",
          storageGroupName, System.currentTimeMillis() - startTimeMillis);
    }
  }

  protected void merge(List<List<TsFileResource>> mergeResources, long timePartition) {
    // wait until unseq merge has finished
    while (isUnseqMerging) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return;
      }
    }
    long startTimeMillis = System.currentTimeMillis();
    try {
      logger.info("{} start to filter compaction condition", storageGroupName);
      for (int i = 0; i < seqLevelNum - 1; i++) {
        if (mergeResources.get(i).size() >= seqFileNumInEachLevel) {
          List<TsFileResource> toMergeTsFiles = mergeResources.get(i);
          logger.info("{} [Hitter Compaction] merge level-{}'s {} TsFiles to next level",
              storageGroupName, i, toMergeTsFiles.size());
          for (TsFileResource toMergeTsFile : toMergeTsFiles) {
            logger.info("{} [Hitter Compaction] start to merge TsFile {}", storageGroupName,
                toMergeTsFile);
          }

          // tmp file which contains all the hitter series
          File newLevelFile = createNewTsFileName(mergeResources.get(i).get(0).getTsFile(), i + 1);
          TsFileResource newResource = new TsFileResource(newLevelFile);
          // merge, read  heavy hitters time series from source files and write to target file
          List<PartialPath> unmergedPaths = QueryHitterManager.getInstance().getQueryHitter()
              .getTopCompactionSeries(new PartialPath(storageGroupName));
          long st = System.nanoTime();
          CompactionUtils
              .hitterMergeTest(newResource, toMergeTsFiles, storageGroupName, new HashSet<>(),
                  unmergedPaths);
          long en = System.nanoTime();
          long interval = en - st;
          System.out.println("合并总时间: " + interval);
          logger.info(
              "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to clean up",
              storageGroupName, i, toMergeTsFiles.size());
          writeLock();
          try {
            sequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
            deleteLevelFilesInList(timePartition, toMergeTsFiles, i, true);
            if (mergeResources.size() > i + 1) {
              mergeResources.get(i + 1).add(newResource);
            }
          } finally {
            writeUnlock();
          }
          deleteLevelFilesInDisk(toMergeTsFiles);
        }
      }
    } catch (Exception e) {
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      // reset the merge working state to false
      logger.info("{} [Compaction] merge end time consumption: {} ms",
          storageGroupName, System.currentTimeMillis() - startTimeMillis);
    }
  }

  protected File createTempTsFileName(File sourceFile) {
    String path = sourceFile.getAbsolutePath();
    return new File(path + MERGE_SUFFIX);
  }

  protected void writeLockAllFiles(List<TsFileResource> toMergeTsFiles) {
    int lockCnt;
    boolean[] locked = new boolean[toMergeTsFiles.size()];
    while (true) {
      lockCnt = 0;
      for (int i = 0; i < toMergeTsFiles.size(); i++) {
        locked[i] = toMergeTsFiles.get(i).tryWriteLock();
        if (locked[i]) {
          lockCnt++;
        }
      }
      if (lockCnt == toMergeTsFiles.size()) {
        break;
      } else {
        for (int i = 0; i < toMergeTsFiles.size(); i++) {
          if (locked[i]) {
            toMergeTsFiles.get(i).writeUnlock();
          }
        }
      }
    }
  }

  protected void writeUnlockAllFiles(List<TsFileResource> toMergeTsFiles) {
    for (int i = 0; i < toMergeTsFiles.size(); i++) {
      toMergeTsFiles.get(i).writeUnlock();
    }
  }

  /**
   * Open an appending writer for an old seq file so we can add new chunks to it.
   */
  private TsFileIOWriter getOldFileWriter(TsFileResource seqFile) throws IOException {
    TsFileIOWriter oldFileWriter;
    try {
      oldFileWriter = new ForceAppendTsFileWriter(seqFile.getTsFile());
      ((ForceAppendTsFileWriter) oldFileWriter).doTruncate();
    } catch (TsFileNotCompleteException e) {
      // this file may already be truncated if this merge is a system reboot merge
      oldFileWriter = new RestorableTsFileIOWriter(seqFile.getTsFile());
    }
    return oldFileWriter;
  }

  private void writeMergedChunkGroup(List<ChunkMetadata> chunkMetadataList, String device,
      TsFileSequenceReader reader, TsFileIOWriter fileWriter)
      throws IOException {
    fileWriter.startChunkGroup(device);
    long maxVersion = 0;
    for (ChunkMetadata chunkMetaData : chunkMetadataList) {
      Chunk chunk = reader.readMemChunk(chunkMetaData);
      fileWriter.writeChunk(chunk, chunkMetaData);
      maxVersion =
          chunkMetaData.getVersion() > maxVersion ? chunkMetaData.getVersion() : maxVersion;
    }
    fileWriter.writeVersion(maxVersion);
    fileWriter.endChunkGroup();
  }

  private void restoreOldFile(TsFileResource seqFile) throws IOException {
    RestorableTsFileIOWriter oldFileRecoverWriter = new RestorableTsFileIOWriter(
        seqFile.getTsFile());
    if (oldFileRecoverWriter.hasCrashed() && oldFileRecoverWriter.canWrite()) {
      oldFileRecoverWriter.endFile();
    } else {
      oldFileRecoverWriter.close();
    }
  }

  @Override
  public void forkCurrentFileList(long timePartition) {
    synchronized (sequenceTsFileResources) {
      forkSeqTsFileList(
          forkedSequenceTsFileResources,
          sequenceTsFileResources.computeIfAbsent(timePartition, this::newSequenceTsFileResources),
          seqLevelNum);
    }
    // we have to copy all unseq file
    synchronized (unSequenceTsFileResources) {
      forkUnSeqTsFileList(
          forkedUnSequenceTsFileResources,
          unSequenceTsFileResources
              .computeIfAbsent(timePartition, this::newUnSequenceTsFileResources),
          unseqLevelNum + 1);
    }
  }

  protected void forkSeqTsFileList(
      List<List<TsFileResource>> forkedTsFileResources,
      List rawTsFileResources, int currMaxLevel) {
    forkedTsFileResources.clear();
    for (int i = 0; i < currMaxLevel - 1; i++) {
      List<TsFileResource> forkedLevelTsFileResources = new ArrayList<>();
      Collection<TsFileResource> levelRawTsFileResources = (Collection<TsFileResource>) rawTsFileResources
          .get(i);
      for (TsFileResource tsFileResource : levelRawTsFileResources) {
        if (tsFileResource.isClosed()) {
          forkedLevelTsFileResources.add(tsFileResource);
          if (forkedLevelTsFileResources.size() >= firstLevelNum * Math.pow(sizeRatio, i)) {
            break;
          }
        }
      }
      forkedTsFileResources.add(forkedLevelTsFileResources);
    }
    // get max level merge file
    Collection<TsFileResource> levelRawTsFileResources = (Collection<TsFileResource>) rawTsFileResources
        .get(currMaxLevel - 1);
    List<TsFileResource> forkedLevelTsFileResources = new ArrayList<>();
    for (TsFileResource tsFileResource : levelRawTsFileResources) {
      if (tsFileResource.isClosed()) {
        forkedLevelTsFileResources.add(tsFileResource);
        if (forkedLevelTsFileResources.size() >= firstLevelNum * Math
            .pow(sizeRatio, currMaxLevel - 2)) {
          break;
        }
      }
    }
    forkedTsFileResources.add(forkedLevelTsFileResources);
  }

  protected void forkUnSeqTsFileList(
      List<List<TsFileResource>> forkedTsFileResources,
      List rawTsFileResources, int currMaxLevel) {
    forkedTsFileResources.clear();
    for (int i = 0; i < currMaxLevel - 1; i++) {
      List<TsFileResource> forkedLevelTsFileResources = new ArrayList<>();
      Collection<TsFileResource> levelRawTsFileResources = (Collection<TsFileResource>) rawTsFileResources
          .get(i);
      for (TsFileResource tsFileResource : levelRawTsFileResources) {
        if (tsFileResource.isClosed()) {
          forkedLevelTsFileResources.add(tsFileResource);
          if (forkedLevelTsFileResources.size() >= firstLevelNum * Math.pow(sizeRatio, i)) {
            break;
          }
        }
      }
      forkedTsFileResources.add(forkedLevelTsFileResources);
    }
  }

  private void mergeFull(List<TsFileResource> mergeFileLst, long timePartitionId) {
    if (isFullMerging) {
      return;
    }
    try {
      if (mergeFileLst.size() >= firstLevelNum * Math.pow(sizeRatio, seqLevelNum - 2)) {
        isFullMerging = true;
        CompactionLogger compactionLogger = new CompactionLogger(storageGroupDir,
            storageGroupName);
        File newLevelFile = createNewTsFileName(mergeFileLst.get(0).getTsFile(),
            seqLevelNum);
        TsFileResource newResource = new TsFileResource(newLevelFile);
        CompactionUtils
            .merge(newResource, mergeFileLst, storageGroupName, compactionLogger,
                new HashSet<>(), true);
        writeLock();
        try {
          sequenceTsFileResources.get(timePartitionId).get(seqLevelNum).add(newResource);
          deleteLevelFilesInList(timePartitionId, mergeFileLst, seqLevelNum - 1, true);
        } finally {
          writeUnlock();
        }
        deleteLevelFilesInDisk(mergeFileLst);
      }
    } catch (Exception e) {
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      isFullMerging = false;
    }
  }
}
