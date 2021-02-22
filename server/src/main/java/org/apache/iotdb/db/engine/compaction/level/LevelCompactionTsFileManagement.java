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

package org.apache.iotdb.db.engine.compaction.level;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TsFileManagement for LEVEL_COMPACTION, use level struct to manage TsFile list
 */
public class LevelCompactionTsFileManagement extends TsFileManagement {

  public static final String MERGE_SUFFIX = ".merge";

  private static final Logger logger = LoggerFactory
      .getLogger(LevelCompactionTsFileManagement.class);

  private final int seqFileNumInEachLevel = Math
      .max(IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel(), 1);
  private final int unseqLevelNum = Math
      .max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);
  private final int unseqFileNumInEachLevel = Math
      .max(IoTDBDescriptor.getInstance().getConfig().getUnseqFileNumInEachLevel(), 1);

  private final boolean enableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
      .isEnableUnseqCompaction();
  private final boolean isForceFullMerge = IoTDBDescriptor.getInstance().getConfig()
      .isForceFullMerge();

  public LevelCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
    clear();
  }

  private void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
    logger.debug("{} [compaction] merge starts to delete real file", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteLevelFile(mergeTsFile);
      logger
          .info("{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  private void deleteLevelFilesInList(long timePartitionId,
      Collection<TsFileResource> mergeTsFiles, int level, boolean sequence) {
    logger.debug("{} [compaction] merge starts to delete file list", storageGroupName);
    if (sequence) {
      if (sequenceTsFileResources.containsKey(timePartitionId)) {
        if (sequenceTsFileResources.get(timePartitionId).size() > level) {
          synchronized (sequenceTsFileResources) {
            sequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
          }
        }
      }
    } else {
      if (unSequenceTsFileResources.containsKey(timePartitionId)) {
        if (unSequenceTsFileResources.get(timePartitionId).size() > level) {
          synchronized (unSequenceTsFileResources) {
            unSequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
          }
        }
      }
    }
  }

  private void deleteLevelFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  @Override
  public List<TsFileResource> getTsFileList(boolean sequence) {
    List<TsFileResource> result = new ArrayList<>();
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        for (List<SortedSet<TsFileResource>> sequenceTsFileList : sequenceTsFileResources
            .values()) {
          for (int i = sequenceTsFileList.size() - 1; i >= 0; i--) {
            result.addAll(sequenceTsFileList.get(i));
          }
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        for (List<List<TsFileResource>> unSequenceTsFileList : unSequenceTsFileResources.values()) {
          for (int i = unSequenceTsFileList.size() - 1; i >= 0; i--) {
            result.addAll(unSequenceTsFileList.get(i));
          }
        }
      }
    }
    return result;
  }

  @Override
  public Iterator<TsFileResource> getIterator(boolean sequence) {
    return getTsFileList(sequence).iterator();
  }

  @Override
  public void remove(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        for (SortedSet<TsFileResource> sequenceTsFileResource : sequenceTsFileResources
            .get(tsFileResource.getTimePartition())) {
          sequenceTsFileResource.remove(tsFileResource);
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        for (List<TsFileResource> unSequenceTsFileResource : unSequenceTsFileResources
            .get(tsFileResource.getTimePartition())) {
          unSequenceTsFileResource.remove(tsFileResource);
        }
      }
    }
  }

  @Override
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource : sequenceTsFileResources
            .values()) {
          for (SortedSet<TsFileResource> levelTsFileResource : partitionSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource : unSequenceTsFileResources
            .values()) {
          for (List<TsFileResource> levelTsFileResource : partitionUnSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      }
    }
  }

  @Override
  public void add(TsFileResource tsFileResource, boolean sequence) {
    long timePartitionId = tsFileResource.getTimePartition();
    int level = getMergeLevel(tsFileResource.getTsFile());
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        if (level <= seqLevelNum - 1) {
          // current file has normal level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources).get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(seqLevelNum - 1)
              .add(tsFileResource);
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        if (level <= unseqLevelNum - 1) {
          // current file has normal level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources).get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(unseqLevelNum - 1).add(tsFileResource);
        }
      }
    }
  }

  @Override
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      add(tsFileResource, sequence);
    }
  }

  @Override
  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      for (SortedSet<TsFileResource> sequenceTsFileResource : sequenceTsFileResources
          .computeIfAbsent(tsFileResource.getTimePartition(), this::newSequenceTsFileResources)) {
        if (sequenceTsFileResource.contains(tsFileResource)) {
          return true;
        }
      }
    } else {
      for (List<TsFileResource> unSequenceTsFileResource : unSequenceTsFileResources
          .computeIfAbsent(tsFileResource.getTimePartition(), this::newUnSequenceTsFileResources)) {
        if (unSequenceTsFileResource.contains(tsFileResource)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void clear() {
    sequenceTsFileResources.clear();
    unSequenceTsFileResources.clear();
  }

  @Override
  @SuppressWarnings("squid:S3776")
  public boolean isEmpty(boolean sequence) {
    if (sequence) {
      for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource : sequenceTsFileResources
          .values()) {
        for (SortedSet<TsFileResource> sequenceTsFileResource : partitionSequenceTsFileResource) {
          if (!sequenceTsFileResource.isEmpty()) {
            return false;
          }
        }
      }
    } else {
      for (List<List<TsFileResource>> partitionUnSequenceTsFileResource : unSequenceTsFileResources
          .values()) {
        for (List<TsFileResource> unSequenceTsFileResource : partitionUnSequenceTsFileResource) {
          if (!unSequenceTsFileResource.isEmpty()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public int size(boolean sequence) {
    int result = 0;
    if (sequence) {
      for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource : sequenceTsFileResources
          .values()) {
        for (int i = seqLevelNum - 1; i >= 0; i--) {
          result += partitionSequenceTsFileResource.get(i).size();
        }
      }
    } else {
      for (List<List<TsFileResource>> partitionUnSequenceTsFileResource : unSequenceTsFileResources
          .values()) {
        for (int i = unseqLevelNum - 1; i >= 0; i--) {
          result += partitionUnSequenceTsFileResource.get(i).size();
        }
      }
    }
    return result;
  }

  /**
   * recover files
   */
  @Override
  @SuppressWarnings("squid:S3776")
  public void recover() {
    File logFile = FSFactoryProducer.getFSFactory()
        .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        Set<String> deviceSet = logAnalyzer.getDeviceSet();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        long offset = logAnalyzer.getOffset();
        String targetFile = logAnalyzer.getTargetFile();
        boolean fullMerge = logAnalyzer.isFullMerge();
        boolean isSeq = logAnalyzer.isSeq();
        if (targetFile == null || sourceFileList.isEmpty()) {
          return;
        }
        File target = new File(targetFile);
        if (deviceSet.isEmpty()) {
          // if not in compaction, just delete the target file
          if (target.exists()) {
            Files.delete(target.toPath());
          }
          return;
        }
        if (fullMerge) {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          TsFileResource targetTsFileResource = getTsFileResource(targetFile, isSeq);
          long timePartition = targetTsFileResource.getTimePartition();
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
          // if not complete compaction, resume merge
          if (writer.hasCrashed()) {
            if (offset > 0) {
              writer.getIOWriterOut().truncate(offset - 1);
            }
            writer.close();
            CompactionLogger compactionLogger = new CompactionLogger(storageGroupDir,
                storageGroupName);
            CompactionUtils
                .merge(targetTsFileResource, getTsFileList(isSeq), storageGroupName,
                    compactionLogger, deviceSet, isSeq);
            compactionLogger.close();
          } else {
            writer.close();
          }
          // complete compaction and delete source file
          deleteAllSubLevelFiles(isSeq, timePartition);
        } else {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          TsFileResource targetResource = getTsFileResource(targetFile, isSeq);
          long timePartition = targetResource.getTimePartition();
          List<TsFileResource> sourceTsFileResources = new ArrayList<>();
          for (String file : sourceFileList) {
            // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
            sourceTsFileResources.add(getTsFileResource(file, isSeq));
          }
          int level = getMergeLevel(new File(sourceFileList.get(0)));
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
          // if not complete compaction, resume merge
          if (writer.hasCrashed()) {
            if (offset > 0) {
              writer.getIOWriterOut().truncate(offset - 1);
            }
            writer.close();
            CompactionLogger compactionLogger = new CompactionLogger(storageGroupDir,
                storageGroupName);
            CompactionUtils
                .merge(targetResource, sourceTsFileResources, storageGroupName,
                    compactionLogger, deviceSet,
                    isSeq);
            compactionLogger.close();
          } else {
            writer.close();
          }
          // complete compaction and delete source file
          deleteLevelFilesInDisk(sourceTsFileResources);
          deleteLevelFilesInList(timePartition, sourceTsFileResources, level, isSeq);
        }
      }
    } catch (IOException | IllegalPathException e) {
      logger.error("recover level tsfile management error ", e);
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete level tsfile management log file error ", e);
        }
      }
    }
  }

  // 对乱序文件第一层做特殊处理
  private boolean handleSpecificCase(long timePartition) {
    if (forkedUnSequenceTsFileResources.get(0).size() > config.getFirstLevelFileNum() &&
        isEmpty(true)) {
      writeLock();
      try {
        TsFileResource oldResource = forkedUnSequenceTsFileResources.get(0).get(0);
        File oldFile = oldResource.getTsFile();
        File newLevelFile = new File(oldFile.getAbsolutePath().replace("unsequence", "sequence"));
        FSFactoryProducer.getFSFactory().moveFile(oldFile, newLevelFile);
        FSFactoryProducer.getFSFactory().moveFile(
            FSFactoryProducer.getFSFactory().getFile(oldFile + RESOURCE_SUFFIX),
            FSFactoryProducer.getFSFactory().getFile(newLevelFile + RESOURCE_SUFFIX));

        oldResource.setFile(newLevelFile);
        unSequenceTsFileResources.get(timePartition).get(0).remove(0);
        sequenceTsFileResources.get(timePartition).get(0).add(oldResource);

        forkedUnSequenceTsFileResources.get(0).remove(0);
        forkedSequenceTsFileResources.get(0).add(oldResource);
      } catch (Exception e) {
        //TODO do nothing
      } finally {
        writeUnlock();
      }
      return true;
    }
    return false;
  }

  // 是否为乱序处理
  private boolean processUnseq() {
    return forkedUnSequenceTsFileResources.get(0).size() > config.getFirstLevelFileNum();
  }

  // 处理顺序, 直接移至下一层级
  private void processSeq(long timePartition) {
    // example : L1, L2, L3, level_num = 4
    for (int level = 1; level < config.getLevelNum() - 1; level++) {
      if (forkedSequenceTsFileResources.get(level - 1).size() > config.getFirstLevelFileNum() *
          Math.pow(config.getSizeRatio(), level)) {
        writeLock();
        try {
          TsFileResource oldRes = forkedSequenceTsFileResources.get(level - 1).get(0);
          File oldFile = oldRes.getTsFile();
          File newLevelFile = createNewTsFileName(oldFile, level);

          FSFactoryProducer.getFSFactory().moveFile(oldFile, newLevelFile);
          FSFactoryProducer.getFSFactory().moveFile(
              FSFactoryProducer.getFSFactory().getFile(oldFile + RESOURCE_SUFFIX),
              FSFactoryProducer.getFSFactory().getFile(newLevelFile + RESOURCE_SUFFIX));

          sequenceTsFileResources.get(timePartition).get(level - 1).remove(oldRes);
          forkedSequenceTsFileResources.get(level - 1).remove(0);
          oldRes.setFile(newLevelFile);
          sequenceTsFileResources.get(timePartition).get(level).add(oldRes);
          forkedSequenceTsFileResources.get(level).add(oldRes);
        } finally {
          writeUnlock();
        }
      }
    }
  }

  @Override
  protected Map<Long, Map<Long, List<TsFileResource>>> selectMergeFile(long timePartition) {
    Map<Long, Map<Long, List<TsFileResource>>> ans = new HashMap<>();
    Map<Long, List<TsFileResource>> files = new HashMap<>();
    // select next unseq files
    TsFileResource unseqFile = forkedUnSequenceTsFileResources.get(0).get(0);
    files.computeIfAbsent(0L, p -> new ArrayList<>()).add(unseqFile);
    List<TsFileResource> seqFileList = getTsFileList(true);
    for (Entry<String, Integer> deviceStartTimeEntry : unseqFile.getDeviceToIndexMap().entrySet()) {
      String deviceId = deviceStartTimeEntry.getKey();
      int deviceIndex = deviceStartTimeEntry.getValue();
      long unseqStartTime = unseqFile.getStartTime(deviceIndex);
      long unseqEndTime = unseqFile.getEndTime(deviceIndex);

      for (int i = 0; i < seqFileList.size(); i++) {
        TsFileResource seqFile = seqFileList.get(i);
        long seqStartTime = seqFile.getStartTime(deviceId);
        long seqEndTime = seqFile.getEndTime(deviceId);
        if (!(unseqEndTime < seqStartTime || unseqStartTime > seqEndTime)) {
          long level = (long) getMergeLevel(seqFile.getTsFile()) + 1;
          if (files.containsKey(level) && files.get(level).contains(seqFile)) {
            continue;
          } else {
            files.computeIfAbsent((long) getMergeLevel(seqFile.getTsFile()) + 1,
                p -> new ArrayList<>()).add(seqFile);
          }
        }
      }
    }
    ans.put(timePartition, files);
    return ans;
  }

  @Override
  protected void mergeFiles(
      Map<Long, Map<Long, List<TsFileResource>>> resources, long timePartition) {
    Map<Long, List<TsFileResource>> mergeResources = resources.get(timePartition);
    List<String> fileNames = new ArrayList<>();
    long maxLevel = 0;
    String seqPath = "";
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    // 获取最大的 level
    for (Entry<Long, List<TsFileResource>> resource : mergeResources.entrySet()) {
      maxLevel = Math.max(maxLevel, resource.getKey());
      if (seqPath.equals("") && resource.getKey() != 0) {
        seqPath = resource.getValue().get(0).getTsFile().getParent();
      }
      if (resource.getKey() == 0) {
        unseqFiles.addAll(resource.getValue());
      } else {
        seqFiles.addAll(resource.getValue());
      }
      for (TsFileResource res : resource.getValue()) {
        fileNames.add(res.getTsFile().getName());
      }
    }
    Collections.sort(fileNames);
    // special case : 顺序数据文件数量为 0, 将 unseq 文件直接下移
    if (seqFiles.isEmpty()) {
      writeLock();
      try {
        TsFileResource oldResource = unseqFiles.get(0);
        File oldFile = oldResource.getTsFile();
        File newLevelFile = new File(oldFile.getAbsolutePath().replace("unsequence", "sequence"));
        FSFactoryProducer.getFSFactory().moveFile(oldFile, newLevelFile);
        FSFactoryProducer.getFSFactory().moveFile(
            FSFactoryProducer.getFSFactory().getFile(oldFile + RESOURCE_SUFFIX),
            FSFactoryProducer.getFSFactory().getFile(newLevelFile + RESOURCE_SUFFIX));

        oldResource.setFile(newLevelFile);
        unSequenceTsFileResources.get(timePartition).get(0).remove(0);
        sequenceTsFileResources.get(timePartition).get(0).add(oldResource);

        forkedUnSequenceTsFileResources.get(0).remove(0);
        forkedSequenceTsFileResources.get(0).add(oldResource);
      } finally {
        writeUnlock();
      }
      return;
    }
    try {
      Set<Long> historicalVersions = new HashSet<>();
      for (TsFileResource tsFileResource : seqFiles) {
        historicalVersions.addAll(tsFileResource.getHistoricalVersions());
      }
      for (TsFileResource tsFileResource : unseqFiles) {
        historicalVersions.addAll(tsFileResource.getHistoricalVersions());
      }
      List<TsFileResource> newTsResources = new ArrayList<>();
      Set<PartialPath> devices = MManager.getInstance()
          .getDevices(new PartialPath(storageGroupName));
      Map<PartialPath, ChunkWriterImpl> chunkWriterCacheMap = new HashMap<>();
      for (PartialPath device : devices) {
        MNode deviceNode = MManager.getInstance().getNodeByPath(device);
        for (Entry<String, MNode> entry : deviceNode.getChildren().entrySet()) {
          MeasurementSchema measurementSchema = ((MeasurementMNode) entry.getValue()).getSchema();
          chunkWriterCacheMap
              .put(new PartialPath(device.toString(), entry.getKey()),
                  new ChunkWriterImpl(measurementSchema));
        }
      }
      List<PartialPath> unmergedSeries =
          MManager.getInstance().getAllTimeseriesPath(new PartialPath(storageGroupName));
      Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = createNewFileWriter(
          MERGE_SUFFIX, seqPath, fileNames, maxLevel - 1);
      RestorableTsFileIOWriter newFileWriter = newTsFilePair.left;
      TsFileResource newResource = newTsFilePair.right;
      newTsResources.add(newResource);

      List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
      for (List<PartialPath> pathList : devicePaths) {
        if (newResource.getTsFileSize() > config.getTsFileSizeThreshold() && fileNames.size() > 1) {
          newResource.setHistoricalVersions(historicalVersions);
          newResource.serialize();
          newFileWriter.endFile();
          newResource.close();
          fileNames.remove(0);
          newTsFilePair = createNewFileWriter(MERGE_SUFFIX, seqPath, fileNames, maxLevel - 1);
          newFileWriter = newTsFilePair.left;
          newResource = newTsFilePair.right;
          newTsResources.add(newResource);
        }

        String device = pathList.get(0).getDevice();
        newFileWriter.startChunkGroup(device);

        for (PartialPath path : pathList) {
          long currMinTime = Long.MAX_VALUE;
          long currMaxTime = Long.MIN_VALUE;
          ChunkWriterImpl chunkWriter = chunkWriterCacheMap.get(path);
          newFileWriter.addSchema(path, chunkWriter.getMeasurementSchema());
          QueryContext context = new QueryContext();
          IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path,
              chunkWriter.getMeasurementSchema().getType(),
              context, seqFiles, unseqFiles, null, null, true);
          while (tsFilesReader.hasNextBatch()) {
            BatchData batchData = tsFilesReader.nextBatch();
            currMinTime = Math.min(currMinTime, batchData.getTimeByIndex(0));
            SystemInfo.getInstance().incrementCompactionNum(batchData.length());
            for (int i = 0; i < batchData.length(); i++) {
              writeBatchPoint(batchData, i, chunkWriter);
            }
            if (!tsFilesReader.hasNextBatch()) {
              currMaxTime = Math.max(batchData.getTimeByIndex(batchData.length() - 1), currMaxTime);
            }
          }
          synchronized (newFileWriter) {
            chunkWriter.writeToFileWriter(newFileWriter);
          }
          newResource.updateStartTime(path.getDevice(), currMinTime);
          newResource.updateEndTime(path.getDevice(), currMaxTime);
          tsFilesReader.close();
        }
        newFileWriter.writeVersion(0L);
        newFileWriter.endChunkGroup();
      }
      newResource.setHistoricalVersions(historicalVersions);
      newResource.serialize();
      newFileWriter.endFile();
      newResource.close();
      cleanUp(resources, newTsResources, maxLevel, timePartition);
    } catch (Exception e) {
      //TODO do nothing
    }
  }

  private void cleanUp(Map<Long, Map<Long, List<TsFileResource>>> resources,
      List<TsFileResource> newTsResources, long level, long timePartition) {
    writeLock();
    try {
      Map<Long, List<TsFileResource>> cleanRes = resources.get(timePartition);
      for (Entry<Long, List<TsFileResource>> res : cleanRes.entrySet()) {
        if (res.getKey() == 0L) {
          unSequenceTsFileResources.get(timePartition).get(0).removeAll(res.getValue());
          forkedUnSequenceTsFileResources.get(0).removeAll(res.getValue());
        } else {
          sequenceTsFileResources.get(timePartition).get((int) (res.getKey() - 1))
              .removeAll(res.getValue());
          forkedSequenceTsFileResources.get((int) (res.getKey() - 1)).removeAll(res.getValue());
        }
        for (TsFileResource deleteRes : res.getValue()) {
          ChunkCache.getInstance().clear();
          ChunkMetadataCache.getInstance().clear();
          TimeSeriesMetadataCache.getInstance().clear();
          FileReaderManager.getInstance().closeFileAndRemoveReader(deleteRes.getTsFilePath());
          deleteRes.delete();
        }
      }
      for (TsFileResource oldResource : newTsResources) {
        File oldFile = oldResource.getTsFile();
        File newLevelFile = new File(oldFile.getAbsolutePath().replace(MERGE_SUFFIX, ""));
        FSFactoryProducer.getFSFactory().moveFile(oldFile, newLevelFile);
        FSFactoryProducer.getFSFactory().moveFile(
            FSFactoryProducer.getFSFactory().getFile(oldFile + RESOURCE_SUFFIX),
            FSFactoryProducer.getFSFactory().getFile(newLevelFile + RESOURCE_SUFFIX));
        oldResource.setFile(newLevelFile);
        sequenceTsFileResources.get(timePartition).get((int) (level - 1)).add(oldResource);
        forkedSequenceTsFileResources.get((int) (level - 1)).add(oldResource);
      }
    } catch (Exception e) {
      //TODO do nothing
    } finally {
      writeUnlock();
    }
  }

  private void deleteAllSubLevelFiles(boolean isSeq, long timePartition) {
    if (isSeq) {
      for (int level = 0; level < sequenceTsFileResources.get(timePartition).size();
          level++) {
        SortedSet<TsFileResource> currLevelMergeFile = sequenceTsFileResources.get(timePartition)
            .get(level);
        deleteLevelFilesInDisk(currLevelMergeFile);
        deleteLevelFilesInList(timePartition, currLevelMergeFile, level, isSeq);
      }
    } else {
      for (int level = 0; level < unSequenceTsFileResources.get(timePartition).size();
          level++) {
        SortedSet<TsFileResource> currLevelMergeFile = sequenceTsFileResources.get(timePartition)
            .get(level);
        deleteLevelFilesInDisk(currLevelMergeFile);
        deleteLevelFilesInList(timePartition, currLevelMergeFile, level, isSeq);
      }
    }
  }

  @Override
  protected void merge(long timePartition) {
//    if (isCompactionWorking()) {
//      return;
//    }
    long startTime = System.currentTimeMillis();
    handleSpecificCase(timePartition);
    if (processUnseq()) {
      Map<Long, Map<Long, List<TsFileResource>>> selectFiles = selectMergeFile(timePartition);
      mergeFiles(selectFiles, timePartition);
    }
    processSeq(timePartition);
    SystemInfo.getInstance().incrementCompactionTime(System.currentTimeMillis() - startTime);
    printInfo();
  }

  @SuppressWarnings("squid:S3776")
  private void merge(List<List<TsFileResource>> mergeResources, boolean sequence,
      long timePartition, int currMaxLevel, int currMaxFileNumInEachLevel) {
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
      for (int i = 0; i < currMaxLevel - 1; i++) {
        if (currMaxFileNumInEachLevel <= mergeResources.get(i).size()) {
          // level is numbered from 0
          if (enableUnseqCompaction && !sequence && i == currMaxLevel - 2) {
            // do not merge current unseq file level to upper level and just merge all of them to seq file
            merge(isForceFullMerge, getTsFileList(true), mergeResources.get(i), Long.MAX_VALUE);
          } else {
            CompactionLogger compactionLogger = new CompactionLogger(storageGroupDir,
                storageGroupName);
            // log source file list and target file for recover
            for (TsFileResource mergeResource : mergeResources.get(i)) {
              compactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
            }
            File newLevelFile = createNewTsFileName(mergeResources.get(i).get(0).getTsFile(),
                i + 1);
            compactionLogger.logSequence(sequence);
            compactionLogger.logFile(TARGET_NAME, newLevelFile);
            List<TsFileResource> toMergeTsFiles = mergeResources.get(i);
            logger.info("{} [Compaction] merge level-{}'s {} TsFiles to next level",
                storageGroupName, i, toMergeTsFiles.size());
            for (TsFileResource toMergeTsFile : toMergeTsFiles) {
              logger.info("{} [Compaction] start to merge TsFile {}", storageGroupName,
                  toMergeTsFile);
            }

            TsFileResource newResource = new TsFileResource(newLevelFile);
            // merge, read from source files and write to target file
            CompactionUtils
                .merge(newResource, toMergeTsFiles, storageGroupName, compactionLogger,
                    new HashSet<>(), sequence);
            logger.info(
                "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to delete old files",
                storageGroupName, i, toMergeTsFiles.size());
            writeLock();
            try {
              if (sequence) {
                sequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
              } else {
                unSequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
              }
              deleteLevelFilesInList(timePartition, toMergeTsFiles, i, sequence);
              if (mergeResources.size() > i + 1) {
                mergeResources.get(i + 1).add(newResource);
              }
            } finally {
              writeUnlock();
            }
            deleteLevelFilesInDisk(toMergeTsFiles);
            compactionLogger.close();
            File logFile = FSFactoryProducer.getFSFactory()
                .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
            if (logFile.exists()) {
              Files.delete(logFile.toPath());
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      // reset the merge working state to false
      logger.info("{} [Compaction] merge end time isSeq = {}, consumption: {} ms",
          storageGroupName, sequence,
          System.currentTimeMillis() - startTimeMillis);
    }
  }

  /**
   * if level < maxLevel-1, the file need compaction else, the file can be merged later
   */
  private File createNewTsFileName(File sourceFile, int level) {
    String path = sourceFile.getAbsolutePath();
    String prefixPath = path.substring(0, path.lastIndexOf(FILE_NAME_SEPARATOR) + 1);
    return new File(prefixPath + level + TSFILE_SUFFIX);
  }

  public static int getMergeLevel(File file) {
    String mergeLevelStr = file.getPath()
        .substring(file.getPath().lastIndexOf(FILE_NAME_SEPARATOR) + 1)
        .replaceAll(TSFILE_SUFFIX, "");
    return Integer.parseInt(mergeLevelStr);
  }

  private TsFileResource getTsFileResource(String filePath, boolean isSeq) throws IOException {
    if (isSeq) {
      for (List<SortedSet<TsFileResource>> tsFileResourcesWithLevel : sequenceTsFileResources
          .values()) {
        for (SortedSet<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
          for (TsFileResource tsFileResource : tsFileResources) {
            if (tsFileResource.getTsFile().getAbsolutePath().equals(filePath)) {
              return tsFileResource;
            }
          }
        }
      }
    } else {
      for (List<List<TsFileResource>> tsFileResourcesWithLevel : unSequenceTsFileResources
          .values()) {
        for (List<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
          for (TsFileResource tsFileResource : tsFileResources) {
            if (tsFileResource.getTsFile().getAbsolutePath().equals(filePath)) {
              return tsFileResource;
            }
          }
        }
      }
    }
    logger.error("cannot get tsfile resource path: {}", filePath);
    throw new IOException();
  }

  protected Pair<RestorableTsFileIOWriter, TsFileResource> createNewFileWriter
      (String mergeSuffix, String seqDir, List<String> fileNames, long level) throws IOException {
    String fileName = fileNames.get(0);
    String mergeLevelStr = fileName
        .substring(0, fileName.lastIndexOf(FILE_NAME_SEPARATOR) + 1)
        + level + TSFILE_SUFFIX + mergeSuffix;
    // use the minimum version as the version of the new file
    File newFile = FSFactoryProducer.getFSFactory().getFile(seqDir, mergeLevelStr);
    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = new Pair<>(
        new RestorableTsFileIOWriter(newFile), new TsFileResource(newFile));
    return newTsFilePair;
  }
}