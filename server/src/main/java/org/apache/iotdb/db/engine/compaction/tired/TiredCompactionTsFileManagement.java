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

package org.apache.iotdb.db.engine.compaction.tired;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
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

public class TiredCompactionTsFileManagement extends TsFileManagement {

  public static final String MERGE_SUFFIX = ".merge";

  private static final Logger logger = LoggerFactory.getLogger(
      TiredCompactionTsFileManagement.class);
  private final int unseqLevelNum = Math
      .max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);

  public TiredCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
  }

  @Override
  public void forkCurrentFileList(long timePartition) throws IOException {
    synchronized (sequenceTsFileResources) {
      forkTsFileList(
          forkedSequenceTsFileResources,
          sequenceTsFileResources.computeIfAbsent(timePartition, this::newSequenceTsFileResources),
          config.getLevelNum());
    }
    synchronized (unSequenceTsFileResources) {
      forkTsFileList(
          forkedUnSequenceTsFileResources,
          unSequenceTsFileResources
              .computeIfAbsent(timePartition, this::newUnSequenceTsFileResources),
          config.getLevelNum());
    }
  }

  @Override
  protected List<SortedSet<TsFileResource>> newSequenceTsFileResources(Long k) {
    List<SortedSet<TsFileResource>> newSequenceTsFileResources = new CopyOnWriteArrayList<>();
    for (int i = 0; i < config.getLevelNum(); i++) {
      newSequenceTsFileResources.add(Collections.synchronizedSortedSet(new TreeSet<>(
          (o1, o2) -> {
            try {
              int rangeCompare = Long
                  .compare(Long.parseLong(o1.getTsFile().getParentFile().getName()),
                      Long.parseLong(o2.getTsFile().getParentFile().getName()));
              return rangeCompare == 0 ? compareFileName(o1.getTsFile(), o2.getTsFile())
                  : rangeCompare;
            } catch (NumberFormatException e) {
              return compareFileName(o1.getTsFile(), o2.getTsFile());
            }
          })));
    }
    return newSequenceTsFileResources;
  }

  @Override
  protected List<List<TsFileResource>> newUnSequenceTsFileResources(Long k) {
    List<List<TsFileResource>> newUnSequenceTsFileResources = new CopyOnWriteArrayList<>();
    for (int i = 0; i < config.getLevelNum(); i++) {
      newUnSequenceTsFileResources.add(new CopyOnWriteArrayList<>());
    }
    return newUnSequenceTsFileResources;
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

  public static int getMergeLevel(File file) {
    String mergeLevelStr = file.getPath()
        .substring(file.getPath().lastIndexOf(FILE_NAME_SEPARATOR) + 1)
        .replaceAll(TSFILE_SUFFIX, "");
    return Integer.parseInt(mergeLevelStr);
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

  @Override
  public void recover() {
    logger.info("{} no recover logic", storageGroupName);
  }

  @Override
  protected Map<Long, Map<Long, List<TsFileResource>>> selectMergeFile(long timePartition) {
    Map<Long, Map<Long, List<TsFileResource>>> mergedFilesRet = new HashMap<>();
    Map<Long, List<TsFileResource>> selectFiles = new HashMap<>();
    boolean isMergedFileFound = false;
    // judge seq and unseq independently
    for (int i = 0; i < config.getLevelNum() - 1; i++) {
      if (config.getTiredFileNum() <= forkedSequenceTsFileResources.get(i).size()) {
        isMergedFileFound = true;
        List<TsFileResource> mergedRes = forkedSequenceTsFileResources.get(i)
            .subList(0, config.getTiredFileNum());
        selectFiles.put((long) i, mergedRes);
        break;
      }
    }
    for (int i = 0; i < config.getLevelNum() - 1 && !isMergedFileFound; i++) {
      if (config.getTiredFileNum() <= forkedUnSequenceTsFileResources.get(i).size()) {
        List<TsFileResource> mergedRes = forkedUnSequenceTsFileResources.get(i)
            .subList(0, config.getTiredFileNum());
        selectFiles.put((long) i, mergedRes);
        break;
      }
    }
    mergedFilesRet.put(timePartition, selectFiles);
    return mergedFilesRet;
  }

  @Override
  protected void merge(long timePartition) {
//    if (isCompactionWorking()) {
//      return;
//    }
    long startTime = System.currentTimeMillis();
    Map<Long, Map<Long, List<TsFileResource>>> selectFiles = selectMergeFile(timePartition);
    mergeFiles(selectFiles, timePartition);
    SystemInfo.getInstance().incrementCompactionTime(System.currentTimeMillis() - startTime);
    printInfo();
  }

  @Override
  protected void mergeFiles(Map<Long, Map<Long, List<TsFileResource>>> resources,
      long timePartition) {
    Map<Long, List<TsFileResource>> mergeResources = resources.get(timePartition);
    List<String> fileNames = new ArrayList<>();
    long mergedLevel = 0;
    String parentPath = "";
    List<TsFileResource> seqFiles = new ArrayList<>();
    List<TsFileResource> unseqFiles = new ArrayList<>();
    // 获取最大的 level
    for (Entry<Long, List<TsFileResource>> resource : mergeResources.entrySet()) {
      mergedLevel = resource.getKey();
      if (parentPath.equals("")) {
        parentPath = resource.getValue().get(0).getTsFile().getParent();
      }
      if (parentPath.contains("unsequence")) {
        unseqFiles.addAll(resource.getValue());
      } else {
        seqFiles.addAll(resource.getValue());
      }
      for (TsFileResource res : resource.getValue()) {
        fileNames.add(res.getTsFile().getName());
      }
    }
    if (seqFiles.isEmpty() && unseqFiles.isEmpty()){
      return;
    }
    Collections.sort(fileNames);
    try {
      // get historical versions
      Set<Long> historicalVersions = new HashSet<>();
      for (TsFileResource tsFileResource : seqFiles) {
        historicalVersions.addAll(tsFileResource.getHistoricalVersions());
      }
      for (TsFileResource tsFileResource : unseqFiles) {
        historicalVersions.addAll(tsFileResource.getHistoricalVersions());
      }

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
          MERGE_SUFFIX, parentPath, fileNames, mergedLevel + 1);
      RestorableTsFileIOWriter newFileWriter = newTsFilePair.left;
      TsFileResource newResource = newTsFilePair.right;

      List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
      for (List<PartialPath> pathList : devicePaths) {
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
      cleanUp(resources, newResource, mergedLevel, timePartition, parentPath);
    } catch (Exception e) {
      //TODO do nothing
    }
  }

  protected Pair<RestorableTsFileIOWriter, TsFileResource> createNewFileWriter
      (String mergeSuffix, String seqDir, List<String> fileNames, long level) throws IOException {
    String fileName = fileNames.get(0);
    String mergeLevelStr = fileName
        .substring(0, fileName.lastIndexOf(FILE_NAME_SEPARATOR) + 1)
        + level + TSFILE_SUFFIX + mergeSuffix;
    // use the minimum version as the version of the new file
    File newFile = FSFactoryProducer.getFSFactory().getFile(seqDir, mergeLevelStr);
    return new Pair<>(new RestorableTsFileIOWriter(newFile), new TsFileResource(newFile));
  }

  private void cleanUp(Map<Long, Map<Long, List<TsFileResource>>> resources,
      TsFileResource newTsResource, long level, long timePartition, String parentPath) {
    writeLock();
    try {
      Map<Long, List<TsFileResource>> cleanRes = resources.get(timePartition);
      for (Entry<Long, List<TsFileResource>> res : cleanRes.entrySet()) {
        if (parentPath.contains("unsequence")) {
          unSequenceTsFileResources.get(timePartition).get((int) (long) (res.getKey()))
              .removeAll(res.getValue());
        } else {
          sequenceTsFileResources.get(timePartition).get((int) (long) (res.getKey()))
              .removeAll(res.getValue());
        }
        for (TsFileResource deleteRes : res.getValue()) {
          deleteRes.delete();
          ChunkMetadataCache.getInstance().clear();
          TimeSeriesMetadataCache.getInstance().clear();
          FileReaderManager.getInstance().closeFileAndRemoveReader(deleteRes.getTsFilePath());
          deleteRes.delete();
        }
      }
      File oldFile = newTsResource.getTsFile();
      File newLevelFile = new File(oldFile.getAbsolutePath().replace(MERGE_SUFFIX, ""));
      FSFactoryProducer.getFSFactory().moveFile(oldFile, newLevelFile);
      FSFactoryProducer.getFSFactory().moveFile(
          FSFactoryProducer.getFSFactory().getFile(oldFile + RESOURCE_SUFFIX),
          FSFactoryProducer.getFSFactory().getFile(newLevelFile + RESOURCE_SUFFIX));
      newTsResource.setFile(newLevelFile);
      if (parentPath.contains("unsequence")) {
        unSequenceTsFileResources.get(timePartition).get((int) (level + 1)).add(newTsResource);
      } else {
        sequenceTsFileResources.get(timePartition).get((int) (level + 1)).add(newTsResource);
      }
    } catch (Exception e) {
      //TODO do nothing
    } finally {
      writeUnlock();
    }
  }
}
