/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index;

import static org.apache.iotdb.db.conf.IoTDBConstant.SEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.UNSEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXED_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEXING_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.STORAGE_GROUP_INDEXED_SUFFIX;
import static org.apache.iotdb.db.index.common.IndexConstant.STORAGE_GROUP_INDEXING_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.io.IndexIOReader;
import org.apache.iotdb.db.index.read.IndexFileResource;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexStorageProcessor manages the index of all time series belonging to a StorageGroup. It can
 * joint two adjacent index files between their boundaries.
 */
public class IndexStorageGroupProcessor {

  private static final Logger logger = LoggerFactory.getLogger(IndexStorageGroupProcessor.class);
  private final String seqDir;
  private final String unseqDir;
  private final String indexSGMetaFileName;
  private final String storageGroupName;

  /**
   * IndexFileName -> IndexFileProcessor. Each IndexFileProcessor corresponds to an open
   * TSFileProcessor, and different processors belong to different partition ranges.
   */
  private final Map<String, IndexProcessor> seqIndexProcessorMap = new ConcurrentHashMap<>();
  private final Map<String, IndexProcessor> unseqIndexProcessorMap = new ConcurrentHashMap<>();

  /**
   * After IndexFileProcessor is closed, it will generate a {@linkplain IndexFileResource} and add
   * the resource to corresponding resource list for reading.
   *
   * When the system restart and a IndexStorageGroupProcessor is created, all its resources will be
   * reloaded. Only the first-layer metadata will be loaded, which is slight enough. Only when a
   * query on a time series arrives, its chunk metadata will be read in and put into the chunk
   * metadata cache for efficiency.
   */
  private List<IndexFileResource> unseqResourceList = new ArrayList<>();
  private List<IndexFileResource> seqResourceList = new ArrayList<>();


  private final Map<Long, Map<String, Map<IndexType, ByteBuffer>>> indexSGSeqMetadata = new TreeMap<>();
  private final Map<Long, Map<String, Map<IndexType, ByteBuffer>>> indexSGUnseqMetadata = new TreeMap<>();
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public IndexStorageGroupProcessor(String storageGroupName, String metaDir) {
    this.storageGroupName = storageGroupName;

    this.seqDir =
        DirectoryManager.getInstance().getIndexRootFolder() + File.separator + SEQUENCE_FLODER_NAME
            + File.separator + storageGroupName;
    this.unseqDir = DirectoryManager.getInstance().getIndexRootFolder() + File.separator
        + UNSEQUENCE_FLODER_NAME + File.separator + storageGroupName;
    this.indexSGMetaFileName = metaDir + File.separator + storageGroupName;
    try {
      recover();
    } catch (IOException e) {
      logger.error("recover meet error", e);
    }
  }

  private void recover() throws IOException {
    logger.info("start to recover Index Storage Group  {}", storageGroupName);
    deserialize();
    List<IndexFileResource> tmpSeqIndexResources = getAllFiles(seqDir);
    List<IndexFileResource> tmpUnseqIndexResources = getAllFiles(unseqDir);
    seqResourceList.addAll(tmpSeqIndexResources);
    unseqResourceList.addAll(tmpUnseqIndexResources);
    logger.info("finish recovering Index Storage Group {}", storageGroupName);
  }

  private List<IndexFileResource> getAllFiles(String indexDir) throws IOException {
    List<IndexFileResource> res = new ArrayList<>();
    List<File> indexFiles = new ArrayList<>();
    File fileFolder = fsFactory.getFile(indexDir);
    if (!fileFolder.exists()) {
      return res;
    }
    File[] subFiles = fileFolder.listFiles();
    if (subFiles != null) {
      for (File partitionFolder : subFiles) {
        continueFailedRenames(partitionFolder);
        if (!partitionFolder.isDirectory()) {
          logger.warn("{} is not a directory.", partitionFolder.getAbsolutePath());
          continue;
        }
        Collections.addAll(indexFiles,
            fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), INDEXED_SUFFIX));
      }
    }

    indexFiles.sort(this::compareFileName);
    for (File f : indexFiles) {
      res.add(new IndexFileResource(f.getAbsolutePath()));
    }
    return res;
  }

  /**
   * <p>Refer to {@code StorageGroupProcessor#compareFileName(java.io.File, java.io.File)}</p>
   *
   * <p>Name format: {systemTime}-{versionNum}-{mergeNum}.index</p>
   */
  private int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(INDEXED_SUFFIX, "")
        .split(IoTDBConstant.FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(INDEXED_SUFFIX, "")
        .split(IoTDBConstant.FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    } else {
      return cmp;
    }
  }


  /**
   * Up to now, we adopt a naive recovery strategy. If the index file has been generated completely,
   * rename it. Otherwise, delete the index file directly. This will not affect the correctness of
   * the index's query results
   */
  private void continueFailedRenames(File fileFolder) throws IOException {
    File[] files = fsFactory.listFilesBySuffix(fileFolder.getAbsolutePath(), INDEXING_SUFFIX);
    if (files != null) {
      for (File indexFile : files) {
        // check fails
        TsFileInput indexInput = FSFactoryProducer
            .getFileInputFactory().getTsFileInput(indexFile.getPath());
        if (IndexIOReader.checkTailComplete(indexInput)) {
          File normalResource = fsFactory.getFile(indexFile.getPath().replace(
              INDEXING_SUFFIX, INDEXED_SUFFIX));
          indexFile.renameTo(normalResource);
        } else {
          indexFile.delete();
        }
        indexInput.close();
      }
    }
  }

  /**
   * In current version, the index file has the same path format as the corresponding tsfile except
   * that tsfile_base_dir and the ".tsfile" suffix are replaced by index_base_dir and ".index".
   * <br>
   * i.e., index_base_dir / [un]sequence / storage_group_name / partition_id / tsfile_name.index
   * <br>
   * e.g.<br>
   *
   * tsfile: data/sequence/root.idx1/0/1587719150666-1-0.tsfile index file:
   * index/sequence/root.idx1/0/1587719150666-1-0.index
   *
   * Since the total size of index files may be large, the base_dir may be selected from a list of
   * param, just like {@linkplain DirectoryManager#getNextFolderForSequenceFile()
   * getNextFolderForSequenceFile} in future.
   */
  private static String getIndexFileName(String tsfileNameWithSuffix) {

    int tsfileLen = tsfileNameWithSuffix.length() - TsFileConstant.TSFILE_SUFFIX.length();
    return tsfileNameWithSuffix.substring(0, tsfileLen);
  }

  public IndexProcessor createIndexFileProcessor(boolean sequence, long partitionId,
      String tsFileName) {

    Map<String, IndexProcessor> processorMap = getSeqOrUnseqProcessorMap(sequence);
    String indexParentDir = sequence ? seqDir : unseqDir;
    indexParentDir += File.separator + partitionId;

    if (fsFactory.getFile(indexParentDir).mkdirs()) {
      logger.info("create the index folder {}", indexParentDir);
    }
    String fullFilePath = indexParentDir + File.separator + getIndexFileName(tsFileName);

    IndexProcessor fileProcessor = processorMap.get(fullFilePath);
    if (fileProcessor == null) {
      Map<String, Map<IndexType, ByteBuffer>> previousMeta = sequence ?
          indexSGSeqMetadata.computeIfAbsent(partitionId, p -> new HashMap<>()) :
          indexSGUnseqMetadata.computeIfAbsent(partitionId, p -> new HashMap<>());
//      fileProcessor = new IndexProcessor(storageGroupName, fullFilePath,
//          sequence, partitionId, previousMeta, this::updateIndexFileResources);
      IndexUtils.breakDown();
      IndexProcessor oldProcessor = processorMap.putIfAbsent(fullFilePath, fileProcessor);
      if (oldProcessor != null) {
        return oldProcessor;
      }
    }
    return fileProcessor;
  }

  private void updateIndexFileResources(boolean sequence, long partitionId,
      IndexFileResource resources, Map<String, Map<IndexType, ByteBuffer>> indexSaved) {
    Map<Long, Map<String, Map<IndexType, ByteBuffer>>> indexSGMetadata;
    if (sequence) {
      seqResourceList.add(resources);
      indexSGMetadata = indexSGSeqMetadata;
    } else {
      unseqResourceList.add(resources);
      indexSGMetadata = indexSGUnseqMetadata;
    }
    // update resource
    Map<String, Map<IndexType, ByteBuffer>> partitionMap = indexSGMetadata
        .computeIfAbsent(partitionId, p -> new HashMap<>());
    indexSaved.forEach((path, savedPathMap) -> {
      Map<IndexType, ByteBuffer> pathMap = partitionMap
          .computeIfAbsent(path, p -> new EnumMap<>(IndexType.class));
      // replace directly
      savedPathMap.forEach(pathMap::put);
    });
  }


  private Map<String, IndexProcessor> getSeqOrUnseqProcessorMap(boolean sequence) {
    return sequence ? seqIndexProcessorMap : unseqIndexProcessorMap;
  }

  private List<IndexFileResource> getSeqOrUnseqResources(boolean sequence) {
    return sequence ? seqResourceList : unseqResourceList;
  }

  public void removeIndexProcessor(String identifier, boolean sequence) throws IOException {
    Map<String, IndexProcessor> processorMap = getSeqOrUnseqProcessorMap(sequence);
    IndexProcessor processor = processorMap.remove(identifier);
    if (processor != null) {
      processor.close();
    }
  }

  public synchronized void close() throws IOException {
    for (Entry<String, IndexProcessor> entry : seqIndexProcessorMap.entrySet()) {
      IndexProcessor processor = entry.getValue();
      processor.close();
    }

    for (Entry<String, IndexProcessor> entry : unseqIndexProcessorMap.entrySet()) {
      IndexProcessor processor = entry.getValue();
      processor.close();
    }
    serialize();
  }

  public synchronized void deleteAll() throws IOException {
    logger.info("Start deleting all storage groups' timeseries");
    close();
    // delete all index files
    for (Entry<String, IndexProcessor> entry : seqIndexProcessorMap.entrySet()) {
      IndexProcessor processor = entry.getValue();
      File file = new File(processor.getIndexSeries().getFullPath());
      IndexUtils.breakDown("File file = new File(processor.getIndexSeries());");
      if (file.exists()) {
        file.delete();
      }
    }
    for (Entry<String, IndexProcessor> entry : unseqIndexProcessorMap.entrySet()) {
      IndexProcessor processor = entry.getValue();
      File file = new File(processor.getIndexSeries().getFullPath());
      if (file.exists()) {
        file.delete();
      }
    }
    File seqFile = new File(seqDir);
    if (seqFile.exists()) {
      FileUtils.deleteDirectory(seqFile);
    }
    File unseqFile = new File(unseqDir);
    if (unseqFile.exists()) {
      FileUtils.deleteDirectory(unseqFile);
    }
    File metaFile = new File(indexSGMetaFileName);
    if (metaFile.exists()) {
      FileUtils.deleteDirectory(metaFile);
    }
    clear();
  }

  private synchronized void clear() {
    seqIndexProcessorMap.clear();
    unseqIndexProcessorMap.clear();
    seqResourceList.clear();
    unseqResourceList.clear();
    indexSGSeqMetadata.clear();
    indexSGUnseqMetadata.clear();
  }

  public List<IndexChunkMeta> getIndexSGMetadata(boolean sequence, String seriesPath,
      IndexType indexType) throws IOException {
    List<IndexChunkMeta> res = new ArrayList<>();
    List<IndexFileResource> resourceList = getSeqOrUnseqResources(sequence);
    for (IndexFileResource indexFileResource : resourceList) {
      res.addAll(indexFileResource.getChunkMetas(seriesPath, indexType));
    }
    return res;
  }

  private void serializeIndexSGMetadata(
      Map<Long, Map<String, Map<IndexType, ByteBuffer>>> indexSGMetadata,
      OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(indexSGMetadata.size(), outputStream);
    for (Entry<Long, Map<String, Map<IndexType, ByteBuffer>>> pEntry : indexSGMetadata.entrySet()) {
      Long partitionId = pEntry.getKey();
      Map<String, Map<IndexType, ByteBuffer>> partitionMap = pEntry.getValue();
      ReadWriteIOUtils.write(partitionId, outputStream);
      ReadWriteIOUtils.write(partitionMap.size(), outputStream);
      for (Entry<String, Map<IndexType, ByteBuffer>> pathEntry : partitionMap.entrySet()) {
        String path = pathEntry.getKey();
        Map<IndexType, ByteBuffer> pathMap = pathEntry.getValue();
        ReadWriteIOUtils.write(path, outputStream);
        ReadWriteIOUtils.write(pathMap.size(), outputStream);
        for (Entry<IndexType, ByteBuffer> indexEntry : pathMap.entrySet()) {
          IndexType indexType = indexEntry.getKey();
          ByteBuffer buffer = indexEntry.getValue();
          ReadWriteIOUtils.write(indexType.serialize(), outputStream);
          ReadWriteIOUtils.write(buffer, outputStream);
        }
      }
    }
  }

  private void deserializeIndexSGMetadata(
      Map<Long, Map<String, Map<IndexType, ByteBuffer>>> indexSGMetadata,
      InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      Map<String, Map<IndexType, ByteBuffer>> partitionMap = new HashMap<>();
      long partitionId = ReadWriteIOUtils.readLong(inputStream);
      int partitionSize = ReadWriteIOUtils.readInt(inputStream);
      for (int partitionI = 0; partitionI < partitionSize; partitionI++) {
        String path = ReadWriteIOUtils.readString(inputStream);
        int pathSize = ReadWriteIOUtils.readInt(inputStream);
        Map<IndexType, ByteBuffer> pathMap = new EnumMap<>(IndexType.class);
        for (int pathI = 0; pathI < pathSize; pathI++) {
          IndexType indexType = IndexType.deserialize(ReadWriteIOUtils.readShort(inputStream));
          ByteBuffer byteBuffer = ReadWriteIOUtils
              .readByteBufferWithSelfDescriptionLength(inputStream);
          pathMap.put(indexType, byteBuffer);
        }
        partitionMap.put(path, pathMap);
      }
      indexSGMetadata.put(partitionId, partitionMap);
    }
  }

  private void serialize() throws IOException {
    OutputStream outputStream = fsFactory
        .getBufferedOutputStream(this.indexSGMetaFileName + STORAGE_GROUP_INDEXING_SUFFIX);
    // add
    serializeIndexSGMetadata(indexSGSeqMetadata, outputStream);
    serializeIndexSGMetadata(indexSGUnseqMetadata, outputStream);
    outputStream.close();
    File src = fsFactory.getFile(this.indexSGMetaFileName + STORAGE_GROUP_INDEXING_SUFFIX);
    File dest = fsFactory.getFile(this.indexSGMetaFileName + STORAGE_GROUP_INDEXED_SUFFIX);
    dest.delete();
    fsFactory.moveFile(src, dest);
  }

  private void deserialize() throws IOException {
    if (!fsFactory.getFile(this.indexSGMetaFileName + STORAGE_GROUP_INDEXED_SUFFIX).exists()) {
      File tmpMetaFile = fsFactory.getFile(this.indexSGMetaFileName + STORAGE_GROUP_INDEXED_SUFFIX);
      if (tmpMetaFile.exists()) {
        tmpMetaFile.delete();
      }
      return;
    }
    InputStream inputStream = fsFactory.getBufferedInputStream(
        this.indexSGMetaFileName + STORAGE_GROUP_INDEXED_SUFFIX);
    deserializeIndexSGMetadata(indexSGSeqMetadata, inputStream);
    deserializeIndexSGMetadata(indexSGUnseqMetadata, inputStream);
    inputStream.close();
  }


  @FunctionalInterface
  public interface UpdateIndexFileResourcesCallBack {

    void call(boolean sequence, long partitionId, IndexFileResource resources,
        Map<String, Map<IndexType, ByteBuffer>> indexSaved);
  }
}
