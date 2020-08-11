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
package org.apache.iotdb.db.index;

import static org.apache.iotdb.db.conf.IoTDBConstant.SEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.UNSEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.META_DIR_NAME;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.read.IndexQueryReader;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
  private final String metaDirPath;
  /**
   * storage group name -> index storage group processor
   */
  private final Map<String, IndexStorageGroupProcessor> processorMap;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public IndexQueryReader getQuerySource(Path seriesPath, IndexType indexType,
      Filter timeFilter)
      throws IOException, MetadataException {
    String series = seriesPath.getFullPath();
    String storageGroupName = MManager.getInstance().getStorageGroupName(series);
    IndexStorageGroupProcessor sgProcessor = createStorageGroupProcessor(storageGroupName);
    List<IndexChunkMeta> seq = sgProcessor.getIndexSGMetadata(true, series, indexType);
    List<IndexChunkMeta> unseq = sgProcessor.getIndexSGMetadata(false, series, indexType);
    return new IndexQueryReader(seriesPath, indexType, timeFilter, seq, unseq);
  }

  @TestOnly
  public List<IndexChunkMeta> getIndexSGMetadata(String storageGroup, boolean sequence,
      String seriesPath, IndexType indexType) throws IOException {
    IndexStorageGroupProcessor sgProcessor = createStorageGroupProcessor(storageGroup);
    return sgProcessor.getIndexSGMetadata(sequence, seriesPath, indexType);
  }

  public IndexFileProcessor getNewIndexFileProcessor(String storageGroup, boolean sequence,
      long partitionId, String tsFileName) {
    IndexStorageGroupProcessor sgProcessor = createStorageGroupProcessor(storageGroup);
    return sgProcessor.createIndexFileProcessor(sequence, partitionId, tsFileName);
  }

  private IndexStorageGroupProcessor createStorageGroupProcessor(String storageGroup) {
    checkIndexSGMetaDataDir();
    IndexStorageGroupProcessor processor = processorMap.get(storageGroup);
    if (processor == null) {
      processor = new IndexStorageGroupProcessor(storageGroup, metaDirPath);
      IndexStorageGroupProcessor oldProcessor = processorMap.putIfAbsent(storageGroup, processor);
      if (oldProcessor != null) {
        return oldProcessor;
      }
    }
    return processor;
  }

  private void checkIndexSGMetaDataDir() {
    File metaDir = fsFactory.getFile(this.metaDirPath);
    if (!metaDir.exists()) {
      boolean mk = fsFactory.getFile(this.metaDirPath).mkdirs();
      if (mk) {
        logger.info("create index SG metadata folder {}", this.metaDirPath);
        System.out.println("create index SG metadata folder " + this.metaDirPath);
      }
    }
  }

  public void removeIndexProcessor(String storageGroupName, boolean sequence, String identifier)
      throws IOException {
    IndexStorageGroupProcessor sgProcessor = processorMap.get(storageGroupName);
    if (sgProcessor == null) {
      return;
    }
    sgProcessor.removeIndexProcessor(identifier, sequence);
  }

  private synchronized void close() throws IOException {
    for (Entry<String, IndexStorageGroupProcessor> entry : processorMap.entrySet()) {
      IndexStorageGroupProcessor processor = entry.getValue();
      processor.close();
    }
  }

  /**
   * Close all opened IndexFileProcessors and clear all data in memory. It's used to simulate the
   * case that IndexManager re-inits from the scratch after index files have been generated and
   * sealed. only for test now.
   */
  @TestOnly
  public synchronized void closeAndClear() throws IOException {
    for (Entry<String, IndexStorageGroupProcessor> entry : processorMap.entrySet()) {
      IndexStorageGroupProcessor processor = entry.getValue();
      processor.close();
    }
    clear();
  }

  private synchronized void clear() {
    this.processorMap.clear();
  }

  @Override
  public void start() throws StartupException {
    IndexBuildTaskPoolManager.getInstance().start();
    try {
      JMXService.registerMBean(this, ServiceType.INDEX_SERVICE.getJmxName());
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    if (!config.isEnableIndex()) {
      return;
    }
    try {
      close();
    } catch (IOException e) {
      logger.error("Close IndexManager failed.", e);
    }
    IndexBuildTaskPoolManager.getInstance().stop();
    JMXService.deregisterMBean(ServiceType.INDEX_SERVICE.getJmxName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.INDEX_SERVICE;
  }


  private IndexManager() {
    processorMap = new ConcurrentHashMap<>();
    metaDirPath =
        DirectoryManager.getInstance().getIndexRootFolder() + File.separator + META_DIR_NAME;
  }

  public static IndexManager getInstance() {
    return IndexManager.InstanceHolder.instance;
  }

  public synchronized void deleteAll() throws IOException {
    logger.info("Start deleting all storage groups' timeseries");
    close();
    // delete all index files
    for (Entry<String, IndexStorageGroupProcessor> entry : processorMap.entrySet()) {
      IndexStorageGroupProcessor processor = entry.getValue();
      processor.deleteAll();
    }
    File indexMetaDir = fsFactory.getFile(this.metaDirPath);
    if (indexMetaDir.exists()) {
      FileUtils.deleteDirectory(indexMetaDir);
    }
    File indexRootDir = fsFactory.getFile(DirectoryManager.getInstance().getIndexRootFolder());
    if (indexRootDir.exists()) {
      FileUtils.deleteDirectory(indexRootDir);
    }
    clear();
  }

  /**
   * When IoTDB restarts, check all index storage groups and delete index data and sg_metadata files
   * for deleted tsfile storage groups.
   */
  public synchronized void refreshIndexData() {
    String indexDataSeqDir =
        DirectoryManager.getInstance().getIndexRootFolder() + File.separator + SEQUENCE_FLODER_NAME;
    String indexDataUnSeqDir =
        DirectoryManager.getInstance().getIndexRootFolder() + File.separator
            + UNSEQUENCE_FLODER_NAME;
//
//    if (fsFactory.getFile(indexParentDir).mkdirs()) {
//      logger.info("create the index folder {}", indexParentDir);
//    }
//    String fullFilePath = indexParentDir + File.separator + getIndexFileName(tsFileName);
//
//    File indexMetaDir = fsFactory.getFile(this.metaDirPath);
//    File indexRootDir = fsFactory.getFile(DirectoryManager.getInstance().getIndexRootFolder());
//
//    Map<String, Map<IndexType, IndexInfo>> indexInfoMap = MManager
//        .getInstance().getAllIndexInfosInStorageGroup(storageGroupName);

  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static IndexManager instance = new IndexManager();
  }
}
