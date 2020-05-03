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

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.index.flush.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();


  private IndexBuildTaskPoolManager indexBuildPool = IndexBuildTaskPoolManager.getInstance();

  private MManager mManager;
//  private static Map<IndexType, IIndex> indexMap = new HashMap<>();

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


  private Map<String, IndexFileProcessor> indexProcessorMap;

  private Thread writeThread;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();


  /**
   * In current version, the index fuile has the same path format as the corresponding tsfile except
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
  private String getIndexFileName(String tsfileNameWithSuffix) {

    int tsfileLen = tsfileNameWithSuffix.length() - TsFileConstant.TSFILE_SUFFIX.length();
    return tsfileNameWithSuffix.substring(0, tsfileLen);
  }

  public IndexFileProcessor getProcessor(String storageGroup, boolean sequence, long partitionId,
      String tsFileName) {
    String relativeDir = (sequence ? SEQUENCE_FLODER_NAME : UNSEQUENCE_FLODER_NAME)
        + File.separator + storageGroup + File.separator + partitionId;
    String indexParentDir = DirectoryManager.getInstance().getIndexRootFolder() +
        File.separator + relativeDir;

    if (SystemFileFactory.INSTANCE.getFile(indexParentDir).mkdirs()) {
      logger.info("create the index folder {}", indexParentDir);
    }
    String fullFilePath = indexParentDir + File.separator + getIndexFileName(tsFileName);

    IndexFileProcessor fileProcessor = indexProcessorMap.get(fullFilePath);
    if (fileProcessor == null) {
      fileProcessor = initIndexProcessor(storageGroup, indexParentDir, fullFilePath, sequence);
      IndexFileProcessor oldProcessor = indexProcessorMap.putIfAbsent(fullFilePath, fileProcessor);
      if (oldProcessor != null) {
        return oldProcessor;
      }
    }
    return fileProcessor;
  }

  private IndexFileProcessor initIndexProcessor(String storageGroup, String indexParentDir,
      String fullFilePath, boolean sequence) {
    return new IndexFileProcessor(storageGroup, indexParentDir, fullFilePath, sequence);
  }


  public void removeIndexProcessor(String identifier) {
    indexProcessorMap.remove(identifier);
//    if (processor != null) {
//      processor.close();
//    }
  }

  public void close() {
    logger.info("IndexManager won't be closed alone, but with StorageEngine and MManger.");
    throw new NotImplementedException("index processor close");
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
    IndexBuildTaskPoolManager.getInstance().stop();
    JMXService.deregisterMBean(ServiceType.INDEX_SERVICE.getJmxName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.INDEX_SERVICE;
  }


  private IndexManager() {
    indexProcessorMap = new ConcurrentHashMap<>();
  }

  public static IndexManager getInstance() {
    return IndexManager.InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static IndexManager instance = new IndexManager();
  }

  public String toString() {
    return String
        .format("the size of Index Build Task Pool: %d", indexBuildPool.getWorkingTasksNumber());
  }

  /**
   * you can refer to MManger. We needn't add write lock since it's controlled by MManager.
   */


}
