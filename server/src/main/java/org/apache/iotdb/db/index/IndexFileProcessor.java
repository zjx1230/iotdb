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

import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_MAP_INIT_RESERVE_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.flush.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage insert ahead logs of a TsFile.
 */
public class IndexFileProcessor implements Comparable<IndexFileProcessor> {

  private static final Logger logger = LoggerFactory.getLogger(IndexFileProcessor.class);
  private final String storageGroupName;
  private String indexFilePath;

  //  private String path;
  private String indexParentDir;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ReadWriteLock lock = new ReentrantReadWriteLock();
//  private static long firstLayerBufferSize = ;

  private Map<String, Map<IndexType, IoTDBIndex>> allPathsIndexMap;

  private ILogWriter currentFileWriter;

  private RestorableTsFileIOWriter writer;
  private final boolean sequence;


  private final IndexBuildTaskPoolManager indexBuildPoolManager;
  private ConcurrentLinkedQueue flushTaskQueue;
  private Future flushTaskFuture;
  private boolean noMoreIndexFlushTask = false;
  public final Object waitingSymbol = new Object();
  private static long maxIndexBufferSize = IoTDBDescriptor.getInstance().getConfig()
      .getIndexBufferSize();
  private final AtomicLong memoryUsed;

  public IndexFileProcessor(String storageGroupName, String indexParentDir, String indexFilePath,
      boolean sequence) {
    this.storageGroupName = storageGroupName;
    this.indexParentDir = indexParentDir;
    this.indexFilePath = indexFilePath;
    this.sequence = sequence;
    this.indexBuildPoolManager = IndexBuildTaskPoolManager.getInstance();
    memoryUsed = new AtomicLong(0);
//    memoryThreshold =
    refreshSeriesIndexMapFromMManager();

  }

  private void refreshSeriesIndexMapFromMManager() {
    Map<String, Map<IndexType, IndexInfo>> indexInfoMap = MManager
        .getInstance().getAllIndexInfosInStorageGroup(storageGroupName);
    if (this.allPathsIndexMap == null) {
      this.allPathsIndexMap = new HashMap<>(indexInfoMap.size() + INDEX_MAP_INIT_RESERVE_SIZE);
    }
    // Add indexes that are not in the previous map
    indexInfoMap.forEach((path, pathIndexInfoMap) -> {
      Map<IndexType, IoTDBIndex> pathIndexMap = allPathsIndexMap
          .putIfAbsent(path, new EnumMap<>(IndexType.class));
      assert pathIndexMap != null;
      pathIndexInfoMap.forEach((indexType, indexInfo) -> pathIndexMap
          .putIfAbsent(indexType, IndexType.constructIndex(indexType, indexInfo)));
    });
    // remove indexes that are removed from the previous map
    for (String pathInMem : new ArrayList<>(allPathsIndexMap.keySet())) {
      Map<IndexType, IoTDBIndex> pathIndexMap = allPathsIndexMap.get(pathInMem);
      if (!indexInfoMap.containsKey(pathInMem)) {
        pathIndexMap.forEach((indexType, index) -> index.delete());
        allPathsIndexMap.remove(pathInMem);
      } else {
        Map<IndexType, IndexInfo> pathIndexInfoMap = indexInfoMap.get(pathInMem);
        for (IndexType indexType : new ArrayList<>(pathIndexMap.keySet())) {
          if (!pathIndexInfoMap.containsKey(indexType)) {
            pathIndexMap.get(indexType).delete();
            pathIndexMap.remove(indexType);
          }
        }
      }
    }
  }


  /**
   * seal the index file, move "indexing" to "index"
   */
  public void close() {
    //TODO
//    sync();
//    forceWal();
//    try {
//      if (this.currentFileWriter != null) {
//        this.currentFileWriter.close();
//        this.currentFileWriter = null;
//      }
//      logger.debug("Log node {} closed successfully", indexFilePath);
//    } catch (IOException e) {
//      logger.error("Cannot close log node {} because:", indexFilePath, e);
//    } finally {
//      lock.writeLock().unlock();
//    }
//    String indexedFullPath = indexFilePath.substring(0, indexFilePath.length() -
//        INDEXING_SUFFIX.length()) + INDEXED_SUFFIX;
//    File oldFile = FSFactoryProducer.getFSFactory().getFile(indexFilePath);
//    File newFile = FSFactoryProducer.getFSFactory().getFile(indexedFullPath);
//    if (oldFile.renameTo(newFile)) {
//      logger.error("Renaming indexing file to indexed failed {}.", indexedFullPath);
//    }
  }


  public String getIndexFilePath() {
    return indexFilePath;
  }

  public String getIndexParentDir() {
    return indexParentDir;
  }

  @Override
  public int hashCode() {
    return indexFilePath.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    return compareTo((IndexFileProcessor) obj) == 0;
  }

  @Override
  public String toString() {
    return "Index File: " + indexFilePath;
  }

  @Override
  public int compareTo(IndexFileProcessor o) {
    return indexFilePath.compareTo(o.indexFilePath);
  }


  @SuppressWarnings("squid:S135")
  private Runnable flushRunTask = () -> {
    long ioTime = 0;
    boolean returnWhenNoTask = false;
    while (true) {
      if (noMoreIndexFlushTask) {
        returnWhenNoTask = true;
      }
      Object indexFlushMessage = flushTaskQueue.poll();
      if (indexFlushMessage == null) {
        if (returnWhenNoTask) {
          break;
        }
        try {
          Thread.sleep(10);
        } catch (@SuppressWarnings("squid:S2142") InterruptedException e) {
          logger.error("Index Flush Task is interrupted, index path {}", indexFilePath, e);
          break;
        }
      } else {
        try {
          // TODO flush task
          throw new IOException("asd");
//          if (indexFlushMessage instanceof StartFlushGroupIOTask) {
//            writer.startChunkGroup(((StartFlushGroupIOTask) indexFlushMessage).deviceId);
//          } else if (indexFlushMessage instanceof IChunkWriter) {
//            ChunkWriterImpl chunkWriter = (ChunkWriterImpl) indexFlushMessage;
//            chunkWriter.writeToFileWriter(MemTableFlushTask.this.writer);
//          } else {
//            writer.endChunkGroup();
//          }
        } catch (@SuppressWarnings("squid:S2139") IOException e) {
          logger.error("Index Flush Task meet IO error, index path: {}", indexFilePath, e);
          throw new FlushRunTimeException(e);
        }
      }
    }
  };


  public void startFlushMemTable() {
    this.flushTaskQueue = new ConcurrentLinkedQueue();
    this.flushTaskFuture = indexBuildPoolManager.submit(flushRunTask);
    this.noMoreIndexFlushTask = false;
    /*
     * If the IndexProcessor of the corresponding storage group is not in indexMap, it means that the
     * current storage group does not build any index in memory yet and we needn't update anything.
     * The recent index information will be obtained when this IndexProcessor is loaded next time.<p>
     * For the IndexProcessor loaded in memory, we need to refresh the newest index information in the
     * start phase.
     */
    refreshSeriesIndexMapFromMManager();
  }


  public void buildIndexForOneSeries(Path path, TVList tvList) {
    // for every index of this path, submit a task to pool.
    throw new RuntimeException();

//    if
//    CopyOnReadLinkedList<T>
//    TODO
  }


  public void endFlushMemTable() throws ExecutionException, InterruptedException {
    noMoreIndexFlushTask = true;
    flushTaskFuture.get();
  }

}
