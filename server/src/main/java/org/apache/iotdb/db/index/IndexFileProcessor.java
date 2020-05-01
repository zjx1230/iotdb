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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.flush.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.index.io.IndexIOWriter;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexFlushChunk;
import org.apache.iotdb.db.index.preprocess.IndexPreprocessor;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage insert ahead logs of a TsFile.
 */
public class IndexFileProcessor implements Comparable<IndexFileProcessor> {

  private static final Logger logger = LoggerFactory.getLogger(IndexFileProcessor.class);
  private final String storageGroupName;
  private String indexFilePath;

  private String indexParentDir;

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * path -> {Map<IndexType, IoTDBIndex>}
   */
  private Map<String, Map<IndexType, IoTDBIndex>> allPathsIndexMap;


  private IndexIOWriter writer;
  private final boolean sequence;


  private final IndexBuildTaskPoolManager indexBuildPoolManager;
  private ConcurrentLinkedQueue<IndexFlushChunk> flushTaskQueue;
  private Future flushTaskFuture;
  private boolean noMoreIndexFlushTask = false;
  private final Object waitingSymbol = new Object();
  private static long memoryThreshold = IoTDBDescriptor.getInstance().getConfig()
      .getIndexBufferSize();
  private final AtomicLong memoryUsed;
  private boolean isFlushing;

  public IndexFileProcessor(String storageGroupName, String indexParentDir, String indexFilePath,
      boolean sequence) {
    this.storageGroupName = storageGroupName;
    this.indexParentDir = indexParentDir;
    this.indexFilePath = indexFilePath;
    this.sequence = sequence;
    this.indexBuildPoolManager = IndexBuildTaskPoolManager.getInstance();
    this.writer = new IndexIOWriter(indexFilePath);
    memoryUsed = new AtomicLong(0);
    isFlushing = false;
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
      if (pathIndexMap == null) {
        pathIndexMap = allPathsIndexMap.get(path);
      }
      for (Entry<IndexType, IndexInfo> entry : pathIndexInfoMap.entrySet()) {
        IndexType indexType = entry.getKey();
        IndexInfo indexInfo = entry.getValue();
        pathIndexMap.putIfAbsent(indexType, IndexType.constructIndex(path, indexType, indexInfo));
      }
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
  @SuppressWarnings("squid:S2589")
  public void close() throws IOException {
    //wait the flushing end.
    long waitingTime;
    long waitingInterval = 100;
    long st = System.currentTimeMillis();
    while (true) {
      if (isFlushing) {
        try {
          Thread.sleep(waitingInterval);
        } catch (InterruptedException e) {
          logger.error("interrupted, index file may not complete.", e);
          return;
        }
        waitingTime = System.currentTimeMillis() - st;
        // wait for too long time.
        if (waitingTime > 3000) {
          waitingInterval = 1000;
          if (logger.isWarnEnabled()) {
            logger.warn(String.format("IndexFileProcessor %s: wait-close time %d ms is too long.",
                storageGroupName, waitingTime));
          }
        }
      } else {
        lock.writeLock().lock();
        // Another flush task starts between isFlushing = true and write lock.
        if (!isFlushing) {
          writer.endFile();
          return;
        }
        lock.writeLock().unlock();
      }
    }
  }


  public String getIndexFilePath() {
    return indexFilePath;
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

  private void updateMemAndNotify(long memoryDelta) {
    long after = memoryUsed.addAndGet(memoryDelta);
    if (after < memoryThreshold) {
      synchronized (waitingSymbol) {
        waitingSymbol.notifyAll();
      }
    }
  }

  @SuppressWarnings("squid:S135")
  private Runnable flushRunTask = () -> {
    boolean returnWhenNoTask = false;
    while (true) {
      if (noMoreIndexFlushTask) {
        returnWhenNoTask = true;
      }
      IndexFlushChunk indexFlushChunk = flushTaskQueue.poll();
      if (indexFlushChunk == null) {
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
          writer.writeIndexData(indexFlushChunk);
          // we can release the memory of indexFlushChunk
          updateMemAndNotify(-indexFlushChunk.getDataSize());
        } catch (@SuppressWarnings("squid:S2139") IOException e) {
          logger.error("Index Flush Task meet IO error, index path: {}", indexFilePath, e);
          throw new FlushRunTimeException(e);
        }
      }
    }
  };

  /**
   * <p>{@linkplain IoTDBIndex} requests memory allocation for processing one point. AtomicLong is
   * used to update the current remaining memory.  If the memory threshold is reached, trigger
   * {@linkplain IoTDBIndex#flush}, construct byte arrays to {@linkplain #flushRunTask}, release the
   * memory occupied during {@linkplain IoTDBIndex#buildNext}, and request the memory allocation
   * again.</p>
   *
   * <p>If it's still unavailable, {@code wait} until {@linkplain #flushRunTask} finishes someone
   * task, releases memory and calls {@code notifyAll}. If it is available, return and call {@code
   * notifyAll} to wake up other waiting index-building threads.</p>
   *
   * @return true if allocate successfully.
   */
  private boolean syncAllocateSize(int mem, IndexPreprocessor preprocessor, IoTDBIndex iotDBIndex,
      Path path) {
    long allowedMemBar = memoryThreshold - mem;
    boolean hasFlushed = false;

    while (true) {
      long expectValue;
      long targetValue;
      do {
        expectValue = memoryUsed.get();
        targetValue = expectValue + mem;
        if (memoryUsed.compareAndSet(expectValue, targetValue)) {
          // allocated successfully
          return true;
        }
      } while (expectValue <= allowedMemBar);

      // flush and release some memory.
      if (!hasFlushed) {
        flushAndAddToQueue(iotDBIndex, path);
        hasFlushed = true;
      } else {
        // still failed, we have to wait
        synchronized (waitingSymbol) {
          try {
            waitingSymbol.wait();
          } catch (InterruptedException e) {
            logger.error("interrupted, canceled");
            return false;
          }
        }
      }
    }
  }

  private void flushAndAddToQueue(IoTDBIndex index, Path path) {
    try {
      IndexFlushChunk indexFlushChunk = index.flush();
      flushTaskQueue.add(indexFlushChunk);
      long memoryDelta = indexFlushChunk.getDataSize();
      memoryDelta -= index.clear();
      updateMemAndNotify(memoryDelta);
    } catch (IndexManagerException e) {
      logger.error("flush path {} errors!", path, e);
    }
  }

  public void startFlushMemTable() {
    lock.writeLock().lock();
    this.isFlushing = true;
    this.flushTaskQueue = new ConcurrentLinkedQueue<>();
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
    lock.writeLock().unlock();
  }

  public void buildIndexForOneSeries(Path path, TVList tvList) {
    // for every index of this path, submit a task to pool.
    if (!allPathsIndexMap.containsKey(path.getFullPath())) {
      return;
    }
    allPathsIndexMap.get(path.getFullPath()).forEach((indexType, index) -> {
      Runnable buildTask = () -> {
        IndexPreprocessor preprocessor = index.initIndexPreprocessor(tvList);
        int previousOffset = Integer.MIN_VALUE;
        while (preprocessor.hasNext()) {
          int currentOffset = preprocessor.getCurrentOffset();
          if (currentOffset != previousOffset && !index.checkNeedIndex(tvList, currentOffset)) {
            return;
          }
          if (!syncAllocateSize(index.getAmortizedSize(), preprocessor, index, path)) {
            return;
          }
          preprocessor.processNext();
          try {
            index.buildNext();
          } catch (IndexManagerException e) {
            //Give up the following data, but the previously serialized chunk will not be affected.
            logger.error("build index failed", e);
            return;
          }
        }
        flushAndAddToQueue(index, path);
      };
      indexBuildPoolManager.submit(buildTask);
    });
  }


  public void endFlushMemTable() throws ExecutionException, InterruptedException {
    lock.writeLock().lock();
    this.isFlushing = false;
    noMoreIndexFlushTask = true;
    flushTaskFuture.get();
    lock.writeLock().unlock();
  }
}
