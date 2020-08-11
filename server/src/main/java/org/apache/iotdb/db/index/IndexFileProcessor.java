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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.index.io.IndexIOWriter;
import org.apache.iotdb.db.index.io.IndexIOWriter.IndexFlushChunk;
import org.apache.iotdb.db.index.preprocess.IndexPreprocessor;
import org.apache.iotdb.db.index.read.IndexFileResource;
import org.apache.iotdb.db.index.IndexStorageGroupProcessor.UpdateIndexFileResourcesCallBack;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.TestOnly;
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


  /**
   * previousMetaPointer is just a point of StorageGroup, thus it can be initialized by null. In
   * general, it won't be updated until the file is closed. when the file is closed, the newly
   * generated map in {@code serializeForNextOpen} will directly update the supper
   * StorageGroupProcessor (not directly replace, but insert layer by layer).  At this time, this
   * map will be updated naturally, but this indexFileProcessor will also be closed at once, so this
   * update will not affect anything.
   *
   * However, it is necessary to consider potentially very complicated and special situations, such
   * as: deleting the index, removing the index and then adding the index exactly same as the
   * previous one, without closing current index file. Will this bring about inconsistency between
   * StorageGroupProcessor and IndexFileProcessor?  We must be very cautious.
   */
  private final Map<String, Map<IndexType, ByteBuffer>> previousMetaPointer;
  private String indexFilePath;

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * path -> {Map<IndexType, IoTDBIndex>}
   */
  private Map<String, Map<IndexType, IoTDBIndex>> allPathsIndexMap;


  private IndexIOWriter writer;
  private final boolean sequence;
  private final UpdateIndexFileResourcesCallBack addResourcesCallBack;

  private static final long LONGEST_FLUSH_IO_WAIT_MS = 60000;

  private final IndexBuildTaskPoolManager indexBuildPoolManager;
  private ConcurrentLinkedQueue<IndexFlushChunk> flushTaskQueue;
  private Future flushTaskFuture;
  private boolean noMoreIndexFlushTask = false;
  private final Object waitingSymbol = new Object();
  private final long memoryThreshold;

  private final AtomicLong memoryUsed;
  private volatile boolean isFlushing;
  private AtomicInteger numIndexBuildTasks;
  private volatile boolean closed;
  private final long partitionId;

  public IndexFileProcessor(String storageGroupName, String indexFilePath, boolean sequence,
      long partitionId, Map<String, Map<IndexType, ByteBuffer>> previousMetaPointer,
      UpdateIndexFileResourcesCallBack addResourcesCallBack) {
    this.storageGroupName = storageGroupName;
    this.indexFilePath = indexFilePath;
    this.sequence = sequence;
    this.partitionId = partitionId;
    this.addResourcesCallBack = addResourcesCallBack;
    this.indexBuildPoolManager = IndexBuildTaskPoolManager.getInstance();
    this.writer = new IndexIOWriter(indexFilePath);
    memoryUsed = new AtomicLong(0);
    memoryThreshold = IoTDBDescriptor.getInstance().getConfig().getIndexBufferSize();
    numIndexBuildTasks = new AtomicInteger(0);
    isFlushing = false;
    closed = false;
    if (previousMetaPointer == null) {
      throw new IndexRuntimeException("previousMeta, maybe potential error");
    }
    this.previousMetaPointer = previousMetaPointer;
    refreshSeriesIndexMapFromMManager();
    System.out.println(allPathsIndexMap);
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
          .computeIfAbsent(path, k -> new EnumMap<>(IndexType.class));
      for (Entry<IndexType, IndexInfo> entry : pathIndexInfoMap.entrySet()) {
        IndexType indexType = entry.getKey();
        IndexInfo indexInfo = entry.getValue();
        ByteBuffer previous = previousMetaPointer.containsKey(path) ?
            previousMetaPointer.get(path).get(indexType) : null;
        if (previous != null) {
          previous = previous.duplicate();
        }
        if (!pathIndexMap.containsKey(indexType)) {
          IoTDBIndex index = IndexType.constructIndex(path, indexType, indexInfo, previous);
          pathIndexMap.putIfAbsent(indexType, index);
        }
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
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
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
            System.out
                .println(String.format("IndexFileProcessor %s: wait-close time %d ms is too long.",
                    storageGroupName, waitingTime));
          }
        }
      } else {
        lock.writeLock().lock();
        try {
          // Another flush task starts between isFlushing = true and write lock.
          if (!isFlushing) {
            IndexFileResource resource = writer.endFile();
            // store Preprocessor
            Map<String, Map<IndexType, ByteBuffer>> saved = serializeForNextOpen();
            if (resource != null) {
              addResourcesCallBack.call(sequence, partitionId, resource, saved);
            }
            closeAndRelease();
            closed = true;
          }
        } finally {
          lock.writeLock().unlock();
        }
        if (closed) {
          return;
        }
      }
    }
  }

  private void closeAndRelease() {
    allPathsIndexMap.forEach((path, pathMap) -> {
      pathMap.forEach((indexType, index) -> index.closeAndRelease());
      pathMap.clear();
    });
    allPathsIndexMap.clear();
    if (flushTaskQueue != null){
      flushTaskQueue.clear();
      flushTaskQueue = null;
    }
  }

  private Map<String, Map<IndexType, ByteBuffer>> serializeForNextOpen() throws IOException {
    Map<String, Map<IndexType, ByteBuffer>> saved = new HashMap<>(allPathsIndexMap.size());
    for (Entry<String, Map<IndexType, IoTDBIndex>> e : allPathsIndexMap.entrySet()) {
      String path = e.getKey();
      Map<IndexType, IoTDBIndex> pathMap = e.getValue();
      Map<IndexType, ByteBuffer> savedPathMap = new EnumMap<>(IndexType.class);
      for (Entry<IndexType, IoTDBIndex> entry : pathMap.entrySet()) {
        IndexType indexType = entry.getKey();
        IoTDBIndex index = entry.getValue();
        ByteBuffer output = index.serialize();
        savedPathMap.put(indexType, output);
      }
      saved.put(path, savedPathMap);
    }
    return saved;
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
    System.out.println(String.format("update %d, after: %d", memoryDelta, after));
    if (after < memoryThreshold) {
      synchronized (waitingSymbol) {
        waitingSymbol.notifyAll();
      }
    }
  }

  @SuppressWarnings("squid:S135")
  private Runnable flushRunTask = () -> {
    boolean returnWhenNoTask = false;
    long startTiming = -1;
    while (true) {
      if (noMoreIndexFlushTask) {
        returnWhenNoTask = true;
      }
      IndexFlushChunk indexFlushChunk = flushTaskQueue.poll();
      if (indexFlushChunk == null) {
        if (returnWhenNoTask && numIndexBuildTasks.get() == 0) {
          if (logger.isDebugEnabled()) {
            logger.debug("IO thread: no more, break");
          }
          System.out.println("IO thread: no more, break");
          break;
        } else if (startTiming == -1) {
          startTiming = System.currentTimeMillis();
          System.out.println(
              "IO thread: no new task, but still wait, taskNum: " + numIndexBuildTasks.get());
        } else {
          long waitingTime = System.currentTimeMillis() - startTiming;
          if (waitingTime > LONGEST_FLUSH_IO_WAIT_MS) {
            logger.error("Too long time no more task, discard the potential rest, return");
          }
        }
        try {
          Thread.sleep(200);
        } catch (@SuppressWarnings("squid:S2142") InterruptedException e) {
          logger.error("Index Flush Task is interrupted, index path {}", indexFilePath, e);
          break;
        }
      } else {
        try {
          startTiming = -1;
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
    if (allowedMemBar < 0) {
      if (logger.isErrorEnabled()) {
        logger.error(String.format("%s-%s required memory > total threshold, terminate",
            path, iotDBIndex.getIndexType()));
      }
      System.out.println(String.format("========%s-%s required memory > total threshold, terminate",
          path, iotDBIndex.getIndexType()));
      return false;
    }

    boolean hasFlushed = false;

    while (true) {
      long expectValue = memoryUsed.get();
      long targetValue = expectValue + mem;
      while (expectValue <= allowedMemBar) {
        if (memoryUsed.compareAndSet(expectValue, targetValue)) {
          // allocated successfully
          return true;
        }
        System.out.println(String.format("%s-%s require %d failed, now mem %d",
            path, iotDBIndex.getIndexType(), mem, targetValue));
        expectValue = memoryUsed.get();
        targetValue = expectValue + mem;
      }
      System.out.println(String.format("%s-%s memory full", path, iotDBIndex.getIndexType()));
      // flush and release some memory.
      if (!hasFlushed) {
        if (preprocessor.getCurrentChunkSize() > 0) {
          flushAndAddToQueue(iotDBIndex, path);
        }
        hasFlushed = true;
      } else {
        System.out.println(String.format("%s-%s have to wait", path, iotDBIndex.getIndexType()));
        // still failed, we have to wait
        synchronized (waitingSymbol) {
          try {
            System.out.println(String.format("||||||||    %s wake", path));
            waitingSymbol.wait();
          } catch (InterruptedException e) {
            logger.error("interrupted, canceled");
            return false;
          }
        }
        System.out.println(String.format("||||||||    %s wake up", path));
      }
    }
  }

  private void flushAndAddToQueue(IoTDBIndex index, Path path) {
    try {
      IndexFlushChunk indexFlushChunk = index.flush();
      flushTaskQueue.add(indexFlushChunk);
      long chunkDataSize = indexFlushChunk.getDataSize();
      long clearSize = index.clear();
      updateMemAndNotify(chunkDataSize - clearSize);
    } catch (IndexManagerException e) {
      logger.error("flush path {} errors!", path, e);
    }
  }

  public void startFlushMemTable() {
    lock.writeLock().lock();
    try {
      if (closed) {
        System.out.println("closed index file !!!!!");
        throw new IndexRuntimeException("closed index file !!!!!");
      }
      if (isFlushing) {
        throw new IndexRuntimeException("There has been a flushing, do you want to wait?");
      }
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
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void buildIndexForOneSeries(Path path, TVList tvList) {
    // for every index of this path, submit a task to pool.
    lock.writeLock().lock();
    try {
      if (!allPathsIndexMap.containsKey(path.getFullPath())) {
        return;
      }
      allPathsIndexMap.get(path.getFullPath()).forEach((indexType, index) -> {
        Runnable buildTask = () -> {
          numIndexBuildTasks.incrementAndGet();
          try {
            IndexPreprocessor preprocessor = index.startFlushTask(tvList);
            int previousOffset = Integer.MIN_VALUE;
            while (preprocessor.hasNext()) {
              int currentOffset = preprocessor.getCurrentChunkOffset();
              if (currentOffset != previousOffset) {
                if (!index.checkNeedIndex(tvList, currentOffset)) {
                  System.out.println("if (!index.checkNeedIndex(tvList, currentOffset))");
                  return;
                }
                previousOffset = currentOffset;
              }
              if (!syncAllocateSize(index.getAmortizedSize(), preprocessor, index, path)) {
                return;
              }
              preprocessor.processNext();
              index.buildNext();
            }
            System.out.println(String.format("%s-%s process all, final flush", path, indexType));
            if (preprocessor.getCurrentChunkSize() > 0) {
              flushAndAddToQueue(index, path);
            }
            index.endFlushTask();
          } catch (IndexManagerException e) {
            //Give up the following data, but the previously serialized chunk will not be affected.
            logger.error("build index failed", e);
            System.out.println("Error: build index failed" + e);
          } catch (RuntimeException e) {
            logger.error("RuntimeException", e);
            System.out.println("RuntimeException: " + e);
          } finally {
            numIndexBuildTasks.decrementAndGet();
          }
        };
        indexBuildPoolManager.submit(buildTask);
      });
    } finally {
      lock.writeLock().unlock();
    }
  }


  public void endFlushMemTable() throws ExecutionException, InterruptedException {
    noMoreIndexFlushTask = true;
    flushTaskFuture.get();
    lock.writeLock().lock();
    this.isFlushing = false;
    lock.writeLock().unlock();
  }

  @TestOnly
  public AtomicLong getMemoryUsed() {
    return memoryUsed;
  }

  @TestOnly
  public AtomicInteger getNumIndexBuildTasks() {
    return numIndexBuildTasks;
  }

  @TestOnly
  public Map<String, Map<IndexType, ByteBuffer>> getPreviousMeta() {
    return previousMetaPointer;
  }

}
