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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.io.IndexBuildTaskPoolManager;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexProcessor implements Comparable<IndexProcessor> {

  private static final Logger logger = LoggerFactory.getLogger(IndexProcessor.class);
//  private final String storageGroupName;


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
//  private final Map<IndexType, ByteBuffer> previousMetaPointer;
  private final String indexSeriesDirPath;
  private String indexSeries;
  private final IndexBuildTaskPoolManager indexBuildPoolManager;
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * path -> {Map<IndexType, IoTDBIndex>}
   */

//  private IndexIOWriter writer;
//  private final boolean sequence;
//  private final UpdateIndexFileResourcesCallBack addResourcesCallBack;

//  private static final long LONGEST_FLUSH_IO_WAIT_MS = 60000;

//  private boolean noMoreIndexFlushTask = false;
//  private final Object waitingSymbol = new Object();
//  private final long memoryThreshold;

//  private final AtomicLong memoryUsed;
  //  private volatile boolean isFlushing;
//  private final long partitionId;

  /**
   * we use numIndexBuildTasks to record how many indexes are building. If it's 0, there is no
   * flushing.
   */
  private AtomicInteger numIndexBuildTasks;
  private volatile boolean closed;
  private Map<IndexType, IoTDBIndex> allPathsIndexMap;
  private final IIndexUsable indexUsable;

  public IndexProcessor(String indexSeries, String indexSeriesDirPath) {
//    this.storageGroupName = storageGroupName;

//    this.sequence = sequence;
//    this.partitionId = partitionId;
//    this.addResourcesCallBack = addResourcesCallBack;
    this.indexBuildPoolManager = IndexBuildTaskPoolManager.getInstance();
//    this.writer = new IndexIOWriter(indexFilePath);
//    memoryUsed = new AtomicLong(0);
//    memoryThreshold = IoTDBDescriptor.getInstance().getConfig().getIndexBufferSize();
//    isFlushing = false;

    this.numIndexBuildTasks = new AtomicInteger(0);
    this.indexSeries = indexSeries;
    this.indexSeriesDirPath = indexSeriesDirPath;
    this.closed = false;
    this.indexUsable = deserialize();
//    if (previousMetaPointer == null) {
//      throw new IndexRuntimeException("previousMeta, maybe potential error");
//    }
//    this.previousMetaPointer = previousMetaPointer;
    this.allPathsIndexMap = new HashMap<>();
  }

  private IIndexUsable deserialize() {
    // TODO 把可用区间读进来
    throw new UnsupportedOperationException();
  }

  private void serialize() {
    // TODO 把可用区间信息刷出去
    IndexUtils.breakDown();
  }

  /**
   * seal the index file, move "indexing" to "index"
   */
  @SuppressWarnings("squid:S2589")
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    waitingFlushEndAndDo(() -> {
      lock.writeLock().lock();
      try {
        // store Preprocessor
        for (Entry<IndexType, IoTDBIndex> entry : allPathsIndexMap.entrySet()) {
          IoTDBIndex index = entry.getValue();
          index.serialize();
        }
        closeAndRelease();
        closed = true;
      } finally {
        lock.writeLock().unlock();
      }
    });
  }

  private void waitingFlushEndAndDo(DoSomething doSomething) throws IOException {
    //wait the flushing end.
    long waitingTime;
    long waitingInterval = 100;
    long st = System.currentTimeMillis();
    while (true) {
      if (isFlushing()) {
        try {
          Thread.sleep(waitingInterval);
        } catch (InterruptedException e) {
          logger.error("interrupted, index insert may not complete.", e);
          return;
        }
        waitingTime = System.currentTimeMillis() - st;
        // wait for too long time.
        if (waitingTime > 3000) {
          waitingInterval = 1000;
          if (logger.isWarnEnabled()) {
            logger.warn(String.format("IndexFileProcessor %s: wait-close time %d ms is too long.",
                indexSeries, waitingTime));
            System.out
                .println(String.format("IndexFileProcessor %s: wait-close time %d ms is too long.",
                    indexSeries, waitingTime));
          }
        }
      } else {
        doSomething.act();
        break;
      }
    }
  }

  private void closeAndRelease() {
    allPathsIndexMap.forEach((indexType, index) -> index.closeAndRelease());
    allPathsIndexMap.clear();
    serialize();
  }

  public synchronized void deleteAllFiles() throws IOException {
    logger.info("Start deleting all files in index processor {}", indexSeries);
    close();
    // delete all index files in this dir.
    File indexSeriesDirFile = new File(indexSeriesDirPath);
    if (indexSeriesDirFile.exists()) {
      FileUtils.deleteDirectory(indexSeriesDirFile);
    }
    closeAndRelease();
  }

  public String getIndexSeries() {
    return indexSeries;
  }

  @Override
  public int hashCode() {
    return indexSeries.hashCode();
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

    return compareTo((IndexProcessor) obj) == 0;
  }

  @Override
  public String toString() {
    return "Index File: " + indexSeries;
  }

  @Override
  public int compareTo(IndexProcessor o) {
    return indexSeries.compareTo(o.indexSeries);
  }

  private boolean isFlushing() {
    return numIndexBuildTasks.get() > 0;
  }

  public void startFlushMemTable() {
    lock.writeLock().lock();
    try {
      if (closed) {
//        System.out.println("closed index file !!!!!");
        throw new IndexRuntimeException("closed index file !!!!!");
      }
      if (isFlushing()) {
        throw new IndexRuntimeException("There has been a flushing, do you want to wait?");
      }
//      this.isFlushing = true;
//      this.flushTaskQueue = new ConcurrentLinkedQueue<>();
//      this.flushTaskFuture = indexBuildPoolManager.submit(flushRunTask);
//      this.noMoreIndexFlushTask = false;
      /*
       * If the IndexProcessor of the corresponding storage group is not in indexMap, it means that the
       * current storage group does not build any index in memory yet and we needn't update anything.
       * The recent index information will be obtained when this IndexProcessor is loaded next time.<p>
       * For the IndexProcessor loaded in memory, we need to refresh the newest index information in the
       * start phase.
       */
//      refreshSeriesIndexMapFromMManager();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void buildIndexForOneSeries(PartialPath path, TVList tvList) {
    // for every index of this path, submit a task to pool.
    lock.writeLock().lock();
    numIndexBuildTasks.incrementAndGet();
    try {
      allPathsIndexMap.forEach((indexType, index) -> {
        Runnable buildTask = () -> {
          try {
            IndexFeatureExtractor extractor = index.startFlushTask(tvList);
            int previousOffset = Integer.MIN_VALUE;
            while (extractor.hasNext()) {
              int currentOffset = extractor.getCurrentChunkOffset();
              if (currentOffset != previousOffset) {
                if (!index.checkNeedIndex(tvList, currentOffset)) {
                  System.out.println("if (!index.checkNeedIndex(tvList, currentOffset))");
                  return;
                }
                previousOffset = currentOffset;
              }
//              if (!syncAllocateSize(index.getAmortizedSize(), extractor, index, path)) {
//                return;
//              }
              extractor.processNext();
              index.buildNext();
            }
            System.out
                .println(String.format("%s-%s process all, final flush", indexSeries, indexType));
            if (extractor.getCurrentChunkSize() > 0) {
              index.flush();
            }
            index.endFlushTask();
            this.indexUsable.addUsableRange(path, tvList);
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


  public void endFlushMemTable() {
    // wait until all flushing tasks end.
    try {
      waitingFlushEndAndDo(() -> {
      });
    } catch (IOException ignored) {
    }
  }

//  @TestOnly
//  public AtomicLong getMemoryUsed() {
//    return memoryUsed;
//  }

  @TestOnly
  public AtomicInteger getNumIndexBuildTasks() {
    return numIndexBuildTasks;
  }

  public void updateUnsequenceData(PartialPath path, TVList tvList) {
    this.indexUsable.minusUsableRange(path, tvList);
  }

//  @TestOnly
//  public Map<String, Map<IndexType, ByteBuffer>> getPreviousMeta() {
//    return previousMetaPointer;
//  }

  /**
   * Do something without input and output.
   *
   * <p>This is a <a href="package-summary.html">functional interface</a>
   * whose functional method is {@link #act()}.
   */
  @FunctionalInterface
  private interface DoSomething {

    /**
     * Do something.
     */
    void act() throws IOException;
  }

}
