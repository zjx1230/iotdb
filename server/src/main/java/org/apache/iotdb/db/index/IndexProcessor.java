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

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.func.IndexNaiveFunc;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.index.read.optimize.IIndexCandidateOrderOptimize;
import org.apache.iotdb.db.index.router.IIndexRouter;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 映射关系：索引序列->索引实例调度器。 "索引序列"是一个索引实例覆盖的"序列"或"序列集合"。将索引序列或索引序列集合的所有数据会写入同一个索引实例中。
 *
 * <ul>
 *   <li>在全序列检索中，索引序列是一组序列，例如对"root.steel.*.temperature"创建RTree索引，
 *       意味着将这一组序列中每一条钢的温度序列均插入RTree中，如root.steel.s1.temperature, root.steel.s2.temperature,...
 *   <li>在子序列检索中，索引序列是单条序列，例如对"root.wind.azq01.speed"创建KVMatch或ELB索引，
 *       意味着用一个滑动窗口滑过风机azq01的speed序列，并将所有子序列（或称为子片段、滑动窗口）写入索引。
 * </ul>
 *
 * 目前的设计中。一个"索引序列"允许创建多种索引。例如，"root.steel.*.temperature"上可以同时创建RTree索引和
 * DS-Tree索引，当索引序列有数据更新时，这些索引均会执行插入操作。
 *
 * <p>时间序列（TimeSeries），索引序列（IndexSeries）和索引实例（Index）的ER图为： TimeSeries (1,n) - (1,1) IndexSeries,
 * IndexSeries (1,1) - (1,n) Index
 *
 * <p>索引序列（IndexSeries）与实例调度器（IndexProcessor）一一对应
 *
 * <p>总调度器（IndexManager）、实例调度器（IndexProcessor）和索引实例（Index）的ER图为： IndexManager (1,1) - (0,n)
 * IndexProcessor, IndexProcessor (1,1) - (1,n) Index
 */
public class IndexProcessor implements Comparable<IndexProcessor> {

  private static final Logger logger = LoggerFactory.getLogger(IndexProcessor.class);

  /**
   * 索引序列
   */
  private PartialPath indexSeries;
  /**
   * 索引实例调度器的文件夹路径。
   */
  private final String indexSeriesDirPath;

  /**
   * 被索引序列数据的类型。如果索引序列IndexSeries涉及到一组时间序列（在全序列场景下），则其下的所有序列类型均需相同。
   */
  private TSDataType tsDataType;

  private final IndexBuildTaskPoolManager indexBuildPoolManager;
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  /**
   * 锁粒度：为每一个索引实例分配一个读写锁，允许对索引的并发查询，但索引的写入和查询是互斥的。这一设计有两点值得讨论：
   *
   * <p>1. 查询并发：并发查询提升了查询效率，但要求索引是查询并发安全的； 2.
   * 读写互斥：这意味着IoTDB刷写操作和索引查询之间会互相阻塞，无疑降低了效率。然而要取消这一限制，则要求索引是读写并发安全的。
   */
  private Map<IndexType, ReadWriteLock> indexLockMap;

  /**
   * 记录本IndexProcessor下，当前有多少索引正在写入数据. 如果为0，则则没有索引在写入，即当前不处于flushing状态
   */
  private AtomicInteger numIndexBuildTasks;

  /**
   * whether the processor has been closed
   */
  private volatile boolean closed;

  /**
   * 每个IndexProcessor下包含多种索引。每个IoTDBIndex是一个索引实例
   */
  private Map<IndexType, IoTDBIndex> allPathsIndexMap;

  /**
   * 对于大部分索引，需要经过 FeatureExtractor 对数据进行处理和提取特征后才能写入索引结构。 子序列索引往往使用滑动窗口
   * 来从长序列上截取子片段。两个相邻的子片段可能有固定的间隔，也可能有重叠部分。 因此，当一次索引写入操作完成后，原始数据被清除，子序列索引的 FeatureExtractor
   * 需要保存一些"过往状态量"， 以保证下一次索引写入的正确进行。状态量保存在 {@code previousDataBufferMap} 中。
   *
   * <p>全序列索引的不同写入片段间无关，因此不需要保存状态量。 值得商榷的设计。也可以考虑将这些状态量的序列化/反序列化交给索引自己来管。
   */
  private final Map<IndexType, ByteBuffer> previousDataBufferMap;

  /**
   * 过往状态量的文件名
   */
  private final String previousDataBufferFile;

  /**
   * IoTDB已经刷出到磁盘的数据可能会面临更改和删除。其中更改的一个重要原因是乱序数据：已经刷出的数据覆盖了一段区间， 而乱序数据的时间戳可能落在已刷出数据的区间内，相当于已刷出的序列（已写入索引）被更新了。
   *
   * <p>由于不能假设所有索引都支持删除和更新，因此索引框架引入了"可用区间"的概念。可用区间内数据的索引是有效的，
   * 而对于非可用区间，索引的剪枝或返回均没有保证，因此，非可用区间内的数据需要直接进入索引的"精化阶段"进行精确计算。
   *
   * <p>每个索引实例对应一个可用区间管理器，调度器下所有实例的区间管理器放置在一个文件中，称之为可用区间文件。
   */
  private Map<IndexType, IIndexUsable> usableMap;
  /**
   * 可用区间管理器的文件名
   */
  private final String usableFile;

  /**
   * 后处理阶段（精化阶段）的优化器。目前并未用到。未来设计详见{@link IIndexCandidateOrderOptimize}.
   */
  private final IIndexCandidateOrderOptimize refinePhaseOptimizer;

  public IndexProcessor(PartialPath indexSeries, String indexSeriesDirPath) {
    this.indexBuildPoolManager = IndexBuildTaskPoolManager.getInstance();

    this.numIndexBuildTasks = new AtomicInteger(0);
    this.indexSeries = indexSeries;
    this.indexSeriesDirPath = indexSeriesDirPath;
    File dir = IndexUtils.getIndexFile(indexSeriesDirPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    this.closed = false;
    this.allPathsIndexMap = new EnumMap<>(IndexType.class);
    this.previousDataBufferMap = new EnumMap<>(IndexType.class);
    this.indexLockMap = new EnumMap<>(IndexType.class);
    this.usableMap = new EnumMap<>(IndexType.class);
    this.previousDataBufferFile = indexSeriesDirPath + File.separator + "previousBuffer";
    this.usableFile = indexSeriesDirPath + File.separator + "usableMap";
    this.tsDataType = initSeriesType();
    this.refinePhaseOptimizer = IIndexCandidateOrderOptimize.Factory.getOptimize();
    deserializePreviousBuffer();
    deserializeUsable(indexSeries);
  }

  /**
   * 确定索引的数据类型。无论是针对单一序列的子序列检索，或多条序列的全序列检索，均要求类型相同。 对于全序列检索，在索引数据写入时，如果输入序列的数据类型与本IndexProcessor的类型不符，则会被忽略。
   *
   * @return tsDataType of current IndexProcessor
   */
  private TSDataType initSeriesType() {
    try {
      if (indexSeries.isFullPath()) {
        return MManager.getInstance().getSeriesType(indexSeries);
      } else {
        List<PartialPath> list =
            IoTDB.metaManager.getAllTimeseriesPathWithAlias(indexSeries, 1, 0).left;
        if (list.isEmpty()) {
          throw new IndexRuntimeException("No series in the wildcard path");
        } else {
          return MManager.getInstance().getSeriesType(list.get(0));
        }
      }
    } catch (MetadataException e) {
      throw new IndexRuntimeException("get type failed. ", e);
    }
  }

  private String getIndexDir(IndexType indexType) {
    return indexSeriesDirPath + File.separator + indexType;
  }

  private void serializeUsable() {
    File file = SystemFileFactory.INSTANCE.getFile(usableFile);
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ReadWriteIOUtils.write(usableMap.size(), outputStream);
      for (Entry<IndexType, IIndexUsable> entry : usableMap.entrySet()) {
        IndexType indexType = entry.getKey();
        ReadWriteIOUtils.write(indexType.serialize(), outputStream);
        IIndexUsable v = entry.getValue();
        v.serialize(outputStream);
      }
    } catch (IOException e) {
      logger.error("Error when serialize usability. Given up.", e);
    }
  }

  private void serializePreviousBuffer() {
    File file = SystemFileFactory.INSTANCE.getFile(previousDataBufferFile);
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ReadWriteIOUtils.write(previousDataBufferMap.size(), outputStream);
      for (Entry<IndexType, ByteBuffer> entry : previousDataBufferMap.entrySet()) {
        IndexType indexType = entry.getKey();
        ByteBuffer buffer = entry.getValue();
        ReadWriteIOUtils.write(indexType.serialize(), outputStream);
        ReadWriteIOUtils.write(buffer, outputStream);
      }
    } catch (IOException e) {
      logger.error("Error when serialize previous buffer. Given up.", e);
    }
  }

  private void deserializePreviousBuffer() {
    File file = SystemFileFactory.INSTANCE.getFile(previousDataBufferFile);
    if (!file.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(file)) {
      int size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; i++) {
        IndexType indexType = IndexType.deserialize(ReadWriteIOUtils.readShort(inputStream));
        ByteBuffer byteBuffer =
            ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(inputStream);
        previousDataBufferMap.put(indexType, byteBuffer);
      }
    } catch (IOException e) {
      logger.error("Error when deserialize previous buffer. Given up.", e);
    }
  }

  private void deserializeUsable(PartialPath indexSeries) {
    File file = SystemFileFactory.INSTANCE.getFile(usableFile);
    if (!file.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(file)) {
      int size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; i++) {
        short indexTypeShort = ReadWriteIOUtils.readShort(inputStream);
        IndexType indexType = IndexType.deserialize(indexTypeShort);
        IIndexUsable usable =
            IIndexUsable.Factory.deserializeIndexUsability(indexSeries, inputStream);
        usableMap.put(indexType, usable);
      }
    } catch (IOException | IllegalPathException e) {
      logger.error("Error when deserialize usability. Given up.", e);
    }
  }

  /**
   * 将除了NoIndex之外的所有索引实例刷出磁盘
   */
  @SuppressWarnings("squid:S2589")
  public synchronized void close(boolean deleteFiles) throws IOException {
    if (closed) {
      return;
    }
    waitingFlushEndAndDo(
        () -> {
          lock.writeLock().lock();
          try {
            // store Preprocessor
            for (Entry<IndexType, IoTDBIndex> entry : allPathsIndexMap.entrySet()) {
              IndexType indexType = entry.getKey();
              if (indexType == IndexType.NO_INDEX) {
                continue;
              }
              IoTDBIndex index = entry.getValue();
              previousDataBufferMap.put(entry.getKey(), index.closeAndRelease());
            }
            logger.info("close and release index processor: {}", indexSeries);
            allPathsIndexMap.clear();
            serializeUsable();
            serializePreviousBuffer();
            closed = true;
            if (deleteFiles) {
              File dir = IndexUtils.getIndexFile(indexSeriesDirPath);
              dir.delete();
            }
          } finally {
            lock.writeLock().unlock();
          }
        });
  }

  private void waitingFlushEndAndDo(IndexNaiveFunc indexNaiveAction) throws IOException {
    // wait the flushing end.
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
            logger.warn(
                String.format(
                    "IndexFileProcessor %s: wait-close time %d ms is too long.",
                    indexSeries, waitingTime));
          }
        }
      } else {
        indexNaiveAction.act();
        break;
      }
    }
  }

  public PartialPath getIndexSeries() {
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
    return indexSeries + ": " + allPathsIndexMap;
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
        throw new IndexRuntimeException("closed index file !!!!!");
      }
      if (isFlushing()) {
        throw new IndexRuntimeException("There has been a flushing, do you want to wait?");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * 将已排序的序列写入所有索引中。该序列需要与 {@code indexSeries} 相匹配。path是否与indexSeries匹配参见 {@linkplain
   * IndexMemTableFlushTask#buildIndexForOneSeries(PartialPath, TVList)}
   *
   * @param path 路径
   * @param tvList 需要已排序
   */
  public void buildIndexForOneSeries(PartialPath path, TVList tvList) {
    // for every index of this path, submit a task to pool.
    lock.writeLock().lock();
    numIndexBuildTasks.incrementAndGet();
    try {
      if (tvList.getDataType() != tsDataType) {
        logger.warn(
            "TsDataType unmatched, ignore: indexSeries {}: {}, given series {}: {}",
            indexSeries,
            tsDataType,
            path,
            tvList.getDataType());
      }
      allPathsIndexMap.forEach(
          (indexType, index) -> {
            // NO_INDEX doesn't involve the phase of building index
            if (indexType == IndexType.NO_INDEX) {
              numIndexBuildTasks.decrementAndGet();
              return;
            }
            Runnable buildTask =
                () -> {
                  try {
                    indexLockMap.get(indexType).writeLock().lock();
                    IndexFeatureExtractor extractor = index.startFlushTask(path, tvList);
                    while (extractor.hasNext()) {
                      extractor.processNext();
                      index.buildNext();
                    }
                    index.endFlushTask();
                    // we don't update usable ranges. It's a pretty critical decision.
                    //            this.usableMap.get(indexType)
                    //                .addUsableRange(path, tvList.getMinTime(),
                    // tvList.getLastTime());
                  } catch (IndexManagerException e) {
                    // Give up the following data, but the previously serialized chunk will not be
                    // affected.
                    logger.error("build index failed", e);
                  } catch (RuntimeException e) {
                    logger.error("RuntimeException", e);
                  } finally {
                    numIndexBuildTasks.decrementAndGet();
                    indexLockMap.get(indexType).writeLock().unlock();
                  }
                };
            indexBuildPoolManager.submit(buildTask);
          });
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * 需要等待索引全部刷写完才会返回。
   */
  public void endFlushMemTable() {
    // wait until all flushing tasks end.
    try {
      waitingFlushEndAndDo(() -> {
      });
    } catch (IOException ignored) {
    }
  }

  /**
   * 根据传入的 {@code indexInfoMap} 刷新当前IndexProcessor中的索引实例。在 IndexProcessor 创建之后，
   * 用户进行创建或删除索引，则IndexProcessor中维护的索引实例就是过期的，因此需要刷新。
   *
   * <p>刷新的时刻包括： 1. IndexProcessor创建时，此时是对 IndexProcessor 的初始化 2. 索引刷写时 {@code startFlushMemTable}
   * 。
   *
   * @param indexInfoMap 从 {@link IIndexRouter} 中传入。
   */
  public void refreshSeriesIndexMapFromMManager(Map<IndexType, IndexInfo> indexInfoMap) {
    lock.writeLock().lock();
    try {
      // Add indexes that are not in the previous map
      for (Entry<IndexType, IndexInfo> entry : indexInfoMap.entrySet()) {
        IndexType indexType = entry.getKey();
        IndexInfo indexInfo = entry.getValue();
        if (!allPathsIndexMap.containsKey(indexType)) {
          IoTDBIndex index =
              IndexType.constructIndex(
                  indexSeries,
                  tsDataType,
                  getIndexDir(indexType),
                  indexType,
                  indexInfo,
                  previousDataBufferMap.get(indexType));
          allPathsIndexMap.putIfAbsent(indexType, index);
          indexLockMap.putIfAbsent(indexType, new ReentrantReadWriteLock());
          usableMap.putIfAbsent(
              indexType, IIndexUsable.Factory.createEmptyIndexUsability(indexSeries));
        }
      }

      // remove indexes that are removed from the previous map
      for (IndexType indexType : new ArrayList<>(allPathsIndexMap.keySet())) {
        if (!indexInfoMap.containsKey(indexType)) {
          try {
            allPathsIndexMap.get(indexType).closeAndRelease();
          } catch (IOException e) {
            logger.warn("Meet error when close {} before removing index, {}", indexType,
                e.getMessage());
          }
          // remove index file directories
          File dir = IndexUtils.getIndexFile(getIndexDir(indexType));
          dir.delete();
          allPathsIndexMap.remove(indexType);
          usableMap.remove(indexType);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @TestOnly
  public AtomicInteger getNumIndexBuildTasks() {
    return numIndexBuildTasks;
  }

  /**
   * 对于乱序数据，目前的设计中直接将数据标记为"索引不可用"
   */
  void updateUnsequenceData(PartialPath path, TVList tvList) {
    this.usableMap.forEach(
        (indexType, usable) ->
            usable.minusUsableRange(path, tvList.getMinTime(), tvList.getLastTime()));
  }

  public QueryDataSet query(
      IndexType indexType,
      Map<String, Object> queryProps,
      QueryContext context,
      boolean alignedByTime)
      throws QueryIndexException {
    try {
      lock.readLock().lock();
      try {
        if (!indexLockMap.containsKey(indexType)) {
          throw new QueryIndexException(
              String.format(
                  "%s hasn't been built on %s", indexType.toString(), indexSeries.getFullPath()));
        } else {
          indexLockMap.get(indexType).readLock().lock();
        }
      } finally {
        lock.readLock().unlock();
      }
      IoTDBIndex index = allPathsIndexMap.get(indexType);
      return index.query(
          queryProps, this.usableMap.get(indexType), context, refinePhaseOptimizer, alignedByTime);

    } finally {
      indexLockMap.get(indexType).readLock().unlock();
    }
  }
}
