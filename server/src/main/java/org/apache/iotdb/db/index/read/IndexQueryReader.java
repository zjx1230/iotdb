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
package org.apache.iotdb.db.index.read;

import static org.apache.iotdb.db.index.read.IndexTimeRange.toFilter;
import static org.apache.iotdb.tsfile.read.filter.TimeFilter.not;
import static org.apache.iotdb.tsfile.read.filter.factory.FilterFactory.and;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.IndexQueryException;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.index.IndexUsability;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.index.read.func.IndexFuncResult;
import org.apache.iotdb.db.index.read.optimize.IndexQueryOptimize;
import org.apache.iotdb.db.index.read.optimize.NaiveOptimizer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For a query on a series, IndexQueryResource contains the required seq/unseq chunk metadata list
 * and others. In current version, all unseq TSResources are labeled as modified, so
 * IndexQuerySource use only seq index chunk to help filtering.
 *
 * Refer to org/apache/iotdb/db/query/reader/series/SeriesReader.java:608
 */
public class IndexQueryReader {

  private static final Logger logger = LoggerFactory.getLogger(IndexQueryReader.class);
  private Path seriesPath;
  private final IndexType indexType;
  private PriorityQueue<IndexChunkMeta> seqResources;
  private IndexQueryOptimize indexQueryOptimize = new NaiveOptimizer();
  private IndexUsability indexUsabilityRanger;

  /**
   * unused up to now
   */
  private PriorityQueue<IndexChunkMeta> unseqResources;

  /**
   * both-side closed range. The i-th index-usable range is {@code [range[i*2], range[i*2+1]]}
   */
  private IoTDBIndex index;
  private IndexQueryOptimize optimizer;

  public IndexQueryReader(Path seriesPath, IndexType indexType, Filter timeFilter,
      List<IndexChunkMeta> seqResources,
      List<IndexChunkMeta> unseqResources) {
    this.seriesPath = seriesPath;
    this.indexType = indexType;
    this.seqResources = new PriorityQueue<>(Comparator.comparingLong(IndexChunkMeta::getStartTime));
    this.seqResources.addAll(seqResources);
    this.unseqResources = new PriorityQueue<>(
        Comparator.comparingLong(IndexChunkMeta::getStartTime));
    this.unseqResources.addAll(unseqResources);
    this.optimizer = new NaiveOptimizer();
    this.indexUsabilityRanger = new IndexUsability(timeFilter);
  }

  /**
   * Invoke after having chunkMeta, tell reader about all condition
   */
  void initQueryCondition(Map<String, Object> queryProps,
      List<IndexFuncResult> indexFuncResults) throws UnsupportedIndexFuncException {
//    List<IndexFunc> indexFuncs = new ArrayList<>();
//    indexFuncResults.forEach(p -> indexFuncs.add(p.getIndexFunc()));
    String path = seriesPath.getFullPath();
    index = IndexType.constructQueryIndex(path, indexType, queryProps, indexFuncResults);
    index.initPreprocessor(null, true);
    indexQueryOptimize.needUnpackIndexChunk(null, 0, 0);
  }

  /**
   * For new chunk，update the IndexUsableRange
   *
   * We appreciate more readers familiar with the reading process to review this code.  So far, we
   * tend to add more if-condition and throwing exception to uncover the potential bugs.
   */
  void updateUsableRange(long[] usableRange) {
    if (usableRange.length != 2) {
      throw new UnsupportedOperationException("series reader gives me a range length > 2");
    }
    long start = usableRange[0];
    long end = usableRange[1];
    if (start > end) {
      return;
    }
    this.indexUsabilityRanger.addUsableRange(start, end);
  }

  private boolean chunkOverlapData(IndexChunkMeta indexChunkMeta, long dataStartTime,
      long dataEndTime) {
    return indexChunkMeta.getStartTime() <= dataEndTime
        && indexChunkMeta.getEndTime() >= dataStartTime;
  }

  /**
   * update the chunk
   */
  private void updateIndexChunk(long dataStartTime, long dataEndTime) {
    while (!seqResources.isEmpty()) {
      IndexChunkMeta chunkMeta = seqResources.peek();
      if (chunkMeta.getStartTime() > dataEndTime) {
        break;
      } else if (chunkMeta.getEndTime() < dataStartTime) {
        seqResources.poll();
      } else if (optimizer.needUnpackIndexChunk(indexUsabilityRanger.getIndexUsableRange(),
          chunkMeta.getStartTime(),
          chunkMeta.getEndTime())) {
        chunkMeta = seqResources.poll();
        ByteBuffer chunkData;
        try {
          chunkData = chunkMeta.unpack();
          List<Identifier> candidateList = index.queryByIndex(chunkData);
          if (candidateList != null) {
            updatePrunedRange(chunkMeta, candidateList);
          }
        } catch (IOException e) {
          logger.error("unpack chunk failed:{}, skip it", chunkMeta, e);
        } catch (IndexManagerException e) {
          logger.error("query chunk failed:{}, skip it", chunkMeta, e);
        }
      }
    }
  }

//  private void updatePrunedRange(IndexChunkMeta chunkMeta, List<Identifier> candidateList) {
//    indexPrunedRange.addRange(chunkMeta.getStartTime(), chunkMeta.getEndTime());
//    candidateList.forEach(p -> indexPrunedRange.pruneRange(p.getStartTime(), p.getEndTime()));
//  }

  /**
   * allowRange,  indexUsableRange, chunkPrunedRange
   *
   * chunkPrunedRange = chunkRange \ union(candidate list range)
   *
   * the new allowRange is:
   *
   * allowRange = allowRange  \ (chunkPrunedRange \ not(indexUsableRange))
   */
  private void updatePrunedRange(IndexChunkMeta chunkMeta, List<Identifier> candidateList) {
    Filter chunkPrunedRange = toFilter(chunkMeta.getStartTime(), chunkMeta.getEndTime());
    for (Identifier identifier : candidateList) {
      chunkPrunedRange = and(chunkPrunedRange,
          not(toFilter(identifier.getStartTime(), identifier.getEndTime())));
    }
    Filter validPruned = and(chunkPrunedRange,
        indexUsabilityRanger.getIndexUsableRange().getTimeFilter());
    Filter newAllowRange = and(indexUsabilityRanger.getAllowedRange().getTimeFilter(),
        not(validPruned));
    indexUsabilityRanger.getAllowedRange().setTimeFilter(newAllowRange);
  }


  boolean canSkipDataRange(long dataStartTime, long dataEndTime) {
    updateIndexChunk(dataStartTime, dataEndTime);
//    return indexPrunedRange.fullyContains(dataStartTime, dataEndTime);
    return !indexUsabilityRanger.getAllowedRange().intersect(dataStartTime, dataEndTime);
  }

  /**
   * A new batch of data, process them and written out.
   */
  int appendDataAndPostProcess(BatchData nextOverlappedPageData,
      List<IndexFuncResult> aggregateResultList) throws IndexQueryException {
    int reminding = Integer.MAX_VALUE;
    IndexFeatureExtractor preprocessor = index.startFlushTask(nextOverlappedPageData);
    while (reminding > 0 && preprocessor
        .hasNext(indexUsabilityRanger.getAllowedRange().getTimeFilter())) {
      preprocessor.processNext();
      reminding = index.postProcessNext(aggregateResultList);
    }
    index.endFlushTask();
    return reminding;
  }

  /**
   * release all resources.
   */
  public void release() {
    seqResources.clear();
    unseqResources.clear();
    index.closeAndRelease();
  }
}
