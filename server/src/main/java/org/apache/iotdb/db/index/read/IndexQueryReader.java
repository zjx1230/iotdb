package org.apache.iotdb.db.index.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.common.IndexQueryException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.UnsupportedIndexQueryException;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.IndexPreprocessor;
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


  /**
   * unused up to now
   */
  private PriorityQueue<IndexChunkMeta> unseqResources;

  /**
   * both-side closed range. The i-th index-usable range is {@code [range[i*2], range[i*2+1]]}
   */
  private IndexTimeRange indexUsableRange = new IndexTimeRange();
  private IndexTimeRange indexPrunedRange = new IndexTimeRange();
  private IoTDBIndex index;
  private IndexQueryOptimize optimizer;

  public IndexQueryReader(Path seriesPath, IndexType indexType,
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
  }

  /**
   * Invoke after having chunkMeta, tell reader about all condition
   */
  void initQueryCondition(Map<String, String> queryProps,
      List<IndexFuncResult> indexFuncResults) throws UnsupportedIndexQueryException {
//    List<IndexFunc> indexFuncs = new ArrayList<>();
//    indexFuncResults.forEach(p -> indexFuncs.add(p.getIndexFunc()));
    String path = seriesPath.getFullPath();
    index = IndexType.constructQueryIndex(path, indexType, queryProps, indexFuncResults);
    index.initPreprocessor(null);
  }

  /**
   * 对新的chunk，更新可用区间
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
    this.indexUsableRange.addRange(start, end);
  }

  private boolean chunkOverlapData(IndexChunkMeta indexChunkMeta, long dataStartTime,
      long dataEndTime) {
    return indexChunkMeta.getStartTime() <= dataEndTime
        && indexChunkMeta.getEndTime() >= dataStartTime;
  }

  /**
   * 有了新的可用区间（也可能并没有更新的），判断并加载下一段index，合并得到新的 pruneRanges 和 pruneCandidatePoints 最重要的函数之一
   */
  private void updateIndexChunk(long dataStartTime, long dataEndTime) {
    while (!seqResources.isEmpty()) {
      IndexChunkMeta chunkMeta = seqResources.peek();
      if (chunkMeta.getStartTime() > dataEndTime) {
        break;
      }
      else if (chunkMeta.getEndTime() < dataStartTime) {
        seqResources.poll();
      } else if (optimizer.needUnpackIndexChunk(indexUsableRange, chunkMeta.getStartTime(),
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

  private void updatePrunedRange(IndexChunkMeta chunkMeta, List<Identifier> candidateList) {
    indexPrunedRange.addRange(chunkMeta.getStartTime(), chunkMeta.getEndTime());
    candidateList.forEach(p -> indexPrunedRange.pruneRange(p.getStartTime(), p.getEndTime()));
  }


  boolean canSkipDataRange(long dataStartTime, long dataEndTime) {
    updateIndexChunk(dataStartTime, dataEndTime);
    return indexPrunedRange.fullyContains(dataStartTime, dataEndTime);
  }

  /**
   * 新来一批数据,处理完，写出去
   */
  int appendDataAndPostProcess(BatchData nextOverlappedPageData,
      List<IndexFuncResult> aggregateResultList, Filter timeFilter)
      throws IndexManagerException, IndexQueryException {
    int reminding = Integer.MAX_VALUE;
    IndexPreprocessor preprocessor = index.startFlushTask(nextOverlappedPageData);
    while (reminding > 0 && preprocessor.hasNext(timeFilter)) {
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
