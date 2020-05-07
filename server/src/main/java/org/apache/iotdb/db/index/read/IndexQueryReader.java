package org.apache.iotdb.db.index.read;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.common.IndexQueryException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.io.IndexChunkMeta;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * For a query on a series, IndexQueryResource contains the required seq/unseq chunk metadata list
 * and others. In current version, all unseq TSResources are labeled as modified, so
 * IndexQuerySource use only seq index chunk to help filtering.
 *
 * Refer to org/apache/iotdb/db/query/reader/series/SeriesReader.java:608
 */
public class IndexQueryReader {

  private Path seriesPath;
  private final IndexType indexType;
  private List<IndexChunkMeta> seqResources;
  private List<IndexChunkMeta> unseqResources;

  /**
   * both-side closed range. The i-th index-usable range is {@code [range[i*2], range[i*2+1]]}
   */
  private TimeRange usableRange = new TimeRange();
  //  private Map<String, String> queryProps;
  private List<IndexFunc> indexFuncs;
  private IoTDBIndex index;

  public IndexQueryReader(Path seriesPath, IndexType indexType,
      List<IndexChunkMeta> seqResources,
      List<IndexChunkMeta> unseqResources) {
    this.seriesPath = seriesPath;
    this.indexType = indexType;
    this.seqResources = seqResources;
    this.unseqResources = unseqResources;
  }

  /**
   * 获得chunkmeta之后调用，告诉reader，这次查询的所有条件，以及查询的所有indexFunc
   */
  public void initQuery(Map<String, String> queryProps, List<IndexFunc> indexFuncs) {
    this.indexFuncs = indexFuncs;
    this.index = IndexType
        .constructQueryIndex(seriesPath.getFullPath(), indexType, queryProps, indexFuncs);
  }

  /**
   * 对新的chunk，更新可用区间
   *
   * We appreciate more readers familiar with the reading process to review this code.  So far, we
   * tend to add more if-condition and throwing exception to uncover the potential bugs.
   */
  void updateUsableRange(long[] usableRange) throws IndexQueryException {
    this.usableRange.appendRange(usableRange);
    updateIndexChunk();
  }

  /**
   * 有了新的可用区间（也可能并没有更新的），判断并加载下一段index，合并得到新的 pruneRanges 和 pruneCandidatePoints
   */
  private void updateIndexChunk() {
    // TODO
  }

  public boolean canSkipCurrentChunk(Statistics currentChunkStatistics) {
    //TODO
    throw new UnsupportedOperationException();
  }

  public boolean canSkipCurrentPage(Statistics pageStatistics) {
    //TODO
    throw new UnsupportedOperationException();
  }

  /**
   * 判断某个page或者chunk可以被移除是非常严格的，当某段被skip时，Previous肯定是不需要再用了，所以preprocessor清理掉
   */
  public void clearPrevious() {
    //TODO
    throw new UnsupportedOperationException();
  }

  /**
   * 新来一批数据,处理完，写出去
   */
  public void appendDataAndPostProcess(BatchData nextOverlappedPageData,
      List<AggregateResult> aggregateResultList, boolean[] isCalculatedArray) {
    //TODO
    throw new UnsupportedOperationException();
  }

  /**
   * 这个函数被调用的时候说明所有的数据都处理完了，把数据输出去+
   */
  public void outputIndexResult(List<AggregateResult> aggregateResultList) {
    //TODO
    throw new UnsupportedOperationException();
  }


}
