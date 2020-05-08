package org.apache.iotdb.db.index.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexManagerException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class IndexAggregationExecutor extends AggregationExecutor {

  private final Map<String, String> queryProps;
  private final IndexType indexType;

  public IndexAggregationExecutor(
      QueryIndexPlan queryIndexPlan) {
    super(queryIndexPlan);
    indexType = queryIndexPlan.getIndexType();
    queryProps = queryIndexPlan.getProps();
  }

  /**
   * Compared to super method, it create IndexAggregateResult instead of Other AggregateResult
   *
   * @param pathIdx the idx of path
   * @param tsDataType path type
   */
  /**
   * get aggregation result for one series
   *
   * @param pathToAggrIndexes entry of path to aggregation indexes map
   * @param timeFilter time filter
   * @param context query context
   * @return AggregateResult list
   */
  @Override
  protected List<AggregateResult> aggregateOneSeries(
      Map.Entry<Path, List<Integer>> pathToAggrIndexes,
      Set<String> measurements, Filter timeFilter, QueryContext context)
      throws IOException, QueryProcessException, StorageEngineException {
    List<IndexFuncResult> indexFuncResults = new ArrayList<>();
    List<IndexFunc> indexFuncs = new ArrayList<>();

    Path seriesPath = pathToAggrIndexes.getKey();
    TSDataType tsDataType = dataTypes.get(pathToAggrIndexes.getValue().get(0));

    for (int i : pathToAggrIndexes.getValue()) {
      // construct AggregateResult
      IndexFunc indexFunc = IndexFunc.getIndexFunc(aggregations.get(i));
      IndexFuncResult aggregateResult = new IndexFuncResult(indexFunc, tsDataType, indexType);
      indexFuncResults.add(aggregateResult);
      indexFuncs.add(indexFunc);
    }
    try {
      aggregateOneSeriesIndex(seriesPath, measurements, context, timeFilter, tsDataType,
          indexFuncResults, indexFuncs);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }

    List<AggregateResult> res = new ArrayList<>(indexFuncResults.size());
    res.addAll(indexFuncResults);
    return res;
  }

  /**
   * Override {@linkplain AggregationExecutor#aggregateOneSeries}. Besides {@linkplain
   * QueryDataSource}, we also load the index chunk metadata.
   */
  private void aggregateOneSeriesIndex(Path seriesPath, Set<String> measurements,
      QueryContext context, Filter timeFilter, TSDataType tsDataType,
      List<IndexFuncResult> indexFuncResults, List<IndexFunc> indexFuncs)
      throws StorageEngineException, IOException, QueryProcessException, MetadataException {

    // construct series reader without value filter
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context, timeFilter);
    IndexQueryReader indexQueryReader = IndexManager.getInstance()
        .getQuerySource(seriesPath, indexType);
    indexQueryReader.initQueryCondition(queryProps, indexFuncs);
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    SeriesAggregateReader seriesReader = new SeriesAggregateReader(seriesPath,
        measurements,
        tsDataType, context, queryDataSource, timeFilter, null, null);
    aggregateFromIndexReader(seriesReader, indexQueryReader, indexFuncResults, timeFilter);
    indexQueryReader.release();
  }

  /**
   * <p>Override {@linkplain AggregationExecutor}#aggregateFromReader.</p>
   *
   * <p>In the index query, we utilize the information querying from {@code QueryDataSource} about
   * the overlap and the time order between different files/chunks.  We adopt the two-phase method
   * widely used in database indexing, namely pruning phase and post-processing phase.</p>
   *
   * <p><tt>The pruning phase</tt>.  According to whether a file/chunk has been modified,
   * overlapped with others and fully contained by the given time filtering, we can determine if we
   * can use the built index chunk to prune it.  In the majority of cases, the time range pruned by
   * the index must have no correct result, i.e., no-false-dismissals.</p>
   *
   * <p><tt>The post-processing phase</tt>. After the index pruning, we discard the time range
   * pruned by the index and sequentially scan the rest time range within the time filter, calculate
   * all rest candidates and obtain exact results.</p>
   */
  private void aggregateFromIndexReader(SeriesAggregateReader seriesReader,
      IndexQueryReader indexQueryReader, List<IndexFuncResult> indexFuncResults,
      Filter timeFilter)
      throws QueryProcessException, IOException {
    int remainingToCalculate = indexFuncResults.size();
    boolean[] isCalculatedArray = new boolean[indexFuncResults.size()];
    while (seriesReader.hasNextFile()) {
      // cal by file statistics
      // Compared the super method, we remove the branch of canUseCurrentFileStatistics
      // since indexes are built on chunk or partial chunk.
      while (seriesReader.hasNextChunk()) {
        indexQueryReader.updateUsableRange(seriesReader.getUnOverlappedInCurrentChunk());
        Statistics chunkStatistics = seriesReader.currentChunkStatistics();
        // Index is no-false-negative, but not no-false-positive
        if (indexQueryReader
            .canSkipDataRange(chunkStatistics.getStartTime(), chunkStatistics.getEndTime())) {
          seriesReader.skipCurrentChunk();
          continue;
        }
        remainingToCalculate = aggregateIndexOverlappedPages(seriesReader, indexQueryReader,
            indexFuncResults, isCalculatedArray, remainingToCalculate, timeFilter);
        if (remainingToCalculate == 0) {
          return;
        }
      }
    }
  }

  /**
   * 后处理函数
   */
  private int aggregateIndexOverlappedPages(SeriesAggregateReader seriesReader,
      IndexQueryReader indexQueryReader, List<IndexFuncResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate, Filter timeFilter)
      throws IOException, IndexManagerException {
    // cal by page data
    int newRemainingToCalculate = remainingToCalculate;
    while (seriesReader.hasNextPage()) {
      Statistics pageStatistics = seriesReader.currentPageStatistics();
      // Index is no-false-negative, but not no-false-positive
      if (indexQueryReader
          .canSkipDataRange(pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
        seriesReader.skipCurrentPage();
        continue;
      }
      BatchData nextOverlappedPageData = seriesReader.nextPage();
      indexQueryReader
          .appendDataAndPostProcess(nextOverlappedPageData, aggregateResultList,
              isCalculatedArray, timeFilter);
      for (int i = 0; i < aggregateResultList.size(); i++) {
        if (!isCalculatedArray[i]) {
          AggregateResult aggregateResult = aggregateResultList.get(i);
          if (aggregateResult.isCalculatedAggregationResult()) {
            isCalculatedArray[i] = true;
            newRemainingToCalculate--;
            if (newRemainingToCalculate == 0) {
              return newRemainingToCalculate;
            }
          }
        }
      }
    }
    return newRemainingToCalculate;
  }

}
