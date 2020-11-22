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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.common.IndexFunc;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.read.func.IndexFuncResult;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

public class IndexAggregationExecutor extends AggregationExecutor {

  private final Map<String, Object> queryProps;
  private final IndexType indexType;

  public IndexAggregationExecutor(
      QueryIndexPlan queryIndexPlan) {
    super(null);
    //    super(queryIndexPlan);
    indexType = queryIndexPlan.getIndexType();
    queryProps = queryIndexPlan.getProps();
    IndexUtils.breakDown("to be changed asdasdasdasxx");
  }

  /**
   * The index function is regarded as a kind of aggregation function.
   *
   * @param aggregateResultList aggregate result list
   */
  @Override
  protected QueryDataSet constructDataSet(List<AggregateResult> aggregateResultList, RawDataQueryPlan plan)
      throws QueryProcessException {
    List<PartialPath> dupSeries = new ArrayList<>();
    List<TSDataType> dupTypes = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      dupSeries.add(selectedSeries.get(i));
      dupSeries.add(selectedSeries.get(i));
      dupTypes.add(TSDataType.INT32);
      dupTypes.add(((IndexFuncResult) aggregateResultList.get(i)).getIndexFuncDataType());
    }
    ListDataSet dataSet = new ListDataSet(dupSeries, dupTypes);
    int reminding = aggregateResultList.size();
    while (reminding > 0) {
      RowRecord record = new RowRecord(0);
      for (AggregateResult resultData : aggregateResultList) {
        IndexFuncResult indexResult = (IndexFuncResult) resultData;
        TSDataType dataType = indexResult.getIndexFuncDataType();
        Pair<Integer, Object> pair = indexResult.getNextPair();
        if (pair.left != -1) {
          record.addField(pair.left, TSDataType.INT32);
          record.addField(pair.right, dataType);
        } else {
          record.addField(new Binary("-"), TSDataType.TEXT);
          reminding--;
        }
      }
      if (reminding == 0) {
        break;
      }
      dataSet.putRecord(record);
    }
    return dataSet;
  }

  /**
   * get aggregation result for one series
   *
   * @param pathToAggIndexes entry of path to aggregation indexes map
   * @param timeFilter time filter
   * @param context query context
   * @return AggregateResult list
   */
  @Override
  protected void aggregateOneSeries(
      Map.Entry<PartialPath, List<Integer>> pathToAggIndexes,
      AggregateResult[] aggregateResultList, Set<String> measurements, Filter timeFilter, QueryContext context)
      throws IOException, QueryProcessException, StorageEngineException {
    List<IndexFuncResult> indexFuncResults = new ArrayList<>();

    PartialPath seriesPath = pathToAggIndexes.getKey();
    TSDataType tsDataType = dataTypes.get(pathToAggIndexes.getValue().get(0));

    for (int i : pathToAggIndexes.getValue()) {
      // construct AggregateResult
      IndexFunc indexFunc = IndexFunc.getIndexFunc(aggregations.get(i));
      IndexFuncResult aggregateResult = new IndexFuncResult(indexFunc, tsDataType);
      indexFuncResults.add(aggregateResult);
    }
    try {
      aggregateOneSeriesIndex(seriesPath, measurements, context, timeFilter, tsDataType,
          indexFuncResults);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }

    //TODO not consider the new-coming asc/desc
    for (int i = 0; i < aggregateResultList.length; i++) {
      aggregateResultList[i] = indexFuncResults.get(i);
    }
  }

  /**
   * Override {@linkplain AggregationExecutor#aggregateOneSeries}. Besides {@linkplain
   * QueryDataSource}, we also load the index chunk metadata.
   */
  private void aggregateOneSeriesIndex(PartialPath seriesPath, Set<String> measurements,
      QueryContext context, Filter timeFilter, TSDataType tsDataType,
      List<IndexFuncResult> indexFuncResults)
      throws StorageEngineException, IOException, QueryProcessException, MetadataException {

    // construct series reader without value filter
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context, timeFilter);
//    IndexQueryReader indexQueryReader = IndexManager.getInstance()
//        .getQuerySource(seriesPath, indexType, timeFilter);
    IndexQueryReader indexQueryReader = null;
//    indexQueryReader.initQueryCondition(queryProps, indexFuncResults);
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    //TODO set ascending as true temporarily
    SeriesAggregateReader seriesReader = new SeriesAggregateReader(seriesPath,
        measurements,
        tsDataType, context, queryDataSource, timeFilter, null, null, true);
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
      Filter timeFilter) throws QueryProcessException, IOException {
    while (seriesReader.hasNextFile()) {
      // cal by file statistics
      // Compared the super method, we remove the branch of canUseCurrentFileStatistics
      // since indexes are built on chunk or partial chunk.
      while (seriesReader.hasNextChunk()) {
        indexQueryReader.updateUsableRange(seriesReader.getUnOverlappedInCurrentChunk());
        Statistics chunkStatistics = seriesReader.currentChunkStatistics();
        // Index is no-false-negative, but not no-false-positive
        long startTime = chunkStatistics.getStartTime();
        long endTime = chunkStatistics.getEndTime();
        if (indexQueryReader.canSkipDataRange(startTime, endTime)) {
          seriesReader.skipCurrentChunk();
          continue;
        }
        int remainingToCalculate = aggregateIndexOverlappedPages(seriesReader, indexQueryReader,
            indexFuncResults);
        if (remainingToCalculate == 0) {
          return;
        }
      }
    }
  }

  /**
   * post-processing function
   */
  private int aggregateIndexOverlappedPages(SeriesAggregateReader seriesReader,
      IndexQueryReader indexQueryReader, List<IndexFuncResult> aggregateResultList)
      throws IOException, QueryIndexException {
    // cal by page data
    int remainingToCalculate = Integer.MAX_VALUE;
    while (remainingToCalculate > 0 && seriesReader.hasNextPage()) {
      Statistics pageStatistics = seriesReader.currentPageStatistics();
      // Index is no-false-negative, but not no-false-positive
      //TODO Note that, after calling hasNextPage, seriesReader.currentPageStatistics always return null. So we cannot skip pages.
      if (pageStatistics != null && indexQueryReader.canSkipDataRange(
          pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
        seriesReader.skipCurrentPage();
        continue;
      }
      BatchData nextOverlappedPageData = seriesReader.nextPage();
      remainingToCalculate = indexQueryReader
          .appendDataAndPostProcess(nextOverlappedPageData, aggregateResultList);
    }
    return remainingToCalculate;
  }

}
