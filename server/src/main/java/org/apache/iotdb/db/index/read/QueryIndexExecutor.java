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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
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
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

public class QueryIndexExecutor {

  private final Map<String, Object> queryProps;
  private final IndexType indexType;
  private final QueryContext context;
  private final List<PartialPath> paths;

  public QueryIndexExecutor(QueryIndexPlan queryIndexPlan, QueryContext context) {
    this.indexType = queryIndexPlan.getIndexType();
    this.queryProps = queryIndexPlan.getProps();
    this.paths = queryIndexPlan.getPaths();
    this.context = context;
  }

  public QueryDataSet executeIndexQuery() throws StorageEngineException, QueryIndexException{
    // get all related storage group
    List<TVList> result = IndexManager.getInstance()
        .queryIndex(paths, indexType, queryProps, context);
    return constructDataSet(result);
  }

  private QueryDataSet constructDataSet(List<TVList> result) {
    for (TVList tvList : result) {
      System.out.println(tvList);
    }
//    types =
    ListDataSet dataSet = new ListDataSet(paths, null);
    return dataSet;
  }

  private QueryDataSet constructDataSet(List<AggregateResult> aggregateResultList, RawDataQueryPlan plan)
      throws QueryProcessException {
    List<PartialPath> dupSeries = new ArrayList<>();
    List<TSDataType> dupTypes = new ArrayList<>();
//    for (int i = 0; i < selectedSeries.size(); i++) {
//      dupSeries.add(selectedSeries.get(i));
//      dupSeries.add(selectedSeries.get(i));
//      dupTypes.add(TSDataType.INT32);
//      dupTypes.add(((IndexFuncResult) aggregateResultList.get(i)).getIndexFuncDataType());
//    }
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

}
