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
package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexConstant;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;

public class QueryIndexPlan extends AggregationPlan {
  private Map<String, String> props;
  private IndexType indexType;
  private Map<String, Integer> dupPathToIndex;

  public QueryIndexPlan() {
    super();
    setOperatorType(OperatorType.QUERY_INDEX);
  }

  public static QueryIndexPlan initFromAggregationPlan(AggregationPlan aggregationPlan)
      throws QueryProcessException {
    QueryIndexPlan indexQueryPlan = new QueryIndexPlan();
    indexQueryPlan.setPaths(aggregationPlan.getPaths());
    indexQueryPlan.setDeduplicatedPaths(aggregationPlan.getDeduplicatedPaths());
    indexQueryPlan.setAggregations(aggregationPlan.getAggregations());
    indexQueryPlan.setExpression(aggregationPlan.getExpression());
    indexQueryPlan.setDataTypes(aggregationPlan.getDataTypes());
    indexQueryPlan.setAlignByTime(aggregationPlan.isAlignByTime());
    indexQueryPlan.setDeduplicatedAggregations(aggregationPlan.getDeduplicatedAggregations());
    indexQueryPlan.setDeduplicatedDataTypes(aggregationPlan.getDeduplicatedDataTypes());
    indexQueryPlan.setPathToIndex(aggregationPlan.getPathToIndex());
    indexQueryPlan.setRowLimit(aggregationPlan.getRowLimit());
    indexQueryPlan.setRowOffset(aggregationPlan.getRowOffset());
    return indexQueryPlan;
  }

  public void setPathToIndex(Map<String, Integer> pathToIndex) {
    this.dupPathToIndex = new HashMap<>();
    List<String> dupList = new ArrayList<>(pathToIndex.size() * 2);
    pathToIndex.forEach((s,i)->{
      dupList.add(i*2, IndexConstant.ID + i);
      dupList.add(i*2+1, s);
    });
    for (int i = 0; i < dupList.size(); i++) {
      dupPathToIndex.put(dupList.get(i), i);
    }

  }

  @Override
  public Map<String, Integer> getPathToIndex() {
    return dupPathToIndex;
  }


  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryIndexPlan that = (QueryIndexPlan) o;
    return super.equals(o)
        && Objects.equals(paths, that.paths)
        && Objects.equals(props, that.props)
        && Objects.equals(indexType, that.indexType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), props, indexType);
  }

  @Override
  public String toString() {
    String aggInfo = String.format("Paths: %s, agg names: %s, data types: %s, filter: %s",
        paths, getDeduplicatedAggregations(), getDeduplicatedDataTypes(), getExpression());
    return String.format("Aggregation info: %s, index type: %s, props: %s",
        aggInfo, indexType, props);
  }
}
