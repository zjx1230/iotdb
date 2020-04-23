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

import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.db.index.common.IndexType;

public class QueryIndexPlan extends AggregationPlan {
  private Map<String, String> props;
  private IndexType indexType;

  public QueryIndexPlan() {
  }

  public static QueryIndexPlan initFromAggregationPlan(AggregationPlan aggregationPlan){
    QueryIndexPlan indexQueryPlan = new QueryIndexPlan();
    indexQueryPlan.setPaths(aggregationPlan.getPaths());
    indexQueryPlan.setAggregations(aggregationPlan.getAggregations());
    indexQueryPlan.setExpression(aggregationPlan.getExpression());
    indexQueryPlan.setDataTypes(aggregationPlan.getDataTypes());
    indexQueryPlan.setAlignByTime(aggregationPlan.isAlignByTime());
    indexQueryPlan.setDeduplicatedAggregations(aggregationPlan.getDeduplicatedAggregations());
    indexQueryPlan.setDeduplicatedDataTypes(aggregationPlan.getDeduplicatedDataTypes());
    return indexQueryPlan;
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
