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
package org.apache.iotdb.db.qp.logical.crud;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * this operator is to create a certain index on some time series.
 */
public class QueryIndexOperator extends QueryOperator {

  private Map<String, String> props;

//  private List<Pair<Long, Long>> timeRanges;
  private IndexType indexType;

  public QueryIndexOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.QUERY_INDEX;
  }

//  public List<Pair<Long, Long>> getTimeRanges() {
//    return timeRanges;
//  }
//
//  public void setTimeRanges(
//      List<Pair<Long, Long>> timeRanges) {
//    this.timeRanges = timeRanges;
//  }


  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }
}
