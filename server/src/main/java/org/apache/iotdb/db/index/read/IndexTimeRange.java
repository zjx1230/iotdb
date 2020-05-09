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

import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

public class IndexTimeRange {


  private Filter timeFilter;



  public IndexTimeRange() {
    timeFilter = TimeFilter.gt(Long.MAX_VALUE);
  }

  public IndexTimeRange(Filter timeFilter) {
    if (timeFilter == null) {
      this.timeFilter = TimeFilter.gt(Long.MAX_VALUE);
    } else {
      this.timeFilter = timeFilter;
    }
  }


  public void addRange(long startTime, long endTime) {
//    if (timeFilter == null) {
//      timeFilter = toFilter(startTime, endTime);
//      return;
//    }
    timeFilter = FilterFactory.or(timeFilter, toFilter(startTime, endTime));
  }

  public void pruneRange(long startTime, long endTime) {
//    if (timeFilter == null) {
//      timeFilter = TimeFilter.not(toFilter(startTime, endTime));
//    }
    timeFilter = FilterFactory.and(timeFilter, TimeFilter.not(toFilter(startTime, endTime)));
  }

  /**
   * @return True if this range fully contains [start, end]
   */
  public boolean fullyContains(long startTime, long endTime) {
    return timeFilter.containStartEndTime(startTime, endTime);
  }

  public boolean intersect(long startTime, long endTime) {
    return timeFilter.satisfyStartEndTime(startTime, endTime);
  }

  public static Filter toFilter(long startTime, long endTime) {
    return FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
  }


  public Filter getTimeFilter() {
    return timeFilter;
  }
  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  @Override
  public String toString() {
    return timeFilter == null ? "null" : timeFilter.toString();
  }


}
