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
package org.apache.iotdb.db.index.indexrange;

import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * <p>The star discrepancy is the most well-known measure for the uniformity of point
 * distributions. If the sample points are reasonably equi-distributed, it is expected that the
 * proportion of sample points that fall within that smaller square is proportionately smaller.</p>
 *
 * <p>For efficiency, we calculate the discrepancy only on one-resolution grids. The discrepancy is
 * normalized. A large discrepancy indicates poorly distributed sets.</p>
 *
 * <p>Refer to: Ong, Meng Sang, et.al. "Statistical measures of two dimensional point set
 * uniformity." Computational Statistics & Data Analysis 56.6 (2012): 2159-2181.</p>
 */
public class UniformStrategy extends IndexRangeStrategy {

  @Override
  public boolean needBuildIndex(TVList sortedTVList, int offset, long configStartTime) {
    // find the allowed offset
    while (sortedTVList.getTime(offset) < configStartTime) {
      offset++;
      if (offset == sortedTVList.size()) {
        return false;
      }
    }
    long st = sortedTVList.getTime(offset);
    long end = sortedTVList.getLastTime();
    long interval = (end - st) / sortedTVList.size();
    int interIdx = 1;
    int target = 1;
    float discrepancy = 0;
    int currentNum = 1;
    // the first point must be inside the first interval.
    for (int i = offset + 1; i < sortedTVList.size(); i++) {
      long time = sortedTVList.getTime(i);
      if (time > interIdx * interval) {
        interIdx++;
        discrepancy += Math.abs(target - currentNum);
        currentNum = 0;
      } else {
        currentNum++;
      }
    }
    discrepancy /= 2 * (sortedTVList.size() - 1);
    // the default threshold is 0.3, indicating that more than 70% of the points are not within
    // the corresponding interval.
    return discrepancy < 0.3;
  }

  @Override
  public long[] calculateIndexRange(TVList sortedTVList, long configStartTime) {
    long minTime = sortedTVList.getMinTime();
    long maxTime = sortedTVList.getLastTime();
    if (maxTime < configStartTime) {
      return new long[0];
    } else {
      return new long[]{Math.min(minTime, configStartTime), maxTime};
    }
  }
}
