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
 * The default choice. Preprocessor can custom other strategy.
 * Build index for all range, even if there is only one point.
 */
public class DefaultStrategy extends IndexRangeStrategy {


  @Override
  public boolean needBuildIndex(TVList sortedTVList, int offset, long configStartTime) {
    return sortedTVList.getTime(offset) >= configStartTime;
  }

  @Override
  public long[] calculateIndexRange(TVList sortedTVList, long buildStartTime) {
    long minTime = sortedTVList.getMinTime();
    long maxTime = sortedTVList.getLastTime();
    if (maxTime < buildStartTime) {
      return new long[0];
    } else {
      return new long[]{Math.min(minTime, buildStartTime), maxTime};
    }
  }
}
