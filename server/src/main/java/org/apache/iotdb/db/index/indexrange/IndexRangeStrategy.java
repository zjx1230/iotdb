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

public abstract class IndexRangeStrategy {

  /**
   * Given the TVList and the configured building start time, determine whether to build index by
   * considering the time and value distribution.
   *
   * @param sortedTVList the sorted TVList to build the index
   * @param offset the offset of TVList
   * @param configStartTime user-setting start time
   */
  public abstract boolean needBuildIndex(TVList sortedTVList, int offset, long configStartTime);

  public abstract long[] calculateIndexRange(TVList sortedTVList, long buildStartTime);

}
