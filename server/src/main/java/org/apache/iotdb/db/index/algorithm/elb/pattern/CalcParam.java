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
package org.apache.iotdb.db.index.algorithm.elb.pattern;

import org.apache.iotdb.db.utils.datastructure.TVList;

public interface CalcParam {

  /**
   * A complex pattern contains one or more subpatterns, and they are allowed to specified different
   * Lp-Norm thresholds. Accordingly, the parameters of ELB patterns include subpattern thresholds
   * and subpattern segmentation (left and right borders).
   *
   * <p>In ELB-Index scenario, we use a sliding window or convert a long time series into a list of
   * subsequence. One subsequence (specified by {@code tvList},{@code offset},{@code length}) is
   * regarded as a complex pattern. <code>CalcParam</code> is to calculate the pattern parameters:
   * subpattern thresholds and the left and right borders.
   *
   * @return [subpatternCount, thresholdsArray, minLeftBorders, maxLeftBorders]
   */
  Object[] calculateParam(TVList tvList, int offset, int length);
}
