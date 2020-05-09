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

import static org.apache.iotdb.db.index.common.IndexUtils.getValueRange;

import org.apache.iotdb.db.utils.datastructure.TVList;

public class SingleParamSchema implements CalcParam {

  private final double[] thresholdsArray;
  /**
   * -1 means Ratio mode
   */
  private double thresholdBase;
  /**
   * -1 means Absolute mode
   */
  private double thresholdRatio;
  private final Object[] res;

  public SingleParamSchema(double thresholdBase, double thresholdRatio, int windowRange) {
    this.thresholdBase = thresholdBase;
    this.thresholdRatio = thresholdRatio;
    this.res = new Object[4];
    int subpatternCount = 1;
    thresholdsArray = new double[subpatternCount];
    int[] minLeftBorders = new int[]{0, windowRange};
    int[] maxLeftBorders = new int[]{0, windowRange};
    res[0] = subpatternCount;
    res[1] = thresholdsArray;
    res[2] = minLeftBorders;
    res[3] = maxLeftBorders;
  }

  public SingleParamSchema createInstanceByAbsoluteThreshold(float threshold, int windowRange) {
    return new SingleParamSchema(threshold, -1, windowRange);
  }

  public SingleParamSchema createInstanceByValueRatio(float thresholdRatio, int windowRange) {
    return new SingleParamSchema(-1, thresholdRatio, windowRange);
  }

  /**
   * In SinglePattern, only threshold will be modified. We reuse the Array as much as possible.
   */
  @Override
  public Object[] refreshParam(TVList tvList, int offset, int length) {

    if (this.thresholdRatio != -1) {
      thresholdsArray[0] = getValueRange(tvList, offset, length) * thresholdRatio;
    }else{
      thresholdsArray[0] = thresholdBase;
    }
    return res;
  }

}
