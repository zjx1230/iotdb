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

/**
 * It's the special and simplest case where a complex pattern contains only one subpattern. We only
 * need to determine the subpattern threshold (equivalent to the complex pattern threshold).
 */
public class SingleSegmentationParam implements CalcParam {

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

  /**
   * Users can specify the threshold value directly, or provide a threshold ratio. If it's the *
   * latter, the threshold value is a function of the ratio and the pattern's value range.
   *
   * @param thresholdBase if not -1, it's set to the pattern threshold directly.
   * @param thresholdRatio if not -1, it's used to compute the threshold value.
   * @param windowRange the length of sliding window, used to determine the pattern borders.
   */
  public SingleSegmentationParam(double thresholdBase, double thresholdRatio, int windowRange) {
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

  public SingleSegmentationParam createInstanceByAbsoluteThreshold(float threshold,
      int windowRange) {
    return new SingleSegmentationParam(threshold, -1, windowRange);
  }

  public SingleSegmentationParam createInstanceByValueRatio(float thresholdRatio, int windowRange) {
    return new SingleSegmentationParam(-1, thresholdRatio, windowRange);
  }

  /**
   * In SinglePattern, only threshold will be modified. We reuse the Array as much as possible.
   */
  @Override
  public Object[] calculateParam(TVList tvList, int offset, int length) {

    if (this.thresholdRatio != -1) {
      thresholdsArray[0] = getValueRange(tvList, offset, length) * thresholdRatio;
    } else {
      thresholdsArray[0] = thresholdBase;
    }
    return res;
  }

}
