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

import static org.apache.iotdb.db.index.common.IndexUtils.getDoubleFromAnyType;

import java.util.Arrays;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LNormDouble;
import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * <p> Multiple Indefinite-LEngth Sub-Pattern，MILES-pattern is composed of multiple
 * consecutive sub-patterns with different thresholds. The breakpoints between sub-patterns are
 * indefinite within a small range, i.e. subpattern lengths are somewhat variable.</p>
 *
 * <p> The indefinite breakpoint can be regard as a "wildcard character", representing one or more
 * subpattern case. It could expand your return results and somewhat alleviate the false dismissals
 * caused by outlier and changing trend. </p>
 *
 * <p> Format example:</p>
 * For pattern dataPoints：[1,2,3,4,5,6,7,8]
 * <pre>
 * startTolIndex: [0,3,7]
 * endTolIndex: [0,5,8]
 * toleranceArray: [0.1,0.4,0.7]
 *
 * The first breakpoint is minLeftBorders[1] and maxLeftBorders[1]，which means the second
 * subpattern can start from 3 to 5. Similarly, the third subpattern can start from 7 or 8.
 * </pre>
 */
public class MilesPattern {
  public int[] minLeftBorders;
  public int[] maxLeftBorders;
  /**
   * the number of points in the sliding window
   */
  public int sequenceLen;
  /**
   * the number of subpatterns specified by user
   */
  public int subpatternCount;
  public double[] thresholdsArray;

  public double[] tvList;
  private double[] diff;
  private double[] thresholdPowers;
  private int[] idxSet;
  public final Distance distanceMetric;
  public int tvListOffset;

  public MilesPattern(Distance distanceMetric) {
    this.distanceMetric = distanceMetric;
  }

  public void initPattern(double[] tvList, int tvListOffset, int length, int subpatternCount,
      double[] thresholdsArray, int[] minLeftBorders, int[] maxLeftBorders) {
//    this.tvList = tvList;
    this.tvList = tvList;
    this.tvListOffset = tvListOffset;
    this.sequenceLen = length;
    this.subpatternCount = subpatternCount;
    this.thresholdsArray = thresholdsArray;
    this.minLeftBorders = minLeftBorders;
    this.maxLeftBorders = maxLeftBorders;

    checkBorder();

    //the maximal potential variable range, to avoid frequently allocating memory
    int maxRadius = -1;
    for (int i = 0; i < this.subpatternCount; i++) {
      if (this.maxLeftBorders[i] - this.minLeftBorders[i] > maxRadius) {
        maxRadius = this.maxLeftBorders[i] - this.minLeftBorders[i];
      }
    }
    // lazy-mode to allocate array
    if (diff == null || diff.length < maxRadius + 1) {
      diff = new double[maxRadius + 1];
      idxSet = new int[maxRadius + 1];
    }
    // no matter reallocate, reset them before using.
    Arrays.fill(diff, 0);
    Arrays.fill(idxSet, 0);

    if (thresholdPowers == null || thresholdPowers.length < subpatternCount) {
      thresholdPowers = new double[subpatternCount];
    }
    Arrays.fill(thresholdPowers, 0);
    for (int i = 0; i < subpatternCount; i++) {
      if (distanceMetric instanceof LNormDouble) {
        thresholdPowers[i] = ((LNormDouble) distanceMetric).pow(thresholdsArray[i]);
      }
    }
  }

  private void checkBorder() {
    for (int i = 0; i < subpatternCount; i++) {
      if (minLeftBorders[i] > maxLeftBorders[i]) {
        throw new IllegalIndexParamException(
            String.format("ELB border error: minLeftBorders[%d]=%d > maxLeftBorders[%d]=%d", i,
                minLeftBorders[i], i, maxLeftBorders[i]));
      }
      if (maxLeftBorders[i] >= minLeftBorders[i + 1]) {
        throw new IllegalIndexParamException(
            String.format("ELB border error: maxLeftBorders[%d]=%d > minLeftBorders[%d+1]=%d", i,
                maxLeftBorders[i], i, minLeftBorders[i + 1]));
      }
    }
  }

  public int distEarlyAbandon(Distance disMetric, double[] stream, int offset) {
    throw new UnsupportedOperationException();
  }

  /**
   * Adaptive Post-processing Algorithm.
   *
   * @return positive for matched item, negative for for unmatched one
   */
  public int distEarlyAbandonDetail(Distance disMetric, double[] stream, int offset) {
    LNormDouble normdouble;
    if (disMetric instanceof LNormDouble) {
      normdouble = (LNormDouble) disMetric;
    } else {
      throw new UnsupportedOperationException();
    }
    double leftSum = 0;
    int i = 1;
    for (; i < this.subpatternCount; i++) {
      boolean setFlag = false;
      int j;
      for (j = this.maxLeftBorders[i - 1]; j < this.minLeftBorders[i] - 1; j++) {
        leftSum +=
            normdouble.pow(
                Math.abs(tvList[tvListOffset + j] - stream[j + offset]))
                - normdouble.pow(this.thresholdsArray[i - 1]);
      }
      int k = 0;
      for (; j < this.maxLeftBorders[i]; j++, k++) {
        diff[k] = normdouble
            .pow(Math.abs(tvList[j + tvListOffset] - stream[j + offset]));
        leftSum += diff[k] - thresholdPowers[i - 1];
        if (leftSum <= 0) {
          idxSet[k] = j;
          setFlag = true;
        } else {
          idxSet[k] = -1;
        }
      }
      if (!setFlag) {
        return 1;
      }

      double nextSum = 0;
      /* A special case: the breakpoint is e_i.  It means the previous subpattern ends at e_i-1.
       * If the previous subpattern matches, minSum is 0; otherwise, it is set to maximum.
       */
      double minSum = (idxSet[--k] > 0) ? 0 : Double.MAX_VALUE;
      for (; k > 0; ) {
        nextSum += diff[k] - thresholdPowers[i];
        if (idxSet[--k] > 0 && nextSum < minSum) {
          minSum = nextSum;
        }
      }
      leftSum = minSum;
    }
    for (int j = this.maxLeftBorders[i - 1]; j < this.sequenceLen; j++) {
      leftSum += normdouble
          .pow(Math.abs(tvList[j + tvListOffset] - stream[j + offset]))
          - normdouble.pow(this.thresholdsArray[i - 1]);
    }
    return (leftSum <= 0) ? -1 : 1;
  }


  /**
   *
   * @param disMetric
   * @param stream
   * @param offset
   * @param bps
   * @return
   */
  public int distEarlyAbandonGrain(Distance disMetric, double[] stream, int offset, int[] bps) {
    int ret = 0;
    int len;
    for (int i = 0; i < subpatternCount; i++) {
      len = bps[i + 1] - bps[i];
      int count = disMetric.distEarlyAbandonDetailNoRoot(stream, offset + bps[i], tvList, bps[i], len, thresholdPowers[i] * len);
      if (count > 0) {
        return ret + count;
      } else {
        ret -= count;
      }
    }
    return -ret;
  }

  public int distDetailGrain(Distance disMetric, double[] stream, int offset, int[] bps) {
    int ret = 0;
    int len;
    double dis;
    int i;
    for (i = 0; i < subpatternCount; i++) {
      len = bps[i + 1] - bps[i];
      dis = disMetric.distPower(stream, offset + bps[i], tvList, bps[i], len);
      ret += len;
      if (dis > thresholdPowers[i] * len) {
        return ret;
      }
    }
    return -ret;
  }

  /**
   * calculate the exact distance between this pattern and the given sliding window, return whether it is a matched result.
   * @param slidingWindow to be calculated
   * @return true if slidingWindow is a matched result.
   */
  public boolean exactDistanceCalc(TVList slidingWindow, int offset) {
    int len;
    for (int i = 0; i < subpatternCount; i++) {
      len = minLeftBorders[i + 1] - minLeftBorders[i];
      double dist = distanceMetric.distPower(slidingWindow, offset + minLeftBorders[i], tvList, minLeftBorders[i], len);
      if (dist > thresholdPowers[i] * len) {
        return false;
      }
    }
    return true;
  }

//  public double getDoubleFromRelativeIdx(int idx) {
//    return getDoubleFromAnyType(tvList, tvListOffset + idx);
//  }
}
