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
package org.apache.iotdb.db.index.algorithm.elb.feature;


import java.util.Arrays;
import org.apache.iotdb.db.index.algorithm.elb.pattern.ELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.pattern.MilesPattern;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LInfinityNormdouble;
import org.apache.iotdb.db.index.distance.LNormDouble;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * SEQ-ELB
 */
public class SequenceELBFeature extends ELBFeature {

  private final Distance distance;
  private double[] sumValues;
  private double[] sumUppers;
  private double[] sumLowers;

  public SequenceELBFeature(Distance distance) {
    this.distance = distance;
  }


  /**
   * refresh and calculate new SEQ-ELB
   */
  @Override
  public Pair<double[], double[]> calcPatternFeature(MilesPattern pattern, int blockNum,
      PatternEnvelope envelope) {
    checkAndExpandArrays(blockNum);
    int windowBlockSize = (int) Math.floor(((double) pattern.sequenceLen) / blockNum);

    for (int i = 1; i < blockNum; i++) {
      //get the largest tolerances in this pttNode windows
      if (sumLowers == null || sumLowers.length < windowBlockSize) {
        sumLowers = new double[windowBlockSize];
        sumValues = new double[windowBlockSize];
        sumUppers = new double[windowBlockSize];
      } else {
        Arrays.fill(sumLowers, 0);
        Arrays.fill(sumValues, 0);
        Arrays.fill(sumUppers, 0);
      }

      /* {@code j} is the starting point aligning to all offsets. The leftmost end starts from (the
       * previous segment + 1), and the rightmost end is exactly aligned, i.e. i * windowBlockSize.
       */
      for (int j = (i - 1) * windowBlockSize + 1; j <= i * windowBlockSize; j++) {
        int end = j + windowBlockSize - 1;
        int startIndex = j - ((i - 1) * windowBlockSize + 1);
        //get sum of ε^p
        double interval = getEpsilonSum(pattern, distance, j, end, windowBlockSize);

        for (int l = j; l <= end; l++) {
          sumValues[startIndex] += pattern.tvList[pattern.tvListOffset + l];
        }
        sumLowers[startIndex] = sumValues[startIndex] - interval;
        sumUppers[startIndex] = sumValues[startIndex] + interval;
        //update
        if (sumUppers[startIndex] > upperLines[i]) {
          upperLines[i] = sumUppers[startIndex];
        }
        if (sumLowers[startIndex] < lowerLines[i]) {
          lowerLines[i] = sumLowers[startIndex];
        }
        if (sumValues[startIndex] > actualUppers[i]) {
          actualUppers[i] = sumValues[startIndex];
        }
        if (sumValues[startIndex] < actualLowers[i]) {
          actualLowers[i] = sumValues[startIndex];
        }
      }
    }
    upperLines[0] = Double.MAX_VALUE;
    lowerLines[0] = -Double.MAX_VALUE;
    return new Pair<>(upperLines, lowerLines);
  }

  /**
   * Calculate ε_sum(i)。end -> i，start -> ws
   *
   * @param start i.e. ws
   */
  private static double getEpsilonSum(MilesPattern pattern, Distance distance, int start, int end,
      int windowBlockSize) {
    double p = distance.getP();
    double interval = 0;
    //Line2~Line3，calculate m and n
    int m;
    int n;
    for (m = 0; m < pattern.subpatternCount; m++) {
      if (start < pattern.maxLeftBorders[m]) {
        break;
      }
    }
    for (n = pattern.subpatternCount - 1; n > 0; n--) {
      if (end >= pattern.minLeftBorders[n]) {
        break;
      }
    }
    if (m == 0) {
      System.out.println();
    }
    if (distance instanceof LNormDouble) {
      //Line 4，left
      double sum = (pattern.maxLeftBorders[m - 1] - pattern.minLeftBorders[m - 1]) *
          distance.getThresholdNoRoot(pattern.thresholdsArray[m - 1]);
      //Line 5~7，middle
      for (int i = pattern.maxLeftBorders[m - 1]; i < pattern.minLeftBorders[n + 1] - 1; i++) {
        sum += distance.getThresholdNoRoot(getAldMax(pattern, i));
      }
      //Line 8，right
      sum += (pattern.maxLeftBorders[n + 1] - pattern.minLeftBorders[n + 1]) * distance
          .getThresholdNoRoot(pattern.thresholdsArray[n]);
      interval = Math.pow(sum / windowBlockSize, 1 / p) * windowBlockSize;
    } else if (distance instanceof LInfinityNormdouble) {
      double maxThreshold = -Double.MAX_VALUE;
      for (int i = m - 1; i < n + 1; i++) {
        double temp = distance.getThresholdNoRoot(pattern.thresholdsArray[i]);
        if (temp > maxThreshold) {
          maxThreshold = temp;
        }
      }
      interval = maxThreshold * windowBlockSize;
    }
    return interval;
  }

  private static double getAldMax(MilesPattern pattern, int i) {
    int k;
    for (k = 0; k < pattern.subpatternCount; k++) {
      if (pattern.minLeftBorders[k] <= i && i < pattern.minLeftBorders[k + 1]) {
        break;
      }
    }
    if (i < pattern.maxLeftBorders[k])
    //Equation 6 in paper，Line1
    {
      return Math.max(pattern.thresholdsArray[k - 1], pattern.thresholdsArray[k]);
    } else
    //Equation 6 in paper，Line2
    {
      return pattern.thresholdsArray[k];
    }
  }
}
