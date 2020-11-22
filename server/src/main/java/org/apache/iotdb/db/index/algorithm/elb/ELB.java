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
package org.apache.iotdb.db.index.algorithm.elb;

import static org.apache.iotdb.db.index.common.IndexUtils.getDoubleFromAnyType;

import org.apache.iotdb.db.index.algorithm.elb.feature.ElementELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.feature.SequenceELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.feature.PatternEnvelope;
import org.apache.iotdb.db.index.algorithm.elb.pattern.CalcParam;
import org.apache.iotdb.db.index.algorithm.elb.pattern.ELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.pattern.MilesPattern;
import org.apache.iotdb.db.index.distance.Distance;
import org.apache.iotdb.db.index.distance.LInfinityNormdouble;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * Memory consumption can be considered constant.
 */
public class ELB {

  // final fields
  private final int windowRange;
  private final int blockNum;
  private final int blockWidth;
  private final MilesPattern pattern;

  private PatternEnvelope envelope;
  private ELBType elbType;
  private ELBFeature elbFeature;

  public ELB(Distance distance, int windowRange, int blockWidth, ELBType elbType) {
    this.windowRange = windowRange;
    this.blockWidth = blockWidth;
    this.blockNum = windowRange / blockWidth;

    pattern = new MilesPattern(distance);
    this.elbType = elbType;
    if (elbType == ELBType.SEQ && distance instanceof LInfinityNormdouble) {
      throw new NotImplementedException("For ELB-SEQ on L∞-Norm, there is a direct and simple "
          + "algorithm for Adaptive Post-Processing, But we haven't implemented yet.");
    }
    if (elbType == ELBType.ELE) {
      envelope = new PatternEnvelope();
      elbFeature = new ElementELBFeature();
    } else {
      elbFeature = new SequenceELBFeature(distance);
    }

  }

//  /**
//   * Given {@code offset}, calculate ELB features and append to {@code mbrs}. The pattern threshold
//   * and segmentation will be computed by {@code calcParam}
//   *
//   * @param tvList source data
//   * @param offset the pattern is a subsequence of @{tvList} starting from {@code offset}
//   * @param mbrs mbrs should be empty and this function will append the elb features into it.
//   */
//  public void calcELBFeature(TVList tvList, int offset, PrimitiveList mbrs, CalcParam calcParam) {
//    // refresh array
//    Object[] params = calcParam.calculateParam(tvList, offset, windowRange);
//    int subpatternCount = (int) params[0];
//    double[] thresholdsArray = (double[]) params[1];
//    int[] minLeftBorders = (int[]) params[2];
//    int[] maxLeftBorders = (int[]) params[3];
//    pattern
//        .initPattern(tvList, offset, windowRange, subpatternCount, thresholdsArray, minLeftBorders,
//            maxLeftBorders);
//    if (elbType == ELBType.ELE) {
//      envelope.refresh(pattern);
//    }
//    Pair<double[], double[]> features = elbFeature
//        .calcPatternFeature(pattern, blockNum, envelope);
//    appendToMBRs(mbrs, features);
//  }

  private void appendToMBRs(PrimitiveList mbrs, Pair<double[], double[]> features) {
    for (int i = 0; i < blockNum; i++) {
      mbrs.putDouble(features.left[i]);
      mbrs.putDouble(features.right[i]);
    }
  }

  /**
   * Given specified thresholds and segmentation, calculate ELB features and append to {@code
   * mbrs}.
   *
   * @param tvList source data
   * @param offset the pattern is a subsequence of @{tvList} starting from {@code offset}
   * @return left: upper bounds, right: lower bounds
   */
  public Pair<double[], double[]> calcELBFeature(double[] tvList, int offset,
      double[] thresholdsArray, int[] borders) {
    // refresh array
    int subpatternCount = thresholdsArray.length;
    pattern.initPattern(tvList, offset, windowRange, subpatternCount, thresholdsArray, borders,
        borders);
    if (elbType == ELBType.ELE) {
      envelope.refresh(pattern);
    }
    return elbFeature.calcPatternFeature(pattern, blockNum, envelope);
  }

  public boolean exactDistanceCalc(TVList slidingWindow) {
    return pattern.exactDistanceCalc(slidingWindow);
  }

  public enum ELBType {
    ELE, SEQ;
  }

  public static class ELBWindowBlockFeature {

    public long startTime;
    public long endTime;
    public double feature;

    public ELBWindowBlockFeature(long startTime, long endTime, double feature) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.feature = feature;
    }

    public ELBWindowBlockFeature(ELBWindowBlockFeature block) {
      this.startTime = block.startTime;
      this.endTime = block.endTime;
      this.feature = block.feature;
    }

    @Override
    public String toString() {
      return String.format("%d-%d:%.2f", startTime, endTime, feature);
    }
  }

  public static double calcWindowBlockFeature(ELBType elbType, TVList tvList, int offset,
      int blockWidth) {
    switch (elbType) {
      case ELE:
        return getDoubleFromAnyType(tvList, offset + blockWidth - 1);
      case SEQ:
        double res = 0;
        for (int i = offset; i < offset + blockWidth; i++) {
          res += getDoubleFromAnyType(tvList, i);
        }
        // for SEQ, we use sum version for efficiency
        return res;
      default:
        throw new NotImplementedException("unsupported elb type:" + elbType);
    }
  }


  public int getAmortizedSize() {
    return blockNum * Double.BYTES;
  }


}
