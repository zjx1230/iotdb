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

/**
 * Memory consumption can be considered constant.
 */
public class ELBFeatureExtractor {

  // final fields
  private final TVList tvList;
  private final int windowRange;
  private final int blockNum;
  private final MilesPattern pattern;

  // parameters to be generator

  private final CalcParam calcParam;

  private PatternEnvelope envelope;
  private ELBType elbType;
  private ELBFeature elbFeature;

  public ELBFeatureExtractor(TVList tvList, Distance distance, int windowRange,
      CalcParam calcParam, int blockNum, ELBType elbType) {
    this.tvList = tvList;
    this.windowRange = windowRange;
    this.blockNum = blockNum;
    this.calcParam = calcParam;

    pattern = new MilesPattern(distance);
    this.elbType = elbType;
    if (elbType == ELBType.SEQ && distance instanceof LInfinityNormdouble) {
      throw new NotImplementedException("For ELB-SEQ on L∞-Norm, there is a direct and simple "
          + "algorithm for Adaptive Post-Processing, But we haven't realized yet.");
    }
    if (elbType == ELBType.ELE) {
      envelope = new PatternEnvelope();
      elbFeature = new ElementELBFeature();
    } else {
      elbFeature = new SequenceELBFeature(distance);
    }
  }

  /**
   * Given {@code offset}, calculate ELB features and append to {@code mbrs}
   *
   * @param mbrs to be appended.
   */
  public void calcELBFeature(int offset, PrimitiveList mbrs) {
    // refresh array
    Object[] params = calcParam.refreshParam(tvList, offset, windowRange);
    int subpatternCount = (int) params[0];
    double[] thresholdsArray = (double[]) params[1];
    int[] minLeftBorders = (int[]) params[2];
    int[] maxLeftBorders = (int[]) params[3];
    pattern.refresh(tvList, offset, windowRange, subpatternCount, thresholdsArray, minLeftBorders,
        maxLeftBorders);
    if (elbType == ELBType.ELE) {
      envelope.refresh(pattern);
    }
    elbFeature.refreshAndAppendToList(pattern, blockNum, envelope, mbrs);
  }

  public enum ELBType {
    ELE, SEQ
  }

  public int getAmortizedSize() {
    return blockNum * Double.BYTES;
  }
}
