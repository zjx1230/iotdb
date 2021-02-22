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

import org.apache.iotdb.db.index.algorithm.elb.pattern.ELBFeature;
import org.apache.iotdb.db.index.algorithm.elb.pattern.MilesPattern;
import org.apache.iotdb.tsfile.utils.Pair;

/** PatternNode：Pattern is divided into a list of blocks */
public class ElementELBFeature extends ELBFeature {

  /** calculate ELE-ELB. */
  public Pair<double[], double[]> calcPatternFeature(
      MilesPattern pattern, int blockNum, PatternEnvelope envelope) {
    int windowBlockSize = (int) Math.floor(((double) pattern.sequenceLen) / blockNum);
    checkAndExpandArrays(blockNum);
    for (int i = 0; i < blockNum; i++) {
      // get the largest tolerances in this pattern
      for (int j = i * windowBlockSize; j < (i + 1) * windowBlockSize; j++) {
        if (envelope.upperLine[j] > upperLines[i]) {
          upperLines[i] = envelope.upperLine[j];
        }
        if (envelope.lowerLine[j] < lowerLines[i]) {
          lowerLines[i] = envelope.lowerLine[j];
        }
        if (envelope.valueLine[j] > actualUppers[i]) {
          actualUppers[i] = envelope.valueLine[j];
        }
        if (envelope.valueLine[j] < actualLowers[i]) {
          actualLowers[i] = envelope.valueLine[j];
        }
      }
    }
    return new Pair<>(upperLines, lowerLines);
  }
}
