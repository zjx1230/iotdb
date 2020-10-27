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
package org.apache.iotdb.db.index.read.func;

import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.exception.index.UnsupportedIndexFuncException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;

public class IndexFuncFactory {


  public static double calcEuclidean(TVList aligned, double[] patterns) {
    if (aligned.size() != patterns.length) {
      throw new IndexRuntimeException("Sliding windows and modes are not equal in length");
    }
    double ed = 0;
    for (int i = 0; i < patterns.length; i++) {
      double d = IndexUtils.getDoubleFromAnyType(aligned, i) - patterns[i];
      ed += d * d;
    }
    return Math.sqrt(ed);
  }

  public static double calcDTW(TVList aligned, double[] patterns) {
    throw new UnsupportedOperationException("It's easy, but not written yet");
  }

  public static void basicSimilarityCalc(IndexFuncResult funcResult,
      IndexFeatureExtractor indexFeatureExtractor, double[] patterns)
      throws UnsupportedIndexFuncException {
    Identifier identifier;
    TVList aligned;
    switch (funcResult.getIndexFunc()) {
      case TIME_RANGE:
        identifier = indexFeatureExtractor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getEndTime() - identifier.getStartTime());
        break;
      case SERIES_LEN:
        identifier = indexFeatureExtractor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getSubsequenceLength());
        break;
      case SIM_ST:
        identifier = indexFeatureExtractor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getStartTime());
        break;
      case SIM_ET:
        identifier = indexFeatureExtractor.getCurrent_L1_Identifier();
        funcResult.addScalar(identifier.getEndTime());
        break;
      case ED:
        aligned = (TVList) indexFeatureExtractor.getCurrent_L2_AlignedSequence();
        double ed = IndexFuncFactory.calcEuclidean(aligned, patterns);
        funcResult.addScalar(ed);
        TVListAllocator.getInstance().release(aligned);
        break;
      case DTW:
        aligned = (TVList) indexFeatureExtractor.getCurrent_L2_AlignedSequence();
        double dtw = IndexFuncFactory.calcDTW(aligned, patterns);
        funcResult.addScalar(dtw);
        TVListAllocator.getInstance().release(aligned);
        break;
      default:
        throw new UnsupportedIndexFuncException("Unsupported query:" + funcResult.getIndexFunc());
    }
  }

}
