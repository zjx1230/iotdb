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
package org.apache.iotdb.db.index.algorithm.paa;

import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.preprocess.WholeMatchFeatureExtractor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * For PAA feature in the whole matching. A simplified version all PAA PAA (Piecewise Aggregate
 * Approximation), a classical feature in time series.
 *
 * <p>Refer to: Keogh Eamonn, et al. "Dimensionality reduction for fast similarity search in large
 * time series databases." Knowledge and information Systems 3.3 (2001): 263-286.
 */
public class PAAWholeFeatureExtractor extends WholeMatchFeatureExtractor {

  private static final String NON_SUPPORT_MSG = "For whole matching, it's not supported";
  private final int alignedLength;
  private final int featureDim;
  private final float[] featureArray;
  private final int paaWidth;

  public PAAWholeFeatureExtractor(
      int alignedLength,
      int featureDim,
      boolean inQueryMode,
      float[] featureArray) {
    super(inQueryMode);
    this.alignedLength = alignedLength;
    this.featureDim = featureDim;
    if (!inQueryMode && featureArray.length != featureDim) {
      throw new IllegalIndexParamException(
          String.format(
              "the featureDim (%d) doesn't match the length of feature array (%d)",
              featureDim, featureArray.length));
    }
    this.featureArray = featureArray;
    this.paaWidth = alignedLength / featureDim;
  }


  @Override
  public Object getCurrent_L3_Feature() {
    throw new UnsupportedOperationException();
  }

  /**
   * For Whole mathing, it's will deep copy
   */
  @Override
  public TVList getCurrent_L2_AlignedSequence() {
    TVList res = TVListAllocator.getInstance().allocate(TSDataType.DOUBLE);
    double timeInterval =
        ((double) (srcData.getLastTime() - srcData.getMinTime())) / (srcData.size() - 1);
    for (int i = 0; i < alignedLength; i++) {
      int idx = i >= srcData.size() ? srcData.size() - 1 : i;
      long t = (long) (srcData.getMinTime() + timeInterval * i);
      res.putDouble(t, IndexUtils.getDoubleFromAnyType(srcData, idx));
    }
    return res;
  }

  /**
   * 两件事：长度为aligned_len，不足补齐，多了不管
   */
  private void fillGivenFeatureArray() {
    //    featureArray
    for (int i = 0; i < featureDim; i++) {
      float sum = 0;
      for (int j = 0; j < paaWidth; j++) {
        int idx = paaWidth * i + j;
        if (idx >= srcData.size()) {
          idx = srcData.size() - 1;
        }
        switch (srcData.getDataType()) {
          case INT32:
            sum += srcData.getInt(idx);
            break;
          case INT64:
            sum += srcData.getLong(idx);
            break;
          case FLOAT:
            sum += srcData.getFloat(idx);
            break;
          case DOUBLE:
            sum += srcData.getDouble(idx);
            break;
          default:
            throw new NotImplementedException(srcData.getDataType().toString());
        }
      }
      featureArray[i] = sum / paaWidth;
    }
  }

  @Override
  public void processNext() {
    if (inQueryMode) {
      // do nothing.
    } else {
      fillGivenFeatureArray();
    }
    hasNewData = false;
  }


  @Override
  public long clear() {
    return 0;
  }

  @Override
  public void clearProcessedSrcData() {
    this.srcData = null;
  }
}
