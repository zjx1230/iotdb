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

import java.io.IOException;
import java.io.OutputStream;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.TimeFixedPreprocessor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * PAA (Piecewise Aggregate Approximation), a classical feature in time series. <p>
 *
 * Refer to: Keogh Eamonn, et al. "Dimensionality reduction for fast similarity search in large time
 * series databases." Knowledge and information Systems 3.3 (2001): 263-286.
 */
public class PAATimeFixedPreprocessor extends TimeFixedPreprocessor {

  public PAATimeFixedPreprocessor(TSDataType tsDataType, int windowRange, int slideStep, int paaDim,
      long timeAnchor, boolean storeIdentifier, boolean storeAligned) {
    super(tsDataType, windowRange, slideStep, paaDim, timeAnchor, storeIdentifier, storeAligned);
  }

  public PAATimeFixedPreprocessor(TSDataType tsDataType, int windowRange, int slideStep, int paaDim,
      boolean storeIdentifier, boolean storeAligned, long timeAnchor) {
    super(tsDataType, windowRange, slideStep, paaDim, timeAnchor, storeIdentifier, storeAligned);
  }


  protected boolean checkValid(int startIdx, long startTime, long endTime) {
    long segStartTime;
    long segEndTime;
    int idx = startIdx;
    // every segment should have at least one point
    for (int i = 0; i < alignedDim; i++) {
      segStartTime = startTime + i * intervalWidth;
      segEndTime = segStartTime + intervalWidth - 1;
      idx = locatedIdxToTimestamp(idx, segStartTime);
      if (!isDataInRange(idx, segStartTime, segEndTime)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Use AVERAGE ALIGN to calculate PAA feature. In addition, PAA requires every segment has at
   * least one point. If {@linkplain #windowRange} isn't divided by {@linkplain #alignedDim}, PAA
   * discards the rest to guarantee the lower-bounding property.
   *
   * @param startIdx the idx from which we start to search the closest timestamp.
   * @param windowStartTime the left border of sequence to be aligned.
   */
  @Override
  protected TVList createAlignedSequence(long windowStartTime, int startIdx) {
    TVList seq = TVListAllocator.getInstance().allocate(TSDataType.FLOAT);
    int windowStartIdx = locatedIdxToTimestamp(startIdx, windowStartTime);
    long windowEndTime = windowStartTime + alignedDim * intervalWidth - 1;
    if (!checkValid(windowStartIdx, windowStartTime, windowEndTime)) {
      return seq;
    }
    int segStartIdx = windowStartIdx;
    int segEndIdx;
    long segmentStartTime = windowStartTime;
    long segmentEndTime;
    for (int i = 0; i < alignedDim; i++) {
      segStartIdx = locatedIdxToTimestamp(segStartIdx, segmentStartTime);
      segmentEndTime = segmentStartTime + intervalWidth - 1;
      segEndIdx = locatedIdxToTimestamp(segStartIdx, segmentEndTime + 1) - 1;
      float sum = 0;
      for (int idx = segStartIdx; idx <= segEndIdx; idx++) {
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
      seq.putFloat(segmentStartTime, sum / (segEndIdx - segStartIdx + 1));
      segmentStartTime += intervalWidth;
    }
    return seq;
  }

  /**
   * custom for {@linkplain PAAIndex}
   *
   * @param currentCoordinates current corners
   */
  void copyFeature(float[] currentCoordinates) {
    if (currentAligned.size() != currentCoordinates.length) {
      throw new IndexRuntimeException(String.format("paa aligned.size %d != corners.length %d",
          currentAligned.size(), currentCoordinates.length));
    }
    for (int i = 0; i < currentCoordinates.length; i++) {
      currentCoordinates[i] = IndexUtils.getFloatFromAnyType(currentAligned, i);
    }
  }

  /**
   * custom for {@linkplain PAAIndex}
   *
   * @param idx the idx-th identifiers
   * @param outputStream to output
   */
  void serializeIdentifier(Integer idx, OutputStream outputStream) throws IOException {
    int actualIdx = idx - flushedOffset;
    if (actualIdx * 3 + 2 >= identifierList.size()) {
      throw new IOException(String.format("PAA serialize: idx %d*3+2 > identifiers size %d", idx,
          identifierList.size()));
    }
    if (!storeIdentifier) {
      throw new IOException("In PAA index, must store the identifier list");
    }
    Identifier identifier = new Identifier(identifierList.getLong(actualIdx * 3),
        identifierList.getLong(actualIdx * 3 + 1),
        (int) identifierList.getLong(actualIdx * 3 + 2));
    identifier.serialize(outputStream);
  }
}
