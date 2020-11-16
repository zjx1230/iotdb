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

import java.util.List;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.db.index.preprocess.IndexFeatureExtractor;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * For PAA feature in the whole matching. A simplified version all PAA PAA (Piecewise Aggregate
 * Approximation), a classical feature in time series. <p>
 *
 * Refer to: Keogh Eamonn, et al. "Dimensionality reduction for fast similarity search in large time
 * series databases." Knowledge and information Systems 3.3 (2001): 263-286.
 */
public class PAAWholeFeatureExtractor extends IndexFeatureExtractor {

  private static final String NON_SUPPORT_MSG = "For whole matching, it's not supported";
  private final int alignedLength;
  private final int paaDim;
  private final int paaWidth;
  private boolean hasNewData;

  public PAAWholeFeatureExtractor(TSDataType dataType, int alignedLength, int paaDim) {
    super(dataType, WindowType.WHOLE_MATCH, alignedLength, -1);
    this.alignedLength = alignedLength;
    this.paaDim = paaDim;
    this.paaWidth = alignedLength / paaDim;
    this.hasNewData = false;
  }

  @Override
  protected void initParams() {
    // Do nothing
    hasNewData = true;
  }

  @Override
  public boolean hasNext() {
    return hasNewData;
  }

  @Override
  public Identifier getCurrent_L1_Identifier() {
    return new Identifier(srcData.getMinTime(), srcData.getLastTime(), srcData.size());
  }

  @Override
  public Object getCurrent_L2_AlignedSequence() {
    return srcData;
  }

  @Override
  public Object getCurrent_L3_Feature() {
    PrimitiveList seq = PrimitiveList.newList(TSDataType.FLOAT);
    for (int i = 0; i < paaDim; i++) {
      float sum = 0;
      for (int j = 0; j <  paaWidth; j++) {
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
      seq.putFloat(sum / paaWidth);
    }
    return seq;
  }

  @Override
  public boolean hasNext(Filter timeFilter) {
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public void processNext() {
    hasNewData = false;
  }

  @Override
  public int getCurrentChunkOffset() {
//    return 0;
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public int getCurrentChunkSize() {
//    return 0;
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public List<Identifier> getLatestN_L1_Identifiers(int latestN) {
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public List<Object> getLatestN_L2_AlignedSequences(int latestN) {
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public long getChunkStartTime() {
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public long getChunkEndTime() {
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public long clear() {
    return 0;
  }

  @Override
  public int getAmortizedSize() {
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }

  @Override
  public int nextUnprocessedWindowStartIdx() {
    throw new UnsupportedOperationException(NON_SUPPORT_MSG);
  }
}
