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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBWindowBlockFeature;
import org.apache.iotdb.db.index.preprocess.CountFixedPreprocessor;
import org.apache.iotdb.db.index.preprocess.Identifier;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

/**
 * <p>A preprocessor for ELB Matching which calculates the mean value of a list of adjacent blocks
 * over stream/long series.</p>
 *
 * <p>ELB-Match is a scan-based method for accelerating the subsequence similarity query. It stores
 * the block features during the index building phase.</p>
 *
 * <p>Refer to: Kang R, et al. Matching Consecutive Subpatterns over Streaming Time Series[C]
 * APWeb-WAIM Joint International Conference. Springer, Cham, 2018: 90-105.</p>
 */
public class ELBMatchPreprocessor extends CountFixedPreprocessor {

  private final int blockNum;
  private final int blockWidth;

  private final List<ELBWindowBlockFeature> windowBlockFeatures;

  private final ELBType elbType;

  /**
   * ELB divides stream/long series into window-blocks with length of {@code L/b}, where {@code L}
   * is the window range and {@code b} is the feature dimension. For each window-block, ELB
   * calculates the meaning value as its feature.<p>
   *
   * A block contains {@code L/b} points. {@code b} blocks cover adjacent {@code L/b} sliding
   * windows.
   */
  public ELBMatchPreprocessor(TSDataType tsDataType, int windowRange, int blockNum,
      ELBType elbType) {
    super(tsDataType, windowRange, 1, false, false);
    this.elbType = elbType;
    if (blockNum > windowRange) {
      throw new IllegalIndexParamException(String
          .format("In ELB, blockNum %d cannot be larger than windowRange %d", blockNum,
              windowRange));
    }
    this.blockNum = blockNum;
    this.blockWidth = Math.floorDiv(windowRange, blockNum);
    this.windowBlockFeatures = new ArrayList<>();
  }

  @Override
  public void processNext() {
    super.processNext();
    if (!inQueryMode && currentStartTimeIdx % blockWidth == 0) {
      // loop until all blocks in current sliding window are computed.
      int startIdx = windowBlockFeatures.size() * blockWidth;
      int windowEndIdx = currentStartTimeIdx + windowRange;
      while (startIdx + blockWidth <= windowEndIdx) {
        double f = ELBFeatureExtractor
            .calcWindowBlockFeature(elbType, srcData, currentStartTimeIdx, blockWidth);
        // A window block starting from index {@code idx} corresponds {@code L/b} adjacent windows:
        // the time range of the 1-st window: [srcData[idx], srcData[idx+range-1]]
        // the time range of the L/b-th window: [srcData[idx+width], srcData[idx+width+range-1]]
        // Therefore, this window block covers time range [srcData[idx], srcData[idx+width+range-1]]
        long startCoverTime = srcData.getTime(currentStartTimeIdx);
        int endIdx = currentStartTimeIdx + blockWidth + windowRange - 1;
        long endCoverTime = srcData.getTime(endIdx >= srcData.size() ? srcData.size() - 1: endIdx);
        windowBlockFeatures.add(new ELBWindowBlockFeature(startCoverTime, endCoverTime, f));
        startIdx += blockWidth;
      }
    }
  }

  /**
   * custom for {@linkplain ELBIndex}
   *
   * @param idx the idx-th identifiers
   * @param outputStream to output
   */
  void serializeIdentifier(Integer idx, OutputStream outputStream) throws IOException {
    int actualIdx = idx - flushedOffset;
    if (actualIdx * 3 + 2 >= identifierList.size()) {
      throw new IOException(String.format("ELB serialize: idx %d*3+2 > identifiers size %d", idx,
          identifierList.size()));
    }
    if (!storeIdentifier) {
      throw new IOException("In ELB index, must store the identifier list");
    }
    Identifier identifier = new Identifier(identifierList.getLong(actualIdx * 3),
        identifierList.getLong(actualIdx * 3 + 1),
        (int) identifierList.getLong(actualIdx * 3 + 2));
    identifier.serialize(outputStream);
  }

  public void serializeFeatures(ByteArrayOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(windowBlockFeatures.size(), outputStream);
    for (ELBWindowBlockFeature features : windowBlockFeatures) {
      ReadWriteIOUtils.write(features.startTime, outputStream);
      ReadWriteIOUtils.write(features.endTime, outputStream);
      ReadWriteIOUtils.write(features.feature, outputStream);
    }
  }

  static List<ELBWindowBlockFeature> deserializeFeatures(ByteBuffer indexChunkData)
      throws IOException {
    List<ELBWindowBlockFeature> res = new ArrayList<>();
    int size = ReadWriteIOUtils.readInt(indexChunkData);

    for (int i = 0; i < size; i++) {
      long startTime = ReadWriteIOUtils.readLong(indexChunkData);
      long endTime = ReadWriteIOUtils.readLong(indexChunkData);
      double feature = ReadWriteIOUtils.readDouble(indexChunkData);
      res.add(new ELBWindowBlockFeature(startTime, endTime, feature));
    }
    return res;
  }
}
