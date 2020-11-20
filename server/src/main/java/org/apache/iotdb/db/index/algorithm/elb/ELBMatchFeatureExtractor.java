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
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBType;
import org.apache.iotdb.db.index.algorithm.elb.ELBFeatureExtractor.ELBWindowBlockFeature;
import org.apache.iotdb.db.index.preprocess.CountFixedFeatureExtractor;
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
public class ELBMatchFeatureExtractor extends CountFixedFeatureExtractor {

  //  private final int blockNum;
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
  public ELBMatchFeatureExtractor(TSDataType tsDataType, int windowRange, int blockWidth,
      ELBType elbType, boolean inQueryMode) {
    super(tsDataType, -1, -1, false, false, inQueryMode);
    this.elbType = elbType;
    this.blockWidth = blockWidth;
    this.windowBlockFeatures = new ArrayList<>();
    if (inQueryMode) {
      this.windowRange = windowRange; // in query, it's query length.
      this.slideStep = 1; // in query, we need scan every windows
      if (blockWidth > windowRange) {
        throw new IllegalIndexParamException(
            String.format("In ELB, blockWidth %d cannot be larger than windowRange %d", blockWidth,
                windowRange));
      }
    } else {
      this.windowRange = blockWidth;
      this.slideStep = blockWidth;
    }
  }

  @Override
  public void processNext() {
    super.processNext();
    if (!inQueryMode) {
      if (currentStartTimeIdx % blockWidth != 0) {
        throw new IndexRuntimeException("not divided clearly");
      }
      // loop until all blocks in current sliding window are computed.
//      int startIdx = windowBlockFeatures.size() * blockWidth;
//      int windowEndIdx = currentStartTimeIdx + windowRange;
//      while (startIdx + blockWidth <= windowEndIdx) {
      double f = ELBFeatureExtractor
          .calcWindowBlockFeature(elbType, srcData, currentStartTimeIdx, blockWidth);
      // A window block starting from index {@code idx} corresponds {@code L/b} adjacent windows:
      // the time range of the 1-st window: [srcData[idx], srcData[idx+range-1]]
      // the time range of the L/b-th window: [srcData[idx+width], srcData[idx+width+range-1]]
      // Therefore, this window block covers time range [srcData[idx], srcData[idx+width+range-1]]
      long startCoverTime = srcData.getTime(currentStartTimeIdx);
      int endIdx = currentStartTimeIdx + blockWidth - 1;
      if (endIdx >= srcData.size()) {
        throw new IndexRuntimeException("why the block exceeds?");
      }
      long endCoverTime = srcData.getTime(endIdx);
      windowBlockFeatures.add(new ELBWindowBlockFeature(startCoverTime, endCoverTime, f));
    }
  }

  /**
   * custom for {@linkplain ELBIndexNotGood}
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

  static List<ELBWindowBlockFeature> deserializeFeatures(ByteBuffer indexChunkData) {
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
