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
package org.apache.iotdb.db.index.preprocess;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * COUNT_FIXED is very popular in the preprocessing phase. Most previous researches build indexes on
 * sequences with the same length (a.k.a. COUNT_FIXED). The timestamp is very important for time
 * series, although a rich line of researches ignore the time information directly.<p>
 *
 * Note that, in real scenarios, the time series could be variable-frequency, traditional distance
 * metrics (e.g., Euclidean distance) may not make sense for COUNT_FIXED.
 */
public class CountFixedFeatureExtractor extends IndexFeatureExtractor {

  protected final boolean storeIdentifier;
  protected final boolean storeAligned;
  /**
   * how many sliding windows we have already pre-processed. If we have process 2 windows, sliceNum
   * = 1
   */
  protected int sliceNum;
  /**
   * the amount of sliding windows in srcData
   */
  protected int totalProcessedCount;


  protected PrimitiveList identifierList;
  private PrimitiveList alignedList;
  /**
   * the position in srcData of the first data point of the current sliding window
   */
  protected int currentStartTimeIdx;
  private long currentStartTime;
  private long currentEndTime;
  /**
   * The number of processed points up to the last ForcedFlush. ForcedFlush does not change current
   * information (e.g. {@code currentProcessedIdx}, {@code currentStartTime}), but when reading or
   * calculating L1~L3 features, we should carefully subtract {@code lastFlushIdx} from {@code
   * currentProcessedIdx}.
   */
  protected int flushedOffset = 0;
  private long chunkStartTime = -1;
  private long chunkEndTime = -1;
  private int processedStartTimeIdx = -1;

  /**
   * Create CountFixedPreprocessor
   *
   * @param tsDataType data type
   * @param windowRange the sliding window range
   * @param slideStep the update size
   * @param storeIdentifier true if we need to store all identifiers. The cost will be counted.
   */
  public CountFixedFeatureExtractor(TSDataType tsDataType, int windowRange, int slideStep,
      boolean storeIdentifier, boolean storeAligned, boolean inQueryMode) {
    super(tsDataType, WindowType.COUNT_FIXED, windowRange, slideStep, inQueryMode);
    this.storeIdentifier = storeIdentifier;
    this.storeAligned = storeAligned;
  }

  public CountFixedFeatureExtractor(TSDataType tsDataType, int windowRange, int slideStep,
      boolean storeIdentifier, boolean storeAligned) {
    this(tsDataType, windowRange, slideStep, storeIdentifier, storeAligned, false);
//    this.storeIdentifier = storeIdentifier;
//    this.storeAligned = storeAligned;
  }

//  public CountFixedFeatureExtractor(TSDataType tsDataType, int windowRange, int slideStep) {
//    this(tsDataType, windowRange, slideStep, true, true);
//  }
//
//  public CountFixedFeatureExtractor(TSDataType tsDataType, int windowRange) {
//    this(tsDataType, windowRange, 1, true, true);
//  }

  @Override
  protected void initParams() {
    sliceNum = 0;
    this.totalProcessedCount = (srcData.size() - this.windowRange) / slideStep + 1;
    currentStartTimeIdx = -slideStep;

    // init the L1 identifier
    if (storeIdentifier) {
      this.identifierList = PrimitiveList.newList(TSDataType.INT64);
    }
    //init L2
    if (storeAligned) {
      this.alignedList = PrimitiveList.newList(TSDataType.INT32);
    }
  }

  @Override
  public boolean hasNext() {
    return hasNext(null);
  }

  public boolean hasNext(Filter timeFilter) {
    int startIdx = currentStartTimeIdx;
    int endIdx = startIdx + windowRange - 1;

    while (endIdx < srcData.size()) {
      startIdx += slideStep;
      endIdx += slideStep;
      if (endIdx < srcData.size()) {
        long startTime = srcData.getTime(startIdx);
        long endTime = srcData.getTime(endIdx);
        if (timeFilter == null || timeFilter.containStartEndTime(startTime, endTime)) {
          currentStartTimeIdx = startIdx;
          currentStartTime = srcData.getTime(startIdx);
          currentEndTime = srcData.getTime(endIdx);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void processNext() {
    sliceNum++;
    if (chunkStartTime == -1) {
      chunkStartTime = currentStartTime;
    }
    processedStartTimeIdx = currentStartTimeIdx;
    chunkEndTime = currentEndTime;

    // calculate the newest aligned sequence
//    System.out.println(String.format("write ELB: %d - %d, %d", currentStartTime, currentEndTime, windowRange));
    if (storeIdentifier) {
      // it's a naive identifier, we can refine it in the future.
      identifierList.putLong(currentStartTime);
      identifierList.putLong(currentEndTime);
      identifierList.putLong(windowRange);
    }
    if (storeAligned) {
      alignedList.putInt(currentStartTimeIdx);
    }
  }

  @Override
  public int getCurrentChunkOffset() {
    return flushedOffset;
  }

  @Override
  public int getCurrentChunkSize() {
    return sliceNum - flushedOffset;
  }

//  @Override
//  public List<Identifier> getLatestN_L1_Identifiers(int latestN) {
//    latestN = Math.min(getCurrentChunkSize(), latestN);
//    List<Identifier> res = new ArrayList<>(latestN);
//    if (latestN == 0) {
//      return res;
//    }
//    if (storeIdentifier) {
//      int startIdx = sliceNum - latestN;
//      for (int i = startIdx; i < sliceNum; i++) {
//        int actualIdx = i - flushedOffset;
//        Identifier identifier = new Identifier(
//            identifierList.getLong(actualIdx * 3),
//            identifierList.getLong(actualIdx * 3 + 1),
//            (int) identifierList.getLong(actualIdx * 3 + 2));
//        res.add(identifier);
//      }
//      return res;
//    }
//    int startIdxPastN = currentStartTimeIdx - (latestN - 1) * slideStep;
//    while (startIdxPastN >= 0 && startIdxPastN <= currentStartTimeIdx) {
//      res.add(new Identifier(srcData.getTime(startIdxPastN),
//          srcData.getTime(startIdxPastN + windowRange - 1), windowRange));
//      startIdxPastN += slideStep;
//    }
//    return res;
//  }

  @Override
  public List<Object> getLatestN_L2_AlignedSequences(int latestN) {
    latestN = Math.min(getCurrentChunkSize(), latestN);
    List<Object> res = new ArrayList<>(latestN);
    if (latestN == 0) {
      return res;
    }
    int startIdxPastN = Math.max(0, currentStartTimeIdx - (latestN - 1) * slideStep);
    while (startIdxPastN <= currentStartTimeIdx) {
      res.add(createAlignedSequence(startIdxPastN));
      startIdxPastN += slideStep;
    }
    return res;
  }

  @Override
  public long getChunkStartTime() {
    return chunkStartTime;
  }

  @Override
  public long getChunkEndTime() {
    return chunkEndTime;
  }

  /**
   * For COUNT-FIXED preprocessor, given the original data and the window range, we can determine an
   * aligned sequence only by the startIdx. Note that this method is only applicable to
   * preprocessors which do not transform original data points.
   */
  protected TVList createAlignedSequence(int startIdx) {
    TVList seq = TVListAllocator.getInstance().allocate(srcData.getDataType());
    for (int i = startIdx; i < startIdx + windowRange; i++) {
      switch (srcData.getDataType()) {
        case INT32:
          seq.putInt(srcData.getTime(i), srcData.getInt(i));
          break;
        case INT64:
          seq.putLong(srcData.getTime(i), srcData.getLong(i));
          break;
        case FLOAT:
          seq.putFloat(srcData.getTime(i), srcData.getFloat(i));
          break;
        case DOUBLE:
          seq.putDouble(srcData.getTime(i), srcData.getDouble(i));
          break;
        default:
          throw new NotImplementedException(srcData.getDataType().toString());
      }
    }
    return seq;
  }

  public TVList getSrcData() {
    return srcData;
  }

  @Override
  public long clear() {
    long toBeReleased = 0;
    flushedOffset = sliceNum;
    chunkStartTime = -1;
    chunkEndTime = -1;

    if (identifierList != null) {
      toBeReleased += identifierList.size() * Long.BYTES;
      identifierList.clearAndRelease();
    }
    if (alignedList != null) {
      toBeReleased += alignedList.size() * Integer.BYTES;
      alignedList.clearAndRelease();
    }
    return toBeReleased;
  }

  @Override
  public int getAmortizedSize() {
    int res = 0;
    if (storeIdentifier) {
      res += 3 * Long.BYTES;
    }
    if (storeAligned) {
      res += Integer.BYTES;
    }
    return res;
  }

  @Override
  public int nextUnprocessedWindowStartIdx() {
    int next = processedStartTimeIdx + slideStep;
    if (next > srcData.size()) {
//      next = srcData.size();
      next = processedStartTimeIdx;
    }
    return next;
  }

  public int getSliceNum() {
    return sliceNum;
  }

}
