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
package org.apache.iotdb.db.index.feature;

import org.apache.iotdb.db.exception.index.IllegalIndexParamException;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.index.common.IndexUtils.getDataTypeSize;

/**
 * In TIME_FIXED, the sliding window always has a fixed time range. However, since the time series
 * may be frequency-variable, it’s not fixed for the distribution and number of real data points in
 * each sliding window. For indexes processing data with the same dimension, they need to calculate
 * L2_aligned_sequence, whose dimension is specified by the input parameter {@code alignedDim}. Up
 * to now, we adopt the nearest-point alignment rule.
 *
 * <p>TIME_FIXED preprocessor need set {@code baseTime}. For arbitrary window, the startTime is
 * divisible by {@code timeAnchor}
 */
public class TimeFixedFeatureExtractor extends SubMatchFeatureExtractor {
  /** the window range, it depends on the window type. */
  protected final int windowRange;
  /**
   * the the offset between two adjacent subsequences (a.k.a. the update size), depending on the the
   * window type.
   */
  protected final int slideStep;
  /**
   * To anchor the start time of sliding window. Any window start time should be congruence with
   * timeAnchor on {@code slideStep}
   */
  private final long timeAnchor;

  protected final boolean storeIdentifier;
  protected final boolean storeAligned;
  /** The dimension of the aligned subsequence. -1 means not aligned. */
  protected final int alignedDim;
  /** how many subsequences we have pre-processed */
  private int sliceNum;

  private int scanIdx;
  protected PrimitiveList identifierList;
  protected long currentStartTime;
  private long currentEndTime;
  private ArrayList<TVList> alignedList;
  protected TVList currentAligned;
  protected int intervalWidth;

  /**
   * The idx of the last flush. ForcedFlush does not change current information (e.g. {@code
   * currentProcessedIdx}, {@code currentStartTime}), but when reading or calculating L1~L3
   * features, we should carefully subtract {@code lastFlushIdx} from {@code currentProcessedIdx}.
   */
  protected int flushedOffset;

  private long chunkStartTime = -1;
  private long chunkEndTime = -1;
  private long processedStartTime;


  /**
   * Create TimeFixedPreprocessor
   *
   * @param tsDataType data type
   * @param windowRange the sliding window range
   * @param alignedDim the length of sequence after alignment
   * @param slideStep the update size
   * @param storeIdentifier true if we need to store all identifiers. The cost will be counted.
   * @param storeAligned true if we need to store all aligned sequences. The cost will be counted.
   */
  public TimeFixedFeatureExtractor(
      TSDataType tsDataType,
      int windowRange,
      int slideStep,
      int alignedDim,
      long timeAnchor,
      boolean storeIdentifier,
      boolean storeAligned,
      boolean inQueryMode) {
    super(tsDataType, inQueryMode);
    this.storeIdentifier = storeIdentifier;
    this.storeAligned = storeAligned;
    this.alignedDim = alignedDim;
    this.timeAnchor = timeAnchor;
    if (slideStep > windowRange) {
      throw new IllegalIndexParamException(
          "Sorry, We do not yet support the slide step larger than the window range. "
              + "It may miss the information of some data points");
    }
    this.windowRange = windowRange;
    this.slideStep = slideStep;
  }

  public TimeFixedFeatureExtractor(
      TSDataType tsDataType,
      int windowRange,
      int slideStep,
      int alignedDim,
      long timeAnchor,
      boolean storeIdentifier,
      boolean storeAligned) {
    this(
        tsDataType,
        windowRange,
        slideStep,
        alignedDim,
        timeAnchor,
        storeIdentifier,
        storeAligned,
        false);
  }

  public TimeFixedFeatureExtractor(
      TSDataType tsDataType, int windowRange, int alignedDim, int slideStep, long timeAnchor) {
    this(tsDataType, windowRange, slideStep, alignedDim, timeAnchor, true, true, false);
  }

  @Override
  protected void initParams() {
    scanIdx = 0;
    sliceNum = 0;
    flushedOffset = 0;
    this.intervalWidth = windowRange / alignedDim;
    long startTime = srcData.getTime(0);
    startTime = startTime - startTime % slideStep + timeAnchor % slideStep;
    while (startTime > srcData.getTime(0)) {
      startTime -= slideStep;
    }

    // init the L1 identifier
    currentStartTime = startTime - slideStep;
    currentEndTime = startTime + windowRange - slideStep - 1;
    if (storeIdentifier) {
      this.identifierList = PrimitiveList.newList(TSDataType.INT64);
    }
    // init the L2 aligned sequence
    if (alignedDim != -1 && storeAligned) {
      /*
      We assume that in most cases, since multiple indexes are built in parallel, the building of
      each index will be divided into several parts. If the memory is enough, ArrayList only needs
      to be expanded once.
      */
      int estimateTotal =
          (int) ((srcData.getLastTime() - startTime + 1 - this.windowRange) / slideStep + 1);
      this.alignedList = new ArrayList<>(estimateTotal / 2 + 1);
    } else {
      this.alignedList = new ArrayList<>(1);
    }
  }

  @Override
  public boolean hasNext() {
    return hasNext(null);
  }

  public boolean hasNext(Filter timeFilter) {
    long startTime = currentStartTime;
    long endTime = currentEndTime;
    int startIdx = scanIdx;
    while (endTime <= srcData.getLastTime()) {
      startTime += slideStep;
      endTime += slideStep;
      if (endTime > srcData.getLastTime()) {
        return false;
      }
      startIdx = locatedIdxToTimestamp(startIdx, startTime);
      if (checkValid(startIdx, startTime, endTime)) {
        if (timeFilter == null || timeFilter.containStartEndTime(startTime, endTime)) {
          scanIdx = startIdx;
          currentStartTime = startTime;
          currentEndTime = endTime;
          return true;
        }
      }
    }
    return false;
  }

  protected boolean checkValid(int startIdx, long startTime, long endTime) {
    return isDataInRange(startIdx, startTime, endTime);
  }

  protected boolean isDataInRange(int idx, long startTime, long endTime) {
    return idx < srcData.size()
        && srcData.getTime(idx) >= startTime
        && srcData.getTime(idx) <= endTime;
  }

  @Override
  public void processNext() {
    sliceNum++;
    processedStartTime = currentStartTime;
    if (chunkStartTime == -1) {
      chunkStartTime = currentStartTime;
    }
    chunkEndTime = currentEndTime;
    // calculate the newest aligned sequence

    //    System.out.println(String.format("write PAA: %d - %d, %d", currentStartTime,
    // currentEndTime, alignedDim));
    if (storeIdentifier) {
      // it's a naive identifier, we can refine it in the future.
      identifierList.putLong(currentStartTime);
      identifierList.putLong(currentEndTime);
      int currentStartTimeIdx = locatedIdxToTimestamp(scanIdx, currentStartTime);
      int pointSize = locatedIdxToTimestamp(scanIdx, currentEndTime + 1) - currentStartTimeIdx;
      identifierList.putLong(pointSize);
    }
    if (alignedDim != -1) {
      if (currentAligned != null) {
        TVListAllocator.getInstance().release(currentAligned);
      }
      scanIdx = locatedIdxToTimestamp(scanIdx, currentStartTime);
      currentAligned = createAlignedSequence(currentStartTime, scanIdx);
      if (storeAligned) {
        alignedList.add(currentAligned.clone());
      }
    }
  }

  private int getCurrentChunkSize() {
    return sliceNum - flushedOffset;
  }

  /**
   * Find the idx of the minimum timestamp greater than or equal to {@code targetTimestamp}. If not,
   * return the idx of the timestamp closest to {@code targetTimestamp}.
   *
   * @param curIdx the idx to start scanning
   * @param targetTimestamp the target
   * @return the idx of the minimum timestamp >= {@code targetTimestamp}
   */
  private int locatedIdxToTimestamp(int curIdx, long targetTimestamp) {
    while (curIdx < srcData.size() && srcData.getTime(curIdx) < targetTimestamp) {
      curIdx++;
    }
    return curIdx;
  }

  /**
   * Use CLOSEST ALIGN, a naive method not involving average calculation.
   *
   * @param startIdx the idx from which we start to search the closest timestamp.
   * @param windowStartTime the left border of sequence to be aligned.
   */
  protected TVList createAlignedSequence(long windowStartTime, int startIdx) {
    TVList seq = TVListAllocator.getInstance().allocate(srcData.getDataType());
    int windowStartIdx = locatedIdxToTimestamp(startIdx, windowStartTime);
    long windowEndTime = windowStartTime + windowRange - 1;
    if (!checkValid(windowStartIdx, windowStartTime, windowEndTime)) {
      return seq;
    }
    int windowEndIdx = locatedIdxToTimestamp(windowStartIdx, windowEndTime + 1) - 1;
    int segIdx = windowStartIdx;
    long segmentStartTime = windowStartTime;
    for (int i = 0; i < alignedDim; i++) {
      segIdx = locatedIdxToTimestamp(segIdx, segmentStartTime);
      // The original TimeFixedProcessor allows empty segments. An empty segment will select points
      // from its adjacent segment.
      segIdx = Math.min(segIdx, windowEndIdx);
      switch (srcData.getDataType()) {
        case INT32:
          seq.putInt(segmentStartTime, srcData.getInt(segIdx));
          break;
        case INT64:
          seq.putLong(segmentStartTime, srcData.getLong(segIdx));
          break;
        case FLOAT:
          seq.putFloat(segmentStartTime, srcData.getFloat(segIdx));
          break;
        case DOUBLE:
          seq.putDouble(segmentStartTime, srcData.getDouble(segIdx));
          break;
        default:
          throw new NotImplementedException(srcData.getDataType().toString());
      }
      segmentStartTime += intervalWidth;
    }
    return seq;
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
  //    long startTimePastN = currentStartTime - (latestN - 1) * slideStep;
  //    while (startTimePastN <= currentStartTime) {
  //      int startTimePastNIdx = locatedIdxToTimestamp(0, startTimePastN);
  //      int pointSize = locatedIdxToTimestamp(startTimePastNIdx, startTimePastN + windowRange)
  //          - startTimePastNIdx;
  //      res.add(new Identifier(startTimePastN, startTimePastN + windowRange - 1,
  //          pointSize));
  //      startTimePastN += slideStep;
  //    }
  //    return res;
  //  }

  @Override
  public List<Object> getLatestN_L2_AlignedSequences(int latestN) {
    latestN = Math.min(getCurrentChunkSize(), latestN);
    List<Object> res = new ArrayList<>(latestN);
    if (latestN == 0 || alignedDim == -1) {
      return res;
    }
    if (storeAligned) {
      int startIdx = sliceNum - latestN;
      for (int i = startIdx; i < sliceNum; i++) {
        res.add(alignedList.get(i - flushedOffset).clone());
      }
      return res;
    }
    int startIdx = 0;
    long startTimePastN = currentStartTime - (latestN - 1) * slideStep;
    while (startTimePastN <= currentStartTime) {
      startIdx = locatedIdxToTimestamp(startIdx, startTimePastN);
      TVList seq = createAlignedSequence(startTimePastN, startIdx);
      res.add(seq);
      startTimePastN += slideStep;
    }
    return res;
  }

//  @Override
//  public long getChunkStartTime() {
//    return chunkStartTime;
//  }
//
//  @Override
//  public long getChunkEndTime() {
//    return chunkEndTime;
//  }

  @Override
  protected long release() {
    long toBeReleased = 0;
    flushedOffset = sliceNum;
    chunkStartTime = -1;
    chunkEndTime = -1;
    if (identifierList != null) {
      toBeReleased += identifierList.size() * Long.BYTES;
      identifierList.clearAndRelease();
    }
    if (alignedDim != -1 && alignedList != null) {
      for (TVList tv : alignedList) {
        toBeReleased += getDataTypeSize(srcData.getDataType()) * alignedDim;
        TVListAllocator.getInstance().release(tv);
      }
      alignedList.clear();
    }
    return toBeReleased;
  }

  @Override
  public int nextUnprocessedWindowStartIdx() {
    int next = locatedIdxToTimestamp(0, processedStartTime + slideStep);
    if (next >= srcData.size()) {
      next = srcData.size();
    }
    return next;
  }
}
