package org.apache.iotdb.db.index.preprocess;

import static org.apache.iotdb.db.index.common.IndexUtils.getDataTypeSize;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * In TIME_FIXED, the sliding window always has a fixed time range.  However, since the time series
 * may be frequency-variable, it’s not fixed for the distribution and number of real data points in
 * each sliding window. For indexes processing data with the same dimension, they need to calculate
 * L2_aligned_sequence, whose dimension is specified by the input parameter {@code alignedDim}. Up
 * to now, we adopt the nearest-point alignment rule.
 */
public class TimeFixedPreprocessor extends IndexPreprocessor {

  protected final boolean storeIdentifier;
  protected final boolean storeAligned;
  /**
   * The dimension of the aligned subsequence. -1 means not aligned.
   */
  protected final int alignedDim;
  /**
   * how many subsequences we have pre-processed
   */
  private int currentProcessedIdx;
  /**
   * the amount of subsequences in srcData
   */
  private int totalProcessedCount;
  private int scanIdx;
  private PrimitiveList identifierList;
  private long currentStartTime;
  private long currentEndTime;
  private ArrayList<TVList> alignedList;
  private TVList currentAligned;
  protected int intervalWidth;

  /**
   * The idx of the last flush. ForcedFlush does not change current information (e.g. {@code
   * currentProcessedIdx}, {@code currentStartTime}), but when reading or calculating L1~L3
   * features, we should carefully subtract {@code lastFlushIdx} from {@code currentProcessedIdx}.
   */
  private int flushedOffset = 0;

  /**
   * Create TimeFixedPreprocessor
   *
   * @param srcData cannot be empty or null
   * @param windowRange the sliding window range
   * @param alignedDim the length of sequence after alignment
   * @param slideStep the update size
   * @param storeIdentifier true if we need to store all identifiers. The cost will be counted.
   * @param storeAligned true if we need to store all aligned sequences. The cost will be counted.
   */
  public TimeFixedPreprocessor(TVList srcData, int windowRange, int alignedDim,
      int slideStep, boolean storeIdentifier, boolean storeAligned) {
    super(srcData, WindowType.COUNT_FIXED, windowRange, slideStep);
    this.storeIdentifier = storeIdentifier;
    this.storeAligned = storeAligned;
    this.alignedDim = alignedDim;
    initTimeFixedParams();
  }

  public TimeFixedPreprocessor(TVList srcData, int windowRange, int alignedDim,
      int slideStep) {
    this(srcData, windowRange, slideStep, alignedDim, true, true);
  }

  public TimeFixedPreprocessor(TVList srcData, int windowRange, int alignedDim) {
    this(srcData, windowRange, alignedDim, 1, true, true);
  }

  private void initTimeFixedParams() {
    scanIdx = 0;
    currentProcessedIdx = -1;
    this.intervalWidth = windowRange / alignedDim;
    long startTime = srcData.getTime(0);
    long endTime = srcData.getLastTime();
    this.totalProcessedCount = (int) ((endTime - startTime + 1 - this.windowRange) / slideStep + 1);
    // init the L1 identifier
    currentStartTime = startTime - slideStep;
    currentEndTime = startTime + windowRange - slideStep;
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
      this.alignedList = new ArrayList<>(totalProcessedCount / 2 + 1);
    } else {
      this.alignedList = new ArrayList<>(1);
    }
  }

  @Override
  public boolean hasNext() {
    return currentProcessedIdx + 1 < totalProcessedCount;
  }

  @Override
  public void processNext() {
    currentProcessedIdx++;
    currentStartTime += slideStep;
    currentEndTime += slideStep;
    // calculate the newest aligned sequence

    if (storeIdentifier) {
      // it's a naive identifier, we can refine it in the future.
      identifierList.putLong(currentStartTime);
      identifierList.putLong(currentEndTime);
      identifierList.putLong(alignedDim);
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

  @Override
  public int getCurrentChunkOffset() {
    return flushedOffset;
  }

  @Override
  public int getCurrentChunkSize() {
    return currentProcessedIdx - flushedOffset + 1;
  }

  /**
   * Move {@code curIdx} to a right position from which to start scanning. Current implementation is
   * to find the timestamp closest to {@code targetTimestamp}.<p>
   *
   * For easy expansion, users could just override this function and {@linkplain
   * #createAlignedSequence(long, int)}.
   *
   * @param curIdx start idx
   * @param targetTimestamp the target
   * @return the closest idx
   */
  protected int locatedIdxToTimestamp(int curIdx, long targetTimestamp) {
    long curDelta = Math.abs(srcData.getTime(curIdx) - targetTimestamp);
    long nextDelta;
    while (curIdx < srcData.size() - 1 && curDelta >
        (nextDelta = Math.abs(srcData.getTime(curIdx + 1) - targetTimestamp))) {
      curDelta = nextDelta;
      curIdx++;
    }
    return curIdx;
  }

  /**
   * Use CLOSEST ALIGN, a naive method not involving average calculation.
   *
   * @param startIdx the idx from which we start to search the closest timestamp.
   * @param leftBorderTimestamp the left border of sequence to be aligned.
   */
  protected TVList createAlignedSequence(long leftBorderTimestamp, int startIdx) {
    TVList seq = TVListAllocator.getInstance().allocate(srcData.getDataType());
    for (int i = 0; i < alignedDim; i++) {
      startIdx = locatedIdxToTimestamp(startIdx, leftBorderTimestamp);
      switch (srcData.getDataType()) {
        case INT32:
          seq.putInt(leftBorderTimestamp, srcData.getInt(startIdx));
          break;
        case INT64:
          seq.putLong(leftBorderTimestamp, srcData.getLong(startIdx));
          break;
        case FLOAT:
          seq.putFloat(leftBorderTimestamp, srcData.getFloat(startIdx));
          break;
        case DOUBLE:
          seq.putDouble(leftBorderTimestamp, srcData.getDouble(startIdx));
          break;
        default:
          throw new NotImplementedException(srcData.getDataType().toString());
      }
      leftBorderTimestamp += intervalWidth;
    }
    return seq;
  }

  @Override
  public List<Object> getLatestN_L1_Identifiers(int latestN) {
    latestN = Math.min(getCurrentChunkSize(), latestN);
    List<Object> res = new ArrayList<>(latestN);
    if (latestN == 0) {
      return res;
    }
    if (storeIdentifier) {
      int startIdx = currentProcessedIdx + 1 - latestN;
      for (int i = startIdx; i <= currentProcessedIdx; i++) {
        int actualIdx = i - flushedOffset;
        Identifier identifier = new Identifier(
            identifierList.getLong(actualIdx * 3),
            identifierList.getLong(actualIdx * 3 + 1),
            (int) identifierList.getLong(actualIdx * 3 + 2));
        res.add(identifier);
      }
      return res;
    }
    long startTimePastN = Math.max(currentStartTime - (latestN - 1) * slideStep,
        srcData.getTime(0));
    while (startTimePastN <= currentStartTime) {
      res.add(new Identifier(startTimePastN, startTimePastN + windowRange,
          alignedDim));
      startTimePastN += slideStep;
    }
    return res;
  }


  @Override
  public List<Object> getLatestN_L2_AlignedSequences(int latestN) {
    latestN = Math.min(getCurrentChunkSize(), latestN);
    List<Object> res = new ArrayList<>(latestN);
    if (latestN == 0 || alignedDim == -1) {
      return res;
    }
    if (storeAligned) {
      int startIdx = currentProcessedIdx + 1 - latestN;
      for (int i = startIdx; i <= currentProcessedIdx; i++) {
        res.add(alignedList.get(i - flushedOffset).clone());
      }
      return res;
    }
    int startIdx = 0;
    long startTimePastN = Math.max(currentStartTime - (latestN - 1) * slideStep,
        srcData.getTime(0));
    while (startTimePastN <= currentStartTime) {
      startIdx = locatedIdxToTimestamp(startIdx, startTimePastN);
      TVList seq = createAlignedSequence(startTimePastN, startIdx);
      res.add(seq);
      startTimePastN += slideStep;
    }
    return res;
  }

  @Override
  public long clear() {
    flushedOffset = currentProcessedIdx + 1;
    long toBeReleased = 0;
    if (identifierList != null) {
      toBeReleased += identifierList.size() * Long.BYTES;
      identifierList.clearAndRelease();
    }
    if (alignedDim != -1 && storeAligned) {
      for (TVList tv : alignedList) {
        toBeReleased += getDataTypeSize(srcData.getDataType()) * alignedDim;
        TVListAllocator.getInstance().release(tv);
      }
      alignedList.clear();
    }
    return toBeReleased;
  }

  @Override
  public int getAmortizedSize() {
    int res = 0;
    if (storeAligned) {
      res += 3 * Long.BYTES;
    }
    if (alignedDim != -1 && storeAligned) {
      res += getDataTypeSize(srcData.getDataType()) * alignedDim;
    }
    return res;
  }
}
