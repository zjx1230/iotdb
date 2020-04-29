package org.apache.iotdb.db.index.preprocess;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.primitive.PrimitiveList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * COUNT_FIXED is very popular in the preprocessing phase. Most previous researches build indexes on
 * sequences with the same length (a.k.a. COUNT_FIXED). The timestamp is very important for time
 * series, although a rich line of researches ignore the time information directly.<p>
 *
 * Note that, in real scenarios, the time series could be variable-frequency, traditional distance
 * metrics (e.g., Euclidean distance) may not make sense for COUNT_FIXED.
 */
public class CountFixedPreprocessor extends BasicPreprocessor {

  private final TVList srcData;
  protected final boolean storeIdentifier;
  protected final boolean storeAligned;
  /**
   * how many sliding windows we have already pre-processed
   */
  protected int currentProcessedIdx;
  /**
   * the amount of sliding windows in srcData
   */
  protected int totalProcessedCount;

  private PrimitiveList identifierList;
  private PrimitiveList alignedList;
  /**
   * the position in srcData of the first data point of the current sliding window
   */
  protected int currentStartTimeIdx;
  private long currentStartTime;
  private long currentEndTime;

  /**
   * Create CountFixedPreprocessor
   *
   * @param srcData cannot be empty or null
   * @param windowRange the sliding window range
   * @param slideStep the update size
   * @param storeIdentifier true if we need to store all identifiers. The cost will be counted.
   */
  public CountFixedPreprocessor(TVList srcData, int windowRange, int slideStep,
      boolean storeIdentifier, boolean storeAligned) {
    super(WindowType.COUNT_FIXED, windowRange, slideStep);
    this.srcData = srcData;
    this.storeIdentifier = storeIdentifier;
    this.storeAligned = storeAligned;
    initTimeFixedParams();
  }


  public CountFixedPreprocessor(TVList srcData, int windowRange, int slideStep) {
    this(srcData, windowRange, slideStep, true, true);
  }

  public CountFixedPreprocessor(TVList srcData, int windowRange) {
    this(srcData, windowRange, 1, true, true);
  }

  private void initTimeFixedParams() {
    currentProcessedIdx = -1;
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
    return currentProcessedIdx + 1 < totalProcessedCount;
  }

  @Override
  public void processNext() {
    currentProcessedIdx++;
    currentStartTimeIdx += slideStep;
    currentStartTime = srcData.getTime(currentStartTimeIdx);
    currentEndTime = srcData.getTime(currentStartTimeIdx + windowRange - 1);

    // calculate the newest aligned sequence
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
  public List<Object> getLatestN_L1_Identifiers(int latestN) {
    List<Object> res = new ArrayList<>(latestN);
    if (storeIdentifier) {
      int startIdx = Math.max(0, currentProcessedIdx + 1 - latestN);
      for (int i = startIdx; i <= currentProcessedIdx; i++) {
        Identifier identifier = new Identifier(
            identifierList.getLong(i * 3),
            identifierList.getLong(i * 3 + 1),
            (int) identifierList.getLong(i * 3 + 2));
        res.add(identifier);
      }
      return res;
    }
    int startIdxPastN = currentStartTimeIdx - (latestN - 1) * slideStep;
    while (startIdxPastN >= 0 && startIdxPastN <= currentStartTimeIdx) {
      res.add(new Identifier(srcData.getTime(startIdxPastN),
          srcData.getTime(startIdxPastN + windowRange - 1), windowRange));
      startIdxPastN += slideStep;
    }
    return res;
  }

  @Override
  public List<Object> getLatestN_L2_AlignedSequences(int latestN) {
    List<Object> res = new ArrayList<>(latestN);

    int startIdxPastN = Math.max(0, currentStartTimeIdx - (latestN - 1) * slideStep);
    while (startIdxPastN <= currentStartTimeIdx) {
      res.add(createAlignedSequence(startIdxPastN));
      startIdxPastN += slideStep;
    }
    return res;
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
  public void clear() {
    if (identifierList != null) {
      identifierList.clearAndRelease();
    }
    if (alignedList != null) {
      alignedList.clearAndRelease();
    }
  }
}
