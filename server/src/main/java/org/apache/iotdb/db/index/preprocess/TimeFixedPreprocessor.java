package org.apache.iotdb.db.index.preprocess;

import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * COUNT_FIXED is very popular in the preprocessing phase. Most previous researches build indexes on
 * sequences with the same length (a.k.a. COUNT_FIXED). The timestamp is very important for time
 * series, although a rich line of researches ignore the time information directly.<p>
 *
 * {@code WindowRange}: the number of subsequence.<p>
 *
 * {@code SlideStep}: the offset between two adjacent subsequences<p>
 *
 * {@code needFill}: inactive for COUNT_FIXED<p>
 *
 * Note that, in real scenarios, the time series could be variable-frequency, traditional distance
 * metrics (e.g., Euclidean distance) may not make sense for COUNT_FIXED.
 */
public class TimeFixedPreprocessor extends BasicPreprocessor {

  private final TVList srcData;
  protected final boolean storeIdentifier;
  protected final boolean storeAligned;
  /**
   * -1 means not aligned.
   */
  private final int equispacedLength;
  private int currentIdx;
  private int totalCount;

  public TimeFixedPreprocessor(TVList srcData, int windowRange, int equispacedLength, int slideStep,
      boolean storeAligned, boolean storeIdentifier) {
    super(WindowType.COUNT_FIXED, windowRange, slideStep);
    this.srcData = srcData;
    this.storeIdentifier = storeIdentifier;
    this.storeAligned = storeAligned;
    this.equispacedLength = equispacedLength;
    this.currentIdx = -1;
    initTimeFixedParams();
  }

  private void initTimeFixedParams() {
    long startTime = srcData.getMinTime();
    long endTime = srcData.getLastTime();
    this.totalCount = (int) ((endTime - startTime - this.windowRange) / slideStep);
    if (storeIdentifier)
  }

  public TimeFixedPreprocessor(TVList srcData, int subSeqLength, int equispacedLength,
      int slideStep) {
    this(srcData, subSeqLength, slideStep, equispacedLength, true, true);
  }

  public TimeFixedPreprocessor(TVList srcData, int subSeqLength, int equispacedLength) {
    this(srcData, subSeqLength, 1, equispacedLength, true, true);
  }

  @Override
  public boolean hasNext() {
    return currentIdx + 1 < totalCount;
  }

  @Override
  public void processNext() {
    currentIdx++;
  }

  @Override
  public Object getPastN_L1_Identifiers(int pastN) {
    return null;
  }

  @Override
  public Object getPastN_L2_AlignedSequences(int pastN) {
    return null;
  }
}
