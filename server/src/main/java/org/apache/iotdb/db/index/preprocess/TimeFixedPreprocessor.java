package org.apache.iotdb.db.index.preprocess;

/**
 * For all indexes, the raw input sequence has to be pre-processed before it's organized by indexes.
 * In general, index structure needn't maintain all of original data, but only pointers to the
 * original data (e.g. The start time and the end time can uniquely determine a time sequence).<p>
 *
 * {@linkplain TimeFixedPreprocessor} makes a time window slide over the time series by some rules and
 * obtain a list of subsequences. The time windows may be time-fixed (Euclidean distance),
 * count-fixed (Time Warping). It scans the sequence with a certain overlap step (a.k.a. the update
 * size).
 */
public class TimeFixedPreprocessor implements IPreprocess {


  protected WindowWidthType widthType;
  private int windowRange;
  private int equispacedLength;
  protected int slideStep;

  protected final boolean storeBasicTriplet = true;
  /**
   * the number of filled subsequences to be stored. If it is larger than the number of subsequence
   * or equals to -1, all filled subsequences will be stored.
   */
  protected int filledSequencePastNum;


  public TimeFixedPreprocessor(int windowRange, int equispacedLength, WindowWidthType widthType,
      int slideStep, boolean ) {
    this.windowRange = windowRange;
    this.equispacedLength = equispacedLength;
    this.widthType = widthType;
    this.slideStep = slideStep;
    this.filledSequencePastNum = filledSequencePastNum;
  }

  public TimeFixedPreprocessor(int subSeqLength) {
    this(subSeqLength, -1, WindowWidthType.COUNT_FIXED, 1, 1);
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public void processNext() {

  }

  /**
   * The time window type: time-fixed or count-fixed. It determines the meanings of following
   * fields: {@code windowRange}, {@code windowRange}, {@code slideStep}.<p>
   *
   * COUNT_FIXED:
   * TIME_FIXED:
   *
   * <p>See also: Kanat et. al. General Incremental Sliding-Window Aggregation. VLDB 2015.
   */
  private enum WindowWidthType {
    TIME_FIXED, COUNT_FIXED;
  }
}
