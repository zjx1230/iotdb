package org.apache.iotdb.db.index.preprocess;

/**
 * COUNT_FIXED is very popular in the preprocessing phase. Most index-related researches build thier
 * indexes on sequences with the same length. Although many studies is concerned with time series as
 * they claimed, most of them ignore the time dimension directly, and the default sequence is fixed
 * frequency (i.e. equally spaced). In a real scene, the sequence may be of indefinite frequency,
 * but if the original COUNT_FIXED is still used, the distance measurement (e.g., Euclidean
 * distance) cannot be made sense.  In other words, the time of subsequences sliced in COUNT_FIXED
 * mode is likely to be misaligned. WindowRange: number of data points per subsequence SlideStep:
 * offset between two adjacent subsequences.
 *
 * NeedFill: the default is false because COUNT_FIXED does not need to perform a fill operation.
 */
public class CountFixedPreprocessor implements IPreprocess {


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


  public CountFixedPreprocessor(int windowRange, int equispacedLength, WindowWidthType widthType,
      int slideStep, boolean) {
    this.windowRange = windowRange;
    this.equispacedLength = equispacedLength;
    this.widthType = widthType;
    this.slideStep = slideStep;
    this.filledSequencePastNum = filledSequencePastNum;
  }

  public CountFixedPreprocessor(int subSeqLength) {
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
   * COUNT_FIXED: TIME_FIXED:
   *
   * <p>See also: Kanat et. al. General Incremental Sliding-Window Aggregation. VLDB 2015.
   */
  private enum WindowWidthType {
    TIME_FIXED, COUNT_FIXED;
  }
}
